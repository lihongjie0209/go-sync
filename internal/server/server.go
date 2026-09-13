package server

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"go-sync/internal/event"
	syncv1 "go-sync/internal/rpc/syncv1"
	"go-sync/internal/serverconfig"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Service struct {
	syncv1.UnimplementedReplicationServer
	syncv1.UnimplementedAdminServer
	path     string
	log      *slog.Logger
	logLevel *slog.LevelVar
	metrics  *serverMetrics

	mu            sync.RWMutex
	cfg           serverconfig.Config
	targets       map[string]*target
	connected     map[string]bool
	lastErrors    map[string]string
	reconcileNext map[string]time.Time
	reloadError   string
	certificate   atomic.Pointer[tls.Certificate]
}

func (s *Service) WithLogLevel(level *slog.LevelVar) *Service {
	s.logLevel = level
	return s
}

func New(ctx context.Context, path string, cfg serverconfig.Config, log *slog.Logger) (*Service, error) {
	s := &Service{path: path, cfg: cfg, log: log, metrics: newServerMetrics(), targets: make(map[string]*target), connected: make(map[string]bool), lastErrors: make(map[string]string)}
	certificate, err := tls.LoadX509KeyPair(cfg.GRPC.TLSCertFile, cfg.GRPC.TLSKeyFile)
	if err != nil {
		return nil, err
	}
	s.certificate.Store(&certificate)
	if err := s.openTargets(ctx, cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) openTargets(ctx context.Context, cfg serverconfig.Config) error {
	opened := make(map[string]*target, len(cfg.Syncers))
	for _, syncer := range cfg.Syncers {
		target, err := openTarget(ctx, syncer)
		if err != nil {
			for _, item := range opened {
				item.Close()
			}
			return fmt.Errorf("open target for syncer %q: %w", syncer.ID, err)
		}
		opened[syncer.ID] = target
	}
	if err := validateTargetOverlap(ctx, opened); err != nil {
		for _, item := range opened {
			item.Close()
		}
		return err
	}
	s.targets = opened
	return nil
}

func validateTargetOverlap(ctx context.Context, targets map[string]*target) error {
	seen := make(map[string]string)
	for id, target := range targets {
		instance, err := target.identity(ctx)
		if err != nil {
			return err
		}
		for _, table := range target.cfg.Tables {
			key := instance + "\x00" + target.cfg.Postgres.TargetSchema + "\x00" + table.Name
			if previous := seen[key]; previous != "" && previous != id {
				return fmt.Errorf("syncers %q and %q resolve to the same target table", previous, id)
			}
			seen[key] = id
		}
	}
	return nil
}

func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, target := range s.targets {
		target.Close()
	}
	s.targets = map[string]*target{}
}

func (s *Service) Run(ctx context.Context) error {
	s.mu.RLock()
	cfg := s.cfg
	s.mu.RUnlock()
	listener, err := net.Listen("tcp", cfg.GRPC.ListenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		certificate := s.certificate.Load()
		if certificate == nil {
			return nil, errors.New("TLS certificate unavailable")
		}
		return certificate, nil
	}}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.MaxRecvMsgSize(cfg.GRPC.MaxMessageBytes), grpc.MaxSendMsgSize(cfg.GRPC.MaxMessageBytes))
	syncv1.RegisterReplicationServer(grpcServer, s)
	syncv1.RegisterAdminServer(grpcServer, s)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	g, groupCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		go func() { <-groupCtx.Done(); healthServer.Shutdown(); grpcServer.GracefulStop() }()
		err := grpcServer.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	})
	if cfg.MetricsAddr != "" {
		metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: s.metrics, ReadHeaderTimeout: 5 * time.Second}
		g.Go(func() error {
			go func() {
				<-groupCtx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = metricsServer.Shutdown(shutdownCtx)
			}()
			err := metricsServer.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}
	if cfg.VictoriaMetrics.URL != "" {
		g.Go(func() error { return s.metrics.runVM(groupCtx, s.vmConfig) })
	}
	g.Go(func() error { return s.watch(groupCtx) })
	s.log.InfoContext(ctx, "server started", "grpc_address", listener.Addr().String(), "metrics_address", cfg.MetricsAddr)
	err = g.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *Service) vmConfig() serverconfig.VictoriaMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.VictoriaMetrics
}

func (s *Service) Connect(stream grpc.BidiStreamingServer[syncv1.CollectorFrame, syncv1.ServerFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first frame must be hello")
	}
	target, token, err := s.acquire(hello.SyncerId)
	if err != nil {
		return err
	}
	defer s.release(hello.SyncerId, target)
	if !bearerMatches(stream.Context(), token) {
		return status.Error(codes.Unauthenticated, "invalid syncer credentials")
	}
	lease, err := target.acquireLease(stream.Context())
	if err != nil {
		if errors.Is(err, errTargetLeaseHeld) {
			return status.Error(codes.AlreadyExists, err.Error())
		}
		return status.Error(codes.Unavailable, "target database lease is unavailable")
	}
	defer lease.Close()
	progress, err := target.Progress(stream.Context())
	if err != nil {
		return status.Error(codes.Unavailable, "target database unavailable")
	}
	generation := hello.Generation
	next := progress.Received + 1
	if progress.Generation != "" && progress.Generation != hello.Generation {
		allowed, consumeErr := target.consumeRepairAuthorization(stream.Context())
		if consumeErr != nil {
			return status.Error(codes.Unavailable, "repair authorization is unavailable")
		}
		if !allowed {
			return status.Error(codes.FailedPrecondition, "collector generation differs from target without an authorized repair")
		}
		next = 1
	}
	s.metrics.connected.WithLabelValues(hello.SyncerId).Set(1)
	s.metrics.received.WithLabelValues(hello.SyncerId).Set(float64(progress.Received))
	s.metrics.applied.WithLabelValues(hello.SyncerId).Set(float64(progress.Applied))
	pull := func() error {
		return stream.Send(&syncv1.ServerFrame{Body: &syncv1.ServerFrame_Pull{Pull: &syncv1.Pull{Generation: generation, Seq: next}}})
	}
	if err := pull(); err != nil {
		return err
	}
	openBatch := false
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if metrics := frame.GetMetrics(); metrics != nil {
			s.mu.RLock()
			maxBytes := s.cfg.GRPC.MaxMessageBytes
			s.mu.RUnlock()
			if len(metrics.PrometheusText) > maxBytes {
				return status.Error(codes.ResourceExhausted, "metrics snapshot too large")
			}
			s.metrics.setCollector(hello.SyncerId, metrics.PrometheusText)
			continue
		}
		if frame.GetIdle() != nil {
			if s.retiring(hello.SyncerId, target) && !openBatch {
				return nil
			}
			if hello.ReconcileSupported && !openBatch && s.reconcileDue(hello.SyncerId, time.Now()) {
				repaired, reconcileErr := s.runReconcile(stream, hello.SyncerId, target)
				if reconcileErr != nil {
					s.retryReconcile(hello.SyncerId, time.Now())
					s.metrics.reconciles.WithLabelValues(hello.SyncerId, "error").Inc()
					s.setSyncerError(hello.SyncerId, reconcileErr.Error())
					return status.Error(codes.Unavailable, "reconciliation failed")
				}
				if repaired {
					return nil
				}
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-stream.Context().Done():
				timer.Stop()
				return stream.Context().Err()
			case <-timer.C:
			}
			if err := pull(); err != nil {
				return err
			}
			continue
		}
		if unavailable := frame.GetReplayUnavailable(); unavailable != nil {
			s.setSyncerError(hello.SyncerId, fmt.Sprintf("replay sequence %d unavailable; earliest is %d", unavailable.RequestedSeq, unavailable.EarliestSeq))
			return status.Error(codes.FailedPrecondition, "collector replay history is insufficient; a new snapshot is required")
		}
		incoming := frame.GetEvent()
		if incoming == nil {
			return status.Error(codes.InvalidArgument, "unexpected collector frame")
		}
		var message event.Message
		if err := json.Unmarshal(incoming.Json, &message); err != nil {
			return status.Error(codes.InvalidArgument, "event JSON is invalid")
		}
		if incoming.Seq != next || message.Seq != incoming.Seq || message.ID != incoming.MessageId || message.SourceID != hello.SyncerId || message.Generation != generation {
			return status.Error(codes.InvalidArgument, "event envelope does not match requested sequence")
		}
		expectedID := fmt.Sprintf("%s/%s/%d", hello.SyncerId, generation, next)
		if message.ID != expectedID {
			return status.Error(codes.InvalidArgument, "event message_id is not canonical")
		}
		started := time.Now()
		if err := lease.Store(stream.Context(), target, message, incoming.Json); err != nil {
			s.metrics.applyErrors.WithLabelValues(hello.SyncerId).Inc()
			s.setSyncerError(hello.SyncerId, err.Error())
			return status.Error(codes.FailedPrecondition, "target apply failed")
		}
		s.metrics.applyDuration.WithLabelValues(hello.SyncerId, message.Kind).Observe(time.Since(started).Seconds())
		s.metrics.events.WithLabelValues(hello.SyncerId, message.Kind).Inc()
		updated, progressErr := target.Progress(stream.Context())
		if progressErr == nil {
			s.metrics.received.WithLabelValues(hello.SyncerId).Set(float64(updated.Received))
			s.metrics.applied.WithLabelValues(hello.SyncerId).Set(float64(updated.Applied))
			s.metrics.pending.WithLabelValues(hello.SyncerId).Set(float64(updated.Received - updated.Applied))
		}
		s.setSyncerError(hello.SyncerId, "")
		if err := stream.Send(&syncv1.ServerFrame{Body: &syncv1.ServerFrame_Ack{Ack: &syncv1.Ack{Generation: generation, Seq: next, MessageId: message.ID}}}); err != nil {
			return err
		}
		switch message.Kind {
		case "snapshot_begin", "transaction_rows", "file_begin", "file_chunk":
			openBatch = true
		case "snapshot_end", "transaction_end", "file_end":
			openBatch = false
		}
		next++
		if s.retiring(hello.SyncerId, target) && !openBatch {
			return nil
		}
		if err := pull(); err != nil {
			return err
		}
	}
}

func (s *Service) acquire(id string) (*target, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.targets[id]
	if target == nil {
		return nil, "", status.Error(codes.PermissionDenied, "unknown syncer")
	}
	if s.connected[id] {
		return nil, "", status.Error(codes.AlreadyExists, "syncer already connected")
	}
	var token string
	for _, syncer := range s.cfg.Syncers {
		if syncer.ID == id {
			token = syncer.Token
			break
		}
	}
	s.connected[id] = true
	return target, token, nil
}

func (s *Service) release(id string, used *target) {
	s.mu.Lock()
	s.connected[id] = false
	current := s.targets[id]
	s.mu.Unlock()
	if current != used {
		used.Close()
	}
	s.metrics.connected.WithLabelValues(id).Set(0)
}

func (s *Service) retiring(id string, used *target) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.targets[id] != used
}

func bearerMatches(ctx context.Context, expected string) bool {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	actual := strings.TrimPrefix(values[0], "Bearer ")
	return len(actual) == len(expected) && subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

func (s *Service) ListSyncers(ctx context.Context, _ *syncv1.ListSyncersRequest) (*syncv1.ListSyncersResponse, error) {
	if !s.admin(ctx) {
		return nil, status.Error(codes.Unauthenticated, "invalid administrator credentials")
	}
	s.mu.RLock()
	ids := make([]string, 0, len(s.targets))
	for id := range s.targets {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	configHash, reloadError := s.cfg.Hash(), s.reloadError
	targets := make(map[string]*target, len(ids))
	connected := make(map[string]bool, len(ids))
	lastErrors := make(map[string]string, len(ids))
	for _, id := range ids {
		targets[id], connected[id], lastErrors[id] = s.targets[id], s.connected[id], s.lastErrors[id]
	}
	s.mu.RUnlock()
	response := &syncv1.ListSyncersResponse{ConfigHash: configHash, LastReloadError: reloadError}
	for _, id := range ids {
		p, err := targets[id].Progress(ctx)
		item := &syncv1.SyncerStatus{Id: id, Connected: connected[id], Error: lastErrors[id]}
		if err != nil {
			item.Error = "target database unavailable"
		} else {
			item.Generation, item.ReceivedSeq, item.AppliedSeq = p.Generation, p.Received, p.Applied
		}
		response.Syncers = append(response.Syncers, item)
	}
	return response, nil
}

func (s *Service) ReloadConfig(ctx context.Context, _ *syncv1.ReloadConfigRequest) (*syncv1.ReloadConfigResponse, error) {
	if !s.admin(ctx) {
		return nil, status.Error(codes.Unauthenticated, "invalid administrator credentials")
	}
	if err := s.reload(ctx); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	s.mu.RLock()
	hash := s.cfg.Hash()
	s.mu.RUnlock()
	return &syncv1.ReloadConfigResponse{ConfigHash: hash}, nil
}

func (s *Service) admin(ctx context.Context) bool {
	s.mu.RLock()
	token := s.cfg.AdminToken
	s.mu.RUnlock()
	return bearerMatches(ctx, token)
}

func (s *Service) reload(ctx context.Context) error {
	candidate, err := serverconfig.Load(s.path)
	if err != nil {
		s.setReloadError(err)
		return err
	}
	s.mu.RLock()
	current := s.cfg
	oldTargets := s.targets
	s.mu.RUnlock()
	if !current.StaticEqual(candidate) {
		err := errors.New("listener, metrics address and log output settings require restart")
		s.setReloadError(err)
		return err
	}
	certificate, err := tls.LoadX509KeyPair(candidate.GRPC.TLSCertFile, candidate.GRPC.TLSKeyFile)
	if err != nil {
		s.setReloadError(err)
		return err
	}
	opened := make(map[string]*target, len(candidate.Syncers))
	for _, syncer := range candidate.Syncers {
		var oldCfg *serverconfig.Syncer
		for i := range current.Syncers {
			if current.Syncers[i].ID == syncer.ID {
				oldCfg = &current.Syncers[i]
				break
			}
		}
		if oldCfg != nil && reflect.DeepEqual(oldCfg, &syncer) {
			opened[syncer.ID] = oldTargets[syncer.ID]
			continue
		}
		target, openErr := openTarget(ctx, syncer)
		if openErr != nil {
			for id, item := range opened {
				if item != oldTargets[id] {
					item.Close()
				}
			}
			err = fmt.Errorf("validate syncer %q: %w", syncer.ID, openErr)
			s.setReloadError(err)
			return err
		}
		opened[syncer.ID] = target
	}
	if err := validateTargetOverlap(ctx, opened); err != nil {
		for id, item := range opened {
			if item != oldTargets[id] {
				item.Close()
			}
		}
		s.setReloadError(err)
		return err
	}
	s.mu.Lock()
	s.cfg, s.targets, s.reloadError = candidate, opened, ""
	if !reflect.DeepEqual(current.Reconcile, candidate.Reconcile) {
		s.reconcileNext = make(map[string]time.Time)
	}
	s.mu.Unlock()
	s.certificate.Store(&certificate)
	if s.logLevel != nil {
		switch strings.ToLower(candidate.Log.Level) {
		case "debug":
			s.logLevel.Set(slog.LevelDebug)
		case "info":
			s.logLevel.Set(slog.LevelInfo)
		case "warn":
			s.logLevel.Set(slog.LevelWarn)
		case "error":
			s.logLevel.Set(slog.LevelError)
		}
	}
	for id, item := range oldTargets {
		s.mu.RLock()
		active := s.connected[id]
		s.mu.RUnlock()
		if opened[id] != item && !active {
			item.Close()
		}
	}
	s.metrics.reloads.WithLabelValues("success").Inc()
	s.log.InfoContext(ctx, "server configuration reloaded", "config_hash", candidate.Hash())
	return nil
}

func (s *Service) setSyncerError(id, value string) {
	s.mu.Lock()
	s.lastErrors[id] = value
	s.mu.Unlock()
}

func (s *Service) setReloadError(err error) {
	s.mu.Lock()
	s.reloadError = err.Error()
	s.mu.Unlock()
	s.metrics.reloads.WithLabelValues("error").Inc()
	s.log.Error("server configuration reload rejected", "error", err)
}

func (s *Service) watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	if err := watcher.Add(filepath.Dir(s.path)); err != nil {
		return err
	}
	base := filepath.Base(s.path)
	var timer *time.Timer
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-watcher.Errors:
			if err != nil {
				s.log.WarnContext(ctx, "configuration watcher error", "error", err)
			}
		case event := <-watcher.Events:
			if filepath.Base(event.Name) != base || event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
				continue
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(500*time.Millisecond, func() {
				reloadCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_ = s.reload(reloadCtx)
			})
		}
	}
}
