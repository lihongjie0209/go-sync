package delivery

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/queue"
	syncv1 "go-sync/internal/rpc/syncv1"
	"go-sync/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type GRPCSender struct {
	cfg       config.Config
	log       *slog.Logger
	metrics   *telemetry.Metrics
	reconcile ReconcileController
}

type ReconcileController interface {
	Reconcile(context.Context, *syncv1.ReconcileRequest) (*syncv1.ReconcileResult, error)
	Resume(context.Context, string) error
	Repair(context.Context, string) error
}

var ErrRepairRestart = errors.New("reconciliation repair requested a new snapshot")

func NewGRPC(c config.Config, log *slog.Logger) *GRPCSender {
	return &GRPCSender{cfg: c, log: log}
}

func (s *GRPCSender) WithMetrics(metrics *telemetry.Metrics) *GRPCSender {
	s.metrics = metrics
	return s
}

func (s *GRPCSender) WithReconcile(controller ReconcileController) *GRPCSender {
	s.reconcile = controller
	return s
}

func (s *GRPCSender) Run(ctx context.Context, q *queue.Store) error {
	minDelay, _ := time.ParseDuration(s.cfg.RetryMin)
	maxDelay, _ := time.ParseDuration(s.cfg.RetryMax)
	delay := minDelay
	for {
		err := s.connect(ctx, q)
		if ctx.Err() != nil {
			return nil
		}
		switch status.Code(err) {
		case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
			return err
		}
		s.log.WarnContext(ctx, "grpc delivery disconnected", "error", err, "retry_in", delay)
		if err := Wait(ctx, delay); err != nil {
			return nil
		}
		delay = min(maxDelay, delay*2)
	}
}

func (s *GRPCSender) connect(ctx context.Context, q *queue.Store) error {
	var activeReconcile string
	defer func() {
		if activeReconcile != "" && s.reconcile != nil {
			resumeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := s.reconcile.Resume(resumeCtx, activeReconcile); err != nil {
				s.log.Warn("failed to resume capture after interrupted reconciliation", "error", err)
			}
		}
	}()
	certificate, err := os.ReadFile(s.cfg.GRPC.TLS.CAFile)
	if err != nil {
		return fmt.Errorf("read grpc CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate) {
		return errors.New("grpc CA contains no certificate")
	}
	tlsConfig := credentials.NewClientTLSFromCert(roots, s.cfg.GRPC.TLS.ServerName)
	conn, err := grpc.NewClient(s.cfg.GRPC.Address, grpc.WithTransportCredentials(tlsConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(s.cfg.MaxRowBytes+1<<20), grpc.MaxCallSendMsgSize(s.cfg.MaxRowBytes+1<<20)))
	if err != nil {
		return err
	}
	defer conn.Close()
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.cfg.GRPC.Token)
	stream, err := syncv1.NewReplicationClient(conn).Connect(authCtx)
	if err != nil {
		return err
	}
	generation, earliest, ready, err := q.Bounds()
	if err != nil {
		return err
	}
	if generation == "" {
		return errors.New("capture generation is not ready")
	}
	if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_Hello{Hello: &syncv1.Hello{
		SyncerId: s.cfg.SourceID, Generation: generation, EarliestSeq: earliest, ReadySeq: ready, ProtocolVersion: "v2", ReconcileSupported: s.reconcile != nil,
	}}}); err != nil {
		return err
	}
	metricsEvery, _ := time.ParseDuration(s.cfg.GRPC.MetricsInterval)
	lastMetrics := time.Time{}
	sendMetrics := func() error {
		if s.metrics == nil || time.Since(lastMetrics) < metricsEvery {
			return nil
		}
		payload, err := s.metrics.GatherText()
		if err != nil {
			return err
		}
		lastMetrics = time.Now()
		return stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_Metrics{Metrics: &syncv1.Metrics{
			PrometheusText: payload, CollectedAtUnixMilli: lastMetrics.UnixMilli(),
		}}})
	}
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if request := frame.GetReconcile(); request != nil {
			activeReconcile = request.RequestId
			result := &syncv1.ReconcileResult{RequestId: request.RequestId, SourceSchema: request.SourceSchema, Table: request.Table}
			if s.reconcile == nil {
				result.Error = "collector does not support reconciliation"
			} else {
				reconcileCtx := ctx
				cancel := func() {}
				if request.TimeoutSeconds > 0 {
					reconcileCtx, cancel = context.WithTimeout(ctx, time.Duration(request.TimeoutSeconds)*time.Second)
				}
				computed, reconcileErr := s.reconcile.Reconcile(reconcileCtx, request)
				cancel()
				if reconcileErr != nil {
					s.log.WarnContext(ctx, "source reconciliation failed", "error", reconcileErr)
					result.Error = "source reconciliation failed"
				} else if computed == nil || computed.RequestId != request.RequestId {
					result.Error = "collector returned an invalid reconciliation result"
				} else {
					result = computed
				}
			}
			if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_ReconcileResult{ReconcileResult: result}}); err != nil {
				return err
			}
			continue
		}
		if request := frame.GetResume(); request != nil {
			if s.reconcile == nil {
				return errors.New("grpc server requested reconciliation resume from an unsupported collector")
			}
			if err := s.reconcile.Resume(ctx, request.RequestId); err != nil {
				return err
			}
			activeReconcile = ""
			if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_ResumeAccepted{ResumeAccepted: &syncv1.ResumeAccepted{RequestId: request.RequestId}}}); err != nil {
				return err
			}
			continue
		}
		if request := frame.GetRepair(); request != nil {
			if s.reconcile == nil {
				return errors.New("grpc server requested repair from an unsupported collector")
			}
			if err := s.reconcile.Repair(ctx, request.RequestId); err != nil {
				return err
			}
			activeReconcile = ""
			if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_RepairAccepted{RepairAccepted: &syncv1.RepairAccepted{RequestId: request.RequestId}}}); err != nil {
				return err
			}
			return ErrRepairRestart
		}
		if ack := frame.GetAck(); ack != nil {
			st, err := q.State()
			if err != nil {
				return err
			}
			if ack.Generation != st.Generation || ack.Seq > st.DeliveredSeq+1 {
				return errors.New("grpc server returned a non-contiguous acknowledgement")
			}
			if ack.Seq == st.DeliveredSeq+1 {
				if err := q.Ack(ack.Seq, ack.MessageId); err != nil {
					return err
				}
			}
			if err := sendMetrics(); err != nil {
				return err
			}
			continue
		}
		pull := frame.GetPull()
		if pull == nil {
			return errors.New("grpc server sent an empty frame")
		}
		generation, earliest, ready, err = q.Bounds()
		if err != nil {
			return err
		}
		if pull.Generation != "" && pull.Generation != generation {
			return errors.New("grpc server requested an unknown generation")
		}
		if pull.Seq < earliest {
			if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_ReplayUnavailable{ReplayUnavailable: &syncv1.ReplayUnavailable{
				RequestedSeq: pull.Seq, EarliestSeq: earliest,
			}}}); err != nil {
				return err
			}
			continue
		}
		if pull.Seq > ready {
			if err := sendMetrics(); err != nil {
				return err
			}
			if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_Idle{Idle: &syncv1.Idle{ReadySeq: ready}}}); err != nil {
				return err
			}
			continue
		}
		message, raw, err := q.Get(pull.Seq)
		if err != nil {
			return err
		}
		if err := stream.Send(&syncv1.CollectorFrame{Body: &syncv1.CollectorFrame_Event{Event: &syncv1.Event{
			Seq: message.Seq, MessageId: message.ID, Json: raw,
		}}}); err != nil {
			return err
		}
	}
}
