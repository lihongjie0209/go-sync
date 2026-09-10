//go:build windows

// Package winservice integrates go-sync with the Windows Service Control Manager.
package winservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-sync/internal/app"
	"go-sync/internal/config"
	"go-sync/internal/logging"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const stopTimeout = 30 * time.Second

type handler struct {
	configPath string
	version    string
}

func (h *handler) Execute(_ []string, requests <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	status <- svc.Status{State: svc.StartPending}
	c, err := config.Load(h.configPath)
	if err != nil {
		return false, 1
	}
	if c.Log.File == "" {
		c.Log.File = filepath.Join("logs", "go-sync.log")
	}
	log, closeLog, err := logging.Open(c, filepath.Dir(h.configPath), nil)
	if err != nil {
		return false, 1
	}
	defer closeLog()
	log = log.With("source_type", c.Engine(), "source_id", c.SourceID, "service", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runCollector(ctx, c, log, h.version) }()
	status <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for {
		select {
		case err := <-done:
			if err != nil {
				log.Error("collector stopped", "error", app.SafeError(err))
				return false, 1
			}
			return false, 0
		case request, ok := <-requests:
			if !ok {
				cancel()
				return false, 1
			}
			switch request.Cmd {
			case svc.Interrogate:
				status <- request.CurrentStatus
			case svc.Stop, svc.Shutdown:
				status <- svc.Status{State: svc.StopPending}
				cancel()
				select {
				case err := <-done:
					if err != nil {
						log.Error("collector stopped", "error", app.SafeError(err))
						return false, 1
					}
					return false, 0
				case <-time.After(stopTimeout):
					log.Error("collector stop timed out")
					return false, 1
				}
			}
		}
	}
}

func runCollector(ctx context.Context, c config.Config, log *slog.Logger, version string) error {
	log.InfoContext(ctx, "collector starting", "version", version)
	err := app.Run(ctx, c, log)
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	if err == nil {
		log.InfoContext(ctx, "collector stopped")
	}
	return err
}

func Run(name, configPath, version string) error {
	path, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}
	return svc.Run(name, &handler{configPath: path, version: version})
}

func Install(name, displayName, configPath string) error {
	if err := validateName(name); err != nil {
		return err
	}
	configPath, err := filepath.Abs(configPath)
	if err != nil {
		return err
	}
	c, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load service config: %w", err)
	}
	if !filepath.IsAbs(c.DataDir) {
		return errors.New("service data_dir must be an absolute path")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		return err
	}
	manager, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	if existing, openErr := manager.OpenService(name); openErr == nil {
		existing.Close()
		return errors.New("service already exists")
	}
	service, err := manager.CreateService(name, exe, mgr.Config{
		DisplayName: displayName,
		Description: "go-sync database change collector",
		StartType:   mgr.StartAutomatic,
	}, "service", "run", "--name", name, "--config", configPath)
	if err != nil {
		return err
	}
	return service.Close()
}

func Uninstall(name string) error {
	service, manager, err := open(name)
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	defer service.Close()
	state, err := service.Query()
	if err != nil {
		return err
	}
	if state.State != svc.Stopped {
		if _, err := service.Control(svc.Stop); err != nil {
			return fmt.Errorf("stop service: %w", err)
		}
		if err := waitState(service, svc.Stopped, stopTimeout); err != nil {
			return err
		}
	}
	return service.Delete()
}

func Start(name string) error {
	service, manager, err := open(name)
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	defer service.Close()
	return service.Start()
}

func Stop(name string) error {
	service, manager, err := open(name)
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	defer service.Close()
	state, err := service.Query()
	if err != nil {
		return err
	}
	if state.State == svc.Stopped {
		return nil
	}
	if _, err := service.Control(svc.Stop); err != nil {
		return err
	}
	return waitState(service, svc.Stopped, stopTimeout)
}

func Status(name string) (string, error) {
	service, manager, err := open(name)
	if err != nil {
		return "", err
	}
	defer manager.Disconnect()
	defer service.Close()
	state, err := service.Query()
	if err != nil {
		return "", err
	}
	switch state.State {
	case svc.Stopped:
		return "stopped", nil
	case svc.StartPending:
		return "start_pending", nil
	case svc.StopPending:
		return "stop_pending", nil
	case svc.Running:
		return "running", nil
	case svc.PausePending:
		return "pause_pending", nil
	case svc.Paused:
		return "paused", nil
	case svc.ContinuePending:
		return "continue_pending", nil
	default:
		return fmt.Sprintf("unknown_%d", state.State), nil
	}
}

func open(name string) (*mgr.Service, *mgr.Mgr, error) {
	if err := validateName(name); err != nil {
		return nil, nil, err
	}
	manager, err := mgr.Connect()
	if err != nil {
		return nil, nil, err
	}
	service, err := manager.OpenService(name)
	if err != nil {
		manager.Disconnect()
		return nil, nil, err
	}
	return service, manager, nil
}

func waitState(service *mgr.Service, wanted svc.State, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := service.Query()
		if err != nil {
			return err
		}
		if state.State == wanted {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return errors.New("timed out waiting for service state")
}

func validateName(name string) error {
	if name == "" || len(name) > 256 || strings.ContainsAny(name, `/\\\x00`) {
		return errors.New("invalid service name")
	}
	return nil
}
