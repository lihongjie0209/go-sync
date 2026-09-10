//go:build windows

package serverwinservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/logging"
	"go-sync/internal/server"
	"go-sync/internal/serverconfig"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const stopTimeout = 30 * time.Second

type handler struct{ name, path, version string }

func (h *handler) Execute(_ []string, requests <-chan svc.ChangeRequest, statuses chan<- svc.Status) (bool, uint32) {
	statuses <- svc.Status{State: svc.StartPending}
	cfg, err := serverconfig.Load(h.path)
	if err != nil {
		return false, 1
	}
	if cfg.Log.File == "" {
		cfg.Log.File = filepath.Join("logs", "go-sync-server.log")
	}
	log, closeLog, logLevel, err := logging.OpenDynamic(config.Config{Log: cfg.Log}, filepath.Dir(h.path), nil)
	if err != nil {
		return false, 1
	}
	defer closeLog()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service, err := server.New(ctx, h.path, cfg, log.With("component", "server", "service", true))
	if err != nil {
		return false, 1
	}
	service.WithLogLevel(logLevel)
	defer service.Close()
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	statuses <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for {
		select {
		case err := <-done:
			if err != nil {
				log.Error("server stopped", "error", err)
				return false, 1
			}
			return false, 0
		case request := <-requests:
			switch request.Cmd {
			case svc.Interrogate:
				statuses <- request.CurrentStatus
			case svc.Stop, svc.Shutdown:
				statuses <- svc.Status{State: svc.StopPending}
				cancel()
				select {
				case err := <-done:
					if err != nil {
						return false, 1
					}
					return false, 0
				case <-time.After(stopTimeout):
					return false, 1
				}
			}
		}
	}
}

func Run(name, path, version string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	return svc.Run(name, &handler{name: name, path: absolute, version: version})
}

func Install(name, displayName, path string) error {
	if err := validateName(name); err != nil {
		return err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := serverconfig.Load(absolute); err != nil {
		return fmt.Errorf("load service config: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	manager, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer manager.Disconnect()
	if existing, err := manager.OpenService(name); err == nil {
		existing.Close()
		return errors.New("service already exists")
	}
	service, err := manager.CreateService(name, executable, mgr.Config{DisplayName: displayName, Description: "go-sync standard gRPC receiver", StartType: mgr.StartAutomatic}, "service", "run", "--name", name, "--config", absolute)
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
			return err
		}
		if err := wait(service, svc.Stopped); err != nil {
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
	if err != nil || state.State == svc.Stopped {
		return err
	}
	if _, err := service.Control(svc.Stop); err != nil {
		return err
	}
	return wait(service, svc.Stopped)
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
	names := map[svc.State]string{svc.Stopped: "stopped", svc.StartPending: "start_pending", svc.StopPending: "stop_pending", svc.Running: "running", svc.Paused: "paused"}
	if value := names[state.State]; value != "" {
		return value, nil
	}
	return fmt.Sprintf("unknown_%d", state.State), nil
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
func wait(service *mgr.Service, wanted svc.State) error {
	deadline := time.Now().Add(stopTimeout)
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
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(name) {
		return errors.New("invalid service name")
	}
	return nil
}
