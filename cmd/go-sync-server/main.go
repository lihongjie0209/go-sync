package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/logging"
	"go-sync/internal/server"
	"go-sync/internal/serverconfig"
	"go-sync/internal/serverwinservice"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(execute(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func execute(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Fprintln(out, `Usage: go-sync-server <check|run> --config server.config.json
       go-sync-server service install --config C:\path\server.config.json [--name go-sync-server]
       go-sync-server service <uninstall|start|stop|status> [--name go-sync-server]
       go-sync-server version`)
		return 0
	}
	if args[0] == "version" {
		fmt.Fprintln(out, version)
		return 0
	}
	if args[0] == "service" {
		return executeService(args[1:], out, errOut)
	}
	if args[0] != "check" && args[0] != "run" {
		fmt.Fprintln(errOut, "unknown command")
		return 2
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	flags.SetOutput(errOut)
	path := flags.String("config", "server.config.json", "server configuration file")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	absolute, err := filepath.Abs(*path)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	cfg, err := serverconfig.Load(absolute)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	log, closeLog, logLevel, err := logging.OpenDynamic(config.Config{Log: cfg.Log}, filepath.Dir(absolute), errOut)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	defer closeLog()
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	service, err := server.New(checkCtx, absolute, cfg, log.With("component", "server"))
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	service.WithLogLevel(logLevel)
	defer service.Close()
	if args[0] == "check" {
		_ = json.NewEncoder(out).Encode(map[string]any{"config_hash": cfg.Hash(), "syncers": len(cfg.Syncers), "status": "ok"})
		return 0
	}
	log.InfoContext(ctx, "go-sync-server starting", "version", version, "config_hash", cfg.Hash())
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.ErrorContext(ctx, "go-sync-server stopped", "error", err)
		return 1
	}
	return 0
}

func executeService(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, "service action is required")
		return 2
	}
	action := args[0]
	flags := flag.NewFlagSet("service "+action, flag.ContinueOnError)
	flags.SetOutput(errOut)
	name := flags.String("name", "go-sync-server", "Windows service name")
	path := flags.String("config", "server.config.json", "server configuration file")
	display := flags.String("display-name", "Go Sync Server", "Windows service display name")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	var err error
	switch action {
	case "install":
		err = serverwinservice.Install(*name, *display, *path)
	case "uninstall":
		err = serverwinservice.Uninstall(*name)
	case "start":
		err = serverwinservice.Start(*name)
	case "stop":
		err = serverwinservice.Stop(*name)
	case "status":
		var value string
		value, err = serverwinservice.Status(*name)
		if err == nil {
			fmt.Fprintln(out, value)
		}
	case "run":
		err = serverwinservice.Run(*name, *path, version)
	default:
		fmt.Fprintln(errOut, "unknown service action")
		return 2
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	return 0
}
