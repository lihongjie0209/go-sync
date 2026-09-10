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

	"go-sync/internal/app"
	"go-sync/internal/capture"
	"go-sync/internal/config"
	"go-sync/internal/logging"
	mysqlsource "go-sync/internal/mysql"
	"go-sync/internal/sqlserver"
	"go-sync/internal/sqlserverlegacy"
	"go-sync/internal/winservice"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(execute(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func execute(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		printUsage(out)
		return 0
	}
	if args[0] == "version" {
		fmt.Fprintln(out, version)
		return 0
	}
	if args[0] == "service" {
		return executeService(args[1:], out, errOut)
	}
	if args[0] != "check" && args[0] != "run" && args[0] != "status" {
		fmt.Fprintln(errOut, "unknown command")
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(errOut)
	path := fs.String("config", "config.json", "configuration file")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected positional arguments")
		return 2
	}
	c, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 2
	}
	switch args[0] {
	case "check":
		checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		var info any
		if c.Engine() == "sqlserver" {
			info, err = sqlserver.Inspect(checkCtx, c)
		} else if c.Engine() == "sqlserver_legacy" {
			info, err = sqlserverlegacy.Inspect(checkCtx, c)
		} else if c.Engine() == "mysql" {
			info, err = mysqlsource.Inspect(checkCtx, c)
		} else {
			info, err = capture.Inspect(checkCtx, c)
		}
		if err == nil {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			err = enc.Encode(info)
		}
	case "status":
		var b []byte
		b, err = os.ReadFile(filepath.Join(c.DataDir, "status.json"))
		if err == nil {
			_, err = fmt.Fprintln(out, string(b))
		}
	case "run":
		configPath, pathErr := filepath.Abs(*path)
		if pathErr != nil {
			fmt.Fprintln(errOut, "resolve config path:", pathErr)
			return 1
		}
		log, closeLog, logErr := logging.Open(c, filepath.Dir(configPath), errOut)
		if logErr != nil {
			fmt.Fprintln(errOut, "open log output:", logErr)
			return 1
		}
		defer closeLog()
		log = log.With("source_type", c.Engine(), "source_id", c.SourceID)
		log.InfoContext(ctx, "collector starting", "version", version)
		err = app.Run(ctx, c, log)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.ErrorContext(ctx, "collector stopped", "error", app.SafeError(err))
			return 1
		}
		log.InfoContext(ctx, "collector stopped")
		return 0
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(errOut, app.SafeError(err))
		return 1
	}
	return 0
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, `Usage: go-sync <check|run|status> --config config.json
       go-sync service install --config C:\path\config.json [--name go-sync] [--display-name "Go Sync"]
       go-sync service <uninstall|start|stop|status> [--name go-sync]
       go-sync version`)
}

func executeService(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, "service action is required")
		return 2
	}
	action := args[0]
	if action != "install" && action != "uninstall" && action != "start" && action != "stop" && action != "status" && action != "run" {
		fmt.Fprintln(errOut, "unknown service action")
		return 2
	}
	fs := flag.NewFlagSet("service "+action, flag.ContinueOnError)
	fs.SetOutput(errOut)
	name := fs.String("name", "go-sync", "Windows service name")
	configPath := fs.String("config", "config.json", "configuration file")
	displayName := fs.String("display-name", "Go Sync", "Windows service display name")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(errOut, "unexpected positional arguments")
		return 2
	}
	var err error
	switch action {
	case "install":
		err = winservice.Install(*name, *displayName, *configPath)
	case "uninstall":
		err = winservice.Uninstall(*name)
	case "start":
		err = winservice.Start(*name)
	case "stop":
		err = winservice.Stop(*name)
	case "status":
		var state string
		state, err = winservice.Status(*name)
		if err == nil {
			fmt.Fprintln(out, state)
		}
	case "run":
		err = winservice.Run(*name, *configPath, version)
	}
	if err != nil {
		fmt.Fprintln(errOut, err)
		return 1
	}
	if action != "status" && action != "run" {
		fmt.Fprintf(out, "service %s: %s\n", *name, action)
	}
	return 0
}
