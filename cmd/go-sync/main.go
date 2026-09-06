package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go-sync/internal/app"
	"go-sync/internal/capture"
	"go-sync/internal/config"
	"go-sync/internal/sqlserver"
)

var version = "dev"

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(execute(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func execute(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		fmt.Fprintln(out, "Usage: go-sync <check|run|status> --config config.json\n       go-sync version")
		return 0
	}
	if args[0] == "version" {
		fmt.Fprintln(out, version)
		return 0
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
		log := slog.New(slog.NewJSONHandler(errOut, nil))
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
