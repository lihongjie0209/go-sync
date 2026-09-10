// Package logging creates structured JSON loggers with optional file rotation.
package logging

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"go-sync/internal/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Open creates a logger. Relative file paths are resolved from configDir.
// The returned close function must be called after the logger is no longer used.
func Open(c config.Config, configDir string, console io.Writer) (*slog.Logger, func() error, error) {
	level := new(slog.LevelVar)
	switch strings.ToLower(c.Log.Level) {
	case "debug":
		level.Set(slog.LevelDebug)
	case "info":
		level.Set(slog.LevelInfo)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		return nil, func() error { return nil }, errors.New("invalid log level")
	}

	writers := make([]io.Writer, 0, 2)
	if console != nil {
		writers = append(writers, console)
	}
	closeLog := func() error { return nil }
	if c.Log.File != "" {
		path := c.Log.File
		if !filepath.IsAbs(path) {
			path = filepath.Join(configDir, path)
		}
		path = filepath.Clean(path)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, closeLog, err
		}
		rotator := &lumberjack.Logger{
			Filename:   path,
			MaxSize:    c.Log.MaxSizeMB,
			MaxBackups: c.Log.MaxBackups,
			MaxAge:     c.Log.MaxAgeDays,
			Compress:   c.Log.Compress,
		}
		writers = append(writers, rotator)
		closeLog = rotator.Close
	}
	if len(writers) == 0 {
		return nil, closeLog, errors.New("no log output configured")
	}
	return slog.New(slog.NewJSONHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: level})), closeLog, nil
}
