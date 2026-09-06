//go:build !windows

package walplugin

import (
	"context"
	"log/slog"

	"go-sync/internal/config"
)

// Ensure leaves non-Windows plugin management to the administrator.
func Ensure(context.Context, config.Config, *slog.Logger) error { return nil }
