//go:build !windows

package walplugin

import (
	"testing"

	"go-sync/internal/config"
)

func TestNonWindowsDoesNotInstall(t *testing.T) {
	t.Parallel()
	c := config.Defaults()
	c.DSN = "invalid; must not connect"
	if err := Ensure(t.Context(), c, nil); err != nil {
		t.Fatal(err)
	}
}
