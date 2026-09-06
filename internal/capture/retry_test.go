package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
)

func TestTransientDistinguishesFilesystemAndNetwork(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "plugin path missing", err: &os.PathError{Op: "stat", Path: "plugin", Err: os.ErrNotExist}},
		{name: "plugin permission", err: &os.PathError{Op: "open", Path: "plugin", Err: os.ErrPermission}},
		{name: "unsupported plugin", err: errors.New("unsupported")},
		{name: "network operation", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}, want: true},
		{name: "connection closed", err: io.EOF, want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := transient(fmt.Errorf("startup: %w", tc.err)); got != tc.want {
				t.Fatalf("transient=%v, want %v", got, tc.want)
			}
		})
	}
}
