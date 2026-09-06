//go:build integration

// Package testpg owns disposable PostgreSQL instances through Testcontainers.
// It intentionally has no external-DSN escape hatch.
package testpg

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Postgres struct {
	Container *testcontainers.DockerContainer
	DSN       string
}

func Start(t *testing.T) *Postgres {
	t.Helper()
	version := os.Getenv("GO_SYNC_TEST_PG_VERSION")
	if version == "" {
		version = "9.5.25"
	}
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9]+)?$`).MatchString(version) {
		t.Fatal("invalid GO_SYNC_TEST_PG_VERSION")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate PostgreSQL build fixture")
	}
	buildDir := filepath.Join(filepath.Dir(file), "..", "..", "testdata", "postgres")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	container, err := testcontainers.Run(ctx, "",
		testcontainers.WithDockerfile(testcontainers.FromDockerfile{
			Context: buildDir, Dockerfile: "Dockerfile", BuildArgs: map[string]*string{"PG_VERSION": &version},
		}),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithLabels(map[string]string{"go-sync.integration": "true"}),
		// Keep the container/port alive while testing an immediate PostgreSQL
		// shutdown and crash recovery. No shell Docker commands are involved.
		testcontainers.WithCmd("sh", "-c", "while true; do sh /entrypoint.sh; sleep 1; done"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"pg_isready", "-U", "postgres"}).WithStartupTimeout(60*time.Second)),
	)
	// Register even if startup failed and returned a partially created container.
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start PostgreSQL %s with Testcontainers: %v", version, err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "postgres", User: url.User("postgres"), Host: net.JoinHostPort(host, port.Port()), Path: "postgres", RawQuery: "sslmode=disable"}
	t.Logf("Testcontainers PostgreSQL %s, container %s", version, container.GetContainerID())
	return &Postgres{Container: container, DSN: u.String()}
}

// CrashRestart forces immediate shutdown (no clean checkpoint) and waits for
// PostgreSQL recovery; the container supervisor retains the mapped port.
func (p *Postgres) CrashRestart(ctx context.Context) error {
	code, _, err := p.Container.Exec(ctx, []string{"pg_ctl", "-D", "/data", "stop", "-m", "immediate", "-w"})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("immediate PostgreSQL shutdown exited %d", code)
	}
	return wait.ForExec([]string{"pg_isready", "-U", "postgres"}).WithStartupTimeout(30*time.Second).WaitUntilReady(ctx, p.Container)
}
