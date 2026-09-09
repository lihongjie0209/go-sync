//go:build integration

// Package testmysql owns disposable MySQL 5.6/5.7 fixtures.
package testmysql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Start(t *testing.T, version string) (string, *client.Conn) {
	t.Helper()
	images := map[string]string{
		"5.5": "mysql@sha256:12da85ab88aedfdf39455872fb044f607c32fdc233cd59f1d26769fbf439b045",
		"5.6": "mysql@sha256:20575ecebe6216036d25dab5903808211f1e9ba63dc7825ac20cb975e34cfcae",
		"5.7": "mysql@sha256:4bc6bc963e6d8443453676cae56536f4b8156d78bae03c0145cbe47c2aad73bb",
	}
	image, ok := images[version]
	if !ok {
		t.Fatalf("no pinned official MySQL fixture for %s", version)
	}
	var token [12]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	password := "Test-aA9" + hex.EncodeToString(token[:])
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	command := []string{"--server-id=177001", "--log-bin=mysql-bin", "--binlog-format=ROW", "--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci"}
	if version != "5.5" {
		command = append(command, "--binlog-row-image=FULL")
	}
	container, err := testcontainers.Run(ctx, image,
		testcontainers.WithEnv(map[string]string{"MYSQL_ROOT_PASSWORD": password, "MYSQL_DATABASE": "go_sync_test"}),
		testcontainers.WithCmd(command...),
		testcontainers.WithExposedPorts("3306/tcp"), testcontainers.WithLabels(map[string]string{"go-sync.integration": "true", "go-sync.mysql": version}),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute, wait.ForLog("ready for connections").WithOccurrence(1).WithStartupTimeout(3*time.Minute)))
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start MySQL %s Testcontainer: %v", version, err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}
	address := net.JoinHostPort(host, port.Port())
	var conn *client.Conn
	for {
		conn, err = client.ConnectWithContext(ctx, address, "root", password, "go_sync_test", 10*time.Second)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("MySQL %s did not accept connections: %v", version, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Cleanup(func() { conn.Close() })
	r, err := conn.Execute("SELECT VERSION()")
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := r.GetString(0, 0)
	if !strings.HasPrefix(actual, version+".") {
		t.Fatalf("wrong MySQL engine: got %s, want %s.x", actual, version)
	}
	t.Logf("Testcontainers MySQL engine %s, container %s", actual, container.GetContainerID())
	u := url.URL{Scheme: "mysql", Host: address, User: url.UserPassword("root", password), Path: "/go_sync_test"}
	return u.String(), conn
}
