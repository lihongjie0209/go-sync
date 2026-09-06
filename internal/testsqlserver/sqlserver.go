//go:build integration

// Package testsqlserver owns disposable SQL Server fixtures via Testcontainers.
// There is deliberately no external DSN fallback or manual Docker command.
package testsqlserver

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Start initializes only a disposable test database and verifies the real engine
// major version. Setting compatibility_level=100 is NOT a SQL Server 2008 test.
func Start(t *testing.T) (string, *sql.DB) {
	t.Helper()
	image := os.Getenv("GO_SYNC_TEST_MSSQL_IMAGE")
	if image == "" {
		// SQL Server 2022, ProductVersion 16.0.4265.3; pin the tested image.
		image = "mcr.microsoft.com/mssql/server@sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89"
	}
	major := os.Getenv("GO_SYNC_TEST_MSSQL_MAJOR")
	if major == "" {
		major = "16"
	}
	if _, err := strconv.Atoi(major); err != nil {
		t.Fatal("invalid expected SQL Server major version")
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	password := "Test!aA9" + hex.EncodeToString(token[:])
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	container, err := testcontainers.Run(ctx, image,
		testcontainers.WithEnv(map[string]string{
			"ACCEPT_EULA": "Y", "MSSQL_SA_PASSWORD": password, "MSSQL_PID": "Developer",
			"MSSQL_AGENT_ENABLED": "true", "MSSQL_MEMORY_LIMIT_MB": "2048",
		}),
		testcontainers.WithExposedPorts("1433/tcp"),
		testcontainers.WithLabels(map[string]string{"go-sync.integration": "true"}),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
			wait.ForListeningPort("1433/tcp").WithStartupTimeout(3*time.Minute)),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start SQL Server Testcontainer: %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "1433/tcp")
	if err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "sqlserver", Host: net.JoinHostPort(host, port.Port()),
		User: url.UserPassword("sa", password), RawQuery: "database=master&encrypt=disable"}
	connect := func(dsn string) *sql.DB {
		t.Helper()
		connector, err := mssql.NewConnector(dsn)
		if err != nil {
			t.Fatal("invalid fixture dsn")
		}
		db := sql.OpenDB(connector)
		db.SetMaxOpenConns(4)
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		for {
			if err := db.PingContext(ctx); err == nil {
				return db
			}
			select {
			case <-ctx.Done():
				t.Fatal("SQL Server fixture did not accept connections")
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	master := connect(u.String())
	var version string
	if err := master.QueryRowContext(ctx, "SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion'))").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if strings.Split(version, ".")[0] != major {
		t.Fatalf("wrong actual SQL Server engine: got %s, expected major %s", version, major)
	}
	t.Logf("Testcontainers SQL Server engine %s (expected major %s), container %s", version, major, container.GetContainerID())
	// A listening SQL port is not Agent readiness. The CDC enable procedure
	// creates Agent jobs and otherwise races error 14258 during startup.
	for {
		rows, err := master.QueryContext(ctx, "EXEC msdb.dbo.sp_help_job")
		if err == nil {
			err = rows.Close()
		}
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("SQL Server Agent fixture did not become ready")
		case <-time.After(250 * time.Millisecond):
		}
	}
	for _, statement := range []string{
		"CREATE DATABASE go_sync_test",
		"ALTER DATABASE go_sync_test SET ALLOW_SNAPSHOT_ISOLATION ON",
	} {
		if _, err := master.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	u.RawQuery = "database=go_sync_test&encrypt=disable"
	db := connect(u.String())
	if _, err := db.ExecContext(ctx, "EXEC sys.sp_cdc_enable_db"); err != nil {
		t.Fatal(err)
	}
	return u.String(), db
}
