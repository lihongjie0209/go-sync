//go:build windows

package walplugin

import (
	"context"
	"crypto/rand"
	"debug/pe"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go-sync/internal/config"
	"golang.org/x/sys/windows"
)

// Ensure installs only a missing DLL on a verified local Windows PostgreSQL.
// It never changes server settings, creates a replication slot, or overwrites a DLL.
func Ensure(ctx context.Context, c config.Config, log *slog.Logger) (result error) {
	if !c.Wal2JSONAutoInstall {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pc, err := pgx.ParseConfig(c.DSN)
	if err != nil {
		return errors.New("invalid postgres dsn")
	}
	pc.ConnectTimeout = 10 * time.Second
	pc.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, pc)
	if err != nil {
		return fmt.Errorf("connect for plugin check: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer closeCancel()
		result = errors.Join(result, conn.Close(closeCtx))
	}()
	if _, err = conn.Exec(ctx, "LOAD 'wal2json'"); err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "58P01" {
		return fmt.Errorf("load wal2json (automatic installation requires superuser; disable it for a manually managed plugin): %w", err)
	}
	loadErr := err
	remote, ok := conn.PgConn().Conn().RemoteAddr().(*net.TCPAddr)
	if !ok || !remote.IP.IsLoopback() {
		return errors.New("automatic plugin installation requires a loopback connection to the local postgres instance")
	}
	var pid uint32
	var dataDir string
	var version int
	var backendStart time.Time
	err = conn.QueryRow(ctx, `SELECT pg_backend_pid(), current_setting('data_directory'),
 current_setting('server_version_num')::int,
 (SELECT backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid())`).Scan(
		&pid, &dataDir, &version, &backendStart,
	)
	if err != nil {
		return fmt.Errorf("identify local postgres: %w", err)
	}
	if _, err := versionName(version); err != nil {
		return err
	}
	binDir, arch, err := localExecutable(pid, backendStart)
	if err != nil {
		return err
	}
	if c.PostgresBinDir != "" {
		if !filepath.IsAbs(c.PostgresBinDir) {
			return errors.New("postgres_bin_dir must be absolute")
		}
		expected, err := os.Stat(c.PostgresBinDir)
		if err != nil {
			return fmt.Errorf("resolve configured postgres_bin_dir: %w", err)
		}
		actual, err := os.Stat(binDir)
		if err != nil {
			return fmt.Errorf("inspect connected postgres directory: %w", err)
		}
		if !expected.IsDir() || !os.SameFile(expected, actual) {
			return errors.New("connected postgres executable does not match postgres_bin_dir")
		}
	}
	if err := proveLocalData(ctx, conn, dataDir); err != nil {
		return err
	}
	libDir := filepath.Join(filepath.Dir(binDir), "lib")
	target := filepath.Join(libDir, "wal2json.dll")
	if _, err := os.Lstat(target); err == nil {
		return fmt.Errorf("existing wal2json.dll cannot load; refusing to overwrite it (check ABI and dependencies): %w", loadErr)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect plugin destination: %w", err)
	}
	dll, err := bundledDLL(version, arch)
	if err != nil {
		return err
	}
	installed, err := installAbsent(libDir, dll)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "LOAD 'wal2json'"); err != nil {
		return fmt.Errorf("plugin installed at %q but cannot load; retained for diagnosis, no configuration changed: %w", target, err)
	}
	if installed {
		log.Info("installed embedded wal2json", "path", target, "tag", upstreamTag,
			"server_version_num", version, "architecture", arch, "sha256", digest(dll))
	}
	return nil
}

func localExecutable(pid uint32, backendStart time.Time) (binDir, arch string, result error) {
	process, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return "", "", fmt.Errorf("open local postgres backend process: %w", err)
	}
	defer func() { result = errors.Join(result, windows.CloseHandle(process)) }()
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(process, &created, &exited, &kernel, &user); err != nil {
		return "", "", fmt.Errorf("identify local backend creation time: %w", err)
	}
	// The OS process must predate its server-side backend initialization, narrowly.
	delta := backendStart.Sub(time.Unix(0, created.Nanoseconds()))
	if delta < -time.Second || delta > 30*time.Second {
		return "", "", errors.New("local process creation time does not match connected backend")
	}
	buffer := make([]uint16, 32768)
	size := uint32(len(buffer))
	if err := windows.QueryFullProcessImageName(process, 0, &buffer[0], &size); err != nil {
		return "", "", fmt.Errorf("identify postgres executable: %w", err)
	}
	path, err := filepath.EvalSymlinks(windows.UTF16ToString(buffer[:size]))
	if err != nil {
		return "", "", fmt.Errorf("resolve postgres executable: %w", err)
	}
	if !strings.EqualFold(filepath.Base(path), "postgres.exe") || strings.HasPrefix(path, `\\`) {
		return "", "", errors.New("backend must be a local postgres.exe, not a network executable")
	}
	p, err := pe.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("inspect postgres executable architecture: %w", err)
	}
	defer func() { result = errors.Join(result, p.Close()) }()
	arch, err = machineArch(p.Machine)
	if err != nil {
		return "", "", err
	}
	return filepath.Dir(path), arch, nil
}

func proveLocalData(ctx context.Context, conn *pgx.Conn, dir string) (result error) {
	if !filepath.IsAbs(dir) || strings.HasPrefix(dir, `\\`) {
		return errors.New("postgres data_directory must be an absolute local path")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("generate local instance challenge: %w", err)
	}
	value := hex.EncodeToString(nonce[:])
	f, err := os.CreateTemp(dir, ".go-sync-plugin-probe-")
	if err != nil {
		return fmt.Errorf("prove local data directory (requires temporary-file permission): %w", err)
	}
	defer func() { result = errors.Join(result, os.Remove(f.Name())) }()
	_, writeErr := f.WriteString(value)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return fmt.Errorf("write local instance challenge: %w", err)
	}
	var actual string
	err = conn.QueryRow(ctx, "SELECT pg_read_file($1, 0, 64)", filepath.Base(f.Name())).Scan(&actual)
	if err != nil {
		return fmt.Errorf("verify local instance challenge: %w", err)
	}
	if actual != value {
		return errors.New("database does not share the verified local data directory")
	}
	return nil
}
