// Package filewatch captures recursive filesystem changes into the durable queue.
package filewatch

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

type Collector struct {
	cfg     config.Config
	q       *queue.Store
	log     *slog.Logger
	metrics *telemetry.Metrics
}

type Inspection struct {
	RootDir string `json:"root_dir"`
	Files   uint64 `json:"files"`
	Bytes   int64  `json:"bytes"`
}

// Inspect verifies traversal without starting watchers or writing state.
func Inspect(ctx context.Context, cfg config.Config) (Inspection, error) {
	result := Inspection{RootDir: cfg.FileWatch.RootDir}
	err := filepath.WalkDir(cfg.FileWatch.RootDir, func(_ string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil
		}
		result.Files++
		result.Bytes += info.Size()
		return nil
	})
	return result, err
}

type fileState struct {
	Size            int64  `json:"size"`
	ModTimeUnixNano int64  `json:"mod_time_unix_nano"`
	Mode            uint32 `json:"mode"`
	SHA256          string `json:"sha256"`
}

type manifest struct {
	Files map[string]fileState `json:"files"`
}

var errFileChanged = errors.New("file changed while being captured")

func New(cfg config.Config, q *queue.Store, log *slog.Logger) *Collector {
	return &Collector{cfg: cfg, q: q, log: log}
}

func (c *Collector) WithMetrics(metrics *telemetry.Metrics) *Collector {
	c.metrics = metrics
	return c
}

func (c *Collector) Run(ctx context.Context) error {
	if err := c.q.Recover(); err != nil {
		return err
	}
	state, err := c.q.State()
	if err != nil {
		return err
	}
	if state.Phase == "" || state.Phase == "snapshot" {
		state = queue.State{ProtocolVersion: "v2", SourceID: c.cfg.SourceID, Generation: uuid.NewString(), Fingerprint: c.cfg.Fingerprint()}
		if err := c.q.Initialize(state); err != nil {
			return err
		}
		if err := c.q.Append(event.Message{Kind: "snapshot_begin"}); err != nil {
			return err
		}
		if err := c.q.Append(event.Message{Kind: "snapshot_end"}); err != nil {
			return err
		}
		if err := c.q.Publish(checkpoint(), true); err != nil {
			return err
		}
	}

	known, err := c.loadManifest()
	if err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	watched := make(map[string]bool)
	interval, _ := time.ParseDuration(c.cfg.FileWatch.ScanInterval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	c.log.InfoContext(ctx, "filesystem capture started", "root", c.cfg.FileWatch.RootDir, "scan_interval", interval, "events", c.cfg.FileWatch.Events)

	scan := func() error {
		current, err := c.scan(ctx, watcher, watched)
		if err != nil {
			return err
		}
		if err := c.publishChanges(ctx, known, current); errors.Is(err, errFileChanged) {
			c.log.WarnContext(ctx, "file changed during capture; retrying on next scan", "error", err)
			return nil
		} else if err != nil {
			return err
		}
		known = current
		return c.saveManifest(known)
	}
	if err := scan(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err, ok := <-watcher.Errors:
			if !ok {
				return errors.New("filesystem watcher stopped")
			}
			if err != nil {
				c.log.WarnContext(ctx, "filesystem watcher error; periodic scan remains active", "error", err)
			}
		case _, ok := <-watcher.Events:
			if !ok {
				return errors.New("filesystem watcher stopped")
			}
			// Coalesce editor rename/write bursts before doing an authoritative scan.
			timer := time.NewTimer(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			if err := scan(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := scan(); err != nil {
				return err
			}
		}
	}
}

func (c *Collector) scan(ctx context.Context, watcher *fsnotify.Watcher, watched map[string]bool) (map[string]fileState, error) {
	result := make(map[string]fileState)
	root, err := os.OpenRoot(c.cfg.FileWatch.RootDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	err = filepath.WalkDir(c.cfg.FileWatch.RootDir, func(full string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if !watched[full] {
				if err := watcher.Add(full); err != nil {
					return err
				}
				watched[full] = true
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(c.cfg.FileWatch.RootDir, full)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !c.matches(rel) {
			return nil
		}
		if info.Size() > c.cfg.FileWatch.MaxFileBytes {
			return fmt.Errorf("file %q exceeds max_file_bytes", rel)
		}
		digest, err := hashFile(ctx, root, filepath.FromSlash(rel))
		if err != nil {
			return err
		}
		result[rel] = fileState{Size: info.Size(), ModTimeUnixNano: info.ModTime().UnixNano(), Mode: uint32(info.Mode().Perm()), SHA256: digest}
		return nil
	})
	return result, err
}

func (c *Collector) matches(name string) bool {
	matchAny := func(patterns []string) bool {
		for _, pattern := range patterns {
			matched, _ := filepath.Match(filepath.FromSlash(pattern), filepath.FromSlash(name))
			if matched {
				return true
			}
		}
		return false
	}
	if len(c.cfg.FileWatch.Include) != 0 && !matchAny(c.cfg.FileWatch.Include) {
		return false
	}
	return !matchAny(c.cfg.FileWatch.Exclude)
}

func (c *Collector) publishChanges(ctx context.Context, old, current map[string]fileState) error {
	paths := make([]string, 0, len(old)+len(current))
	for name := range current {
		paths = append(paths, name)
	}
	for name := range old {
		if _, ok := current[name]; !ok {
			paths = append(paths, name)
		}
	}
	slices.Sort(paths)
	paths = slices.Compact(paths)
	for _, name := range paths {
		before, existed := old[name]
		after, exists := current[name]
		if exists && existed && before == after {
			continue
		}
		operation := "delete"
		if exists {
			operation = "create"
			if existed {
				operation = "update"
			}
		}
		if !c.allows(operation) {
			continue
		}
		if operation == "delete" {
			if err := c.q.Append(event.Message{Kind: "file_delete", File: &event.FileChange{Path: name, Operation: operation}}); err != nil {
				return err
			}
			if err := c.q.Publish(checkpoint(), false); err != nil {
				return err
			}
			c.metrics.FileCaptured(operation, 0)
			c.log.DebugContext(ctx, "file change published", "path", name, "operation", operation)
			continue
		}
		if err := c.publishFile(ctx, name, operation, after); err != nil {
			return err
		}
	}
	return nil
}

func (c *Collector) publishFile(ctx context.Context, name, operation string, state fileState) error {
	chunks := uint32((state.Size + int64(c.cfg.FileWatch.ChunkBytes) - 1) / int64(c.cfg.FileWatch.ChunkBytes))
	if chunks == 0 {
		chunks = 1
	}
	change := event.FileChange{Path: name, Operation: operation, Size: state.Size, ModTimeUnixNano: state.ModTimeUnixNano, Mode: state.Mode, SHA256: state.SHA256, Chunks: chunks}
	transaction := uuid.NewString()
	if err := c.q.Append(event.Message{Kind: "file_begin", Transaction: transaction, File: &change}); err != nil {
		return err
	}
	root, err := os.OpenRoot(c.cfg.FileWatch.RootDir)
	if err != nil {
		return errors.Join(err, c.q.Recover())
	}
	defer root.Close()
	file, err := root.Open(filepath.FromSlash(name))
	if err != nil {
		return errors.Join(err, c.q.Recover())
	}
	defer file.Close()
	buffer := make([]byte, c.cfg.FileWatch.ChunkBytes)
	hash := sha256.New()
	for chunk := uint32(0); chunk < chunks; chunk++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, c.q.Recover())
		}
		n, readErr := io.ReadFull(file, buffer)
		if errors.Is(readErr, io.ErrUnexpectedEOF) || errors.Is(readErr, io.EOF) {
			readErr = nil
		}
		if readErr != nil {
			return errors.Join(readErr, c.q.Recover())
		}
		part := change
		part.Chunk = chunk
		part.Data = base64.StdEncoding.EncodeToString(buffer[:n])
		_, _ = hash.Write(buffer[:n])
		if err := c.q.Append(event.Message{Kind: "file_chunk", Transaction: transaction, File: &part}); err != nil {
			return errors.Join(err, c.q.Recover())
		}
	}
	info, err := file.Stat()
	if err != nil || info.Size() != state.Size || info.ModTime().UnixNano() != state.ModTimeUnixNano || hex.EncodeToString(hash.Sum(nil)) != state.SHA256 {
		return errors.Join(errFileChanged, err, c.q.Recover())
	}
	if err := c.q.Append(event.Message{Kind: "file_end", Transaction: transaction, File: &change}); err != nil {
		return errors.Join(err, c.q.Recover())
	}
	if err := c.q.Publish(checkpoint(), false); err != nil {
		return err
	}
	c.metrics.FileCaptured(operation, state.Size)
	c.log.DebugContext(ctx, "file change published", "path", name, "operation", operation, "bytes", state.Size, "chunks", chunks)
	return nil
}

func (c *Collector) allows(operation string) bool {
	return slices.Contains(c.cfg.FileWatch.Events, operation)
}

func hashFile(ctx context.Context, root *os.Root, name string) (string, error) {
	file, err := root.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	buffer := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := file.Read(buffer)
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func checkpoint() string { return fmt.Sprintf("file:%d", time.Now().UnixNano()) }

func (c *Collector) manifestPath() string { return filepath.Join(c.cfg.DataDir, "file-manifest.json") }

func (c *Collector) loadManifest() (map[string]fileState, error) {
	payload, err := os.ReadFile(c.manifestPath())
	if errors.Is(err, os.ErrNotExist) {
		return make(map[string]fileState), nil
	}
	if err != nil {
		return nil, err
	}
	var value manifest
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("decode file manifest: %w", err)
	}
	if value.Files == nil {
		value.Files = make(map[string]fileState)
	}
	return value.Files, nil
}

func (c *Collector) saveManifest(files map[string]fileState) (result error) {
	payload, err := json.Marshal(manifest{Files: files})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(c.cfg.DataDir, ".file-manifest-*")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.Remove(temporary.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return errors.Join(err, temporary.Close())
	}
	if _, err := temporary.Write(payload); err != nil {
		return errors.Join(err, temporary.Close())
	}
	if err := temporary.Sync(); err != nil {
		return errors.Join(err, temporary.Close())
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	target := c.manifestPath()
	if err := os.Rename(temporary.Name(), target); err == nil {
		return nil
	} else if runtime.GOOS != "windows" {
		return err
	}
	// Windows Rename does not replace an existing file.
	if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(temporary.Name(), target)
}
