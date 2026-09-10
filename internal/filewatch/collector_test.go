package filewatch

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"go-sync/internal/config"
	"go-sync/internal/event"
	"go-sync/internal/queue"
)

func TestPublishChangesHonorsCreateOnlyAndChunks(t *testing.T) {
	root := t.TempDir()
	data := t.TempDir()
	cfg := config.Defaults()
	cfg.SourceType = "files"
	cfg.SourceID = "files-test"
	cfg.DataDir = data
	cfg.MaxRowBytes = 256 << 10
	cfg.BatchBytes = 64 << 10
	cfg.QueueBytes = 8 << 20
	cfg.ReserveBytes = 0
	cfg.FileWatch = config.FileWatch{RootDir: root, ScanInterval: "1s", Events: []string{"create"}, MaxFileBytes: 1 << 20, ChunkBytes: 64 << 10}
	q, err := queue.Open(data, cfg.QueueBytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if err := q.Initialize(queue.State{ProtocolVersion: "v2", SourceID: cfg.SourceID, Generation: "g", Fingerprint: cfg.Fingerprint()}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"snapshot_begin", "snapshot_end"} {
		if err := q.Append(event.Message{Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Publish("file:1", true); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 70<<10)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(root, "a.bin"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	collector := New(cfg, q, slog.New(slog.NewTextHandler(io.Discard, nil)))
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = watcher.Close() })
	current, err := collector.scan(context.Background(), watcher, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.publishChanges(context.Background(), map[string]fileState{}, current); err != nil {
		t.Fatal(err)
	}
	_, _, ready, err := q.Bounds()
	if err != nil || ready != 6 { // snapshot pair + begin + two chunks + end
		t.Fatalf("ready = %d, error = %v", ready, err)
	}
	if err := collector.publishChanges(context.Background(), current, map[string]fileState{}); err != nil {
		t.Fatal(err)
	}
	_, _, afterDelete, err := q.Bounds()
	if err != nil || afterDelete != ready {
		t.Fatalf("create-only collector published delete: ready=%d error=%v", afterDelete, err)
	}
}

func TestRunInitialScanAndCleanCancellation(t *testing.T) {
	root := t.TempDir()
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "initial.txt"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.SourceType, cfg.SourceID, cfg.DataDir = "files", "files-run", data
	cfg.FileWatch = config.FileWatch{RootDir: root, ScanInterval: "1s", Events: []string{"create", "update", "delete"}, MaxFileBytes: 1 << 20, ChunkBytes: 64 << 10}
	q, err := queue.Open(data, 8<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, q, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		_, _, ready, err := q.Bounds()
		if err != nil {
			t.Fatal(err)
		}
		if ready >= 5 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("collector stopped early: %v", err)
		case <-deadline.C:
			t.Fatal("initial file was not published")
		case <-poll.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not stop after cancellation")
	}
}
