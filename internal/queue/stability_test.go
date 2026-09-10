//go:build stability

package queue

import (
	"fmt"
	"os"
	"testing"
	"time"

	"go-sync/internal/event"
)

func TestDurableQueueStability(t *testing.T) {
	duration := 15 * time.Second
	if configured := os.Getenv("GO_SYNC_STABILITY_DURATION"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid GO_SYNC_STABILITY_DURATION %q", configured)
		}
		duration = parsed
	}
	dir := t.TempDir()
	store, err := Open(dir, 64<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	store.ConfigureReplay(time.Nanosecond, 1024)
	if err := store.Initialize(State{SourceID: "stability", Generation: "generation"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(event.Message{Kind: "snapshot_end"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Publish("0/1", true); err != nil {
		t.Fatal(err)
	}
	message, _, err := store.Peek()
	if err != nil || store.Ack(message.Seq, message.ID) != nil {
		t.Fatalf("seal queue: %v", err)
	}

	deadline := time.Now().Add(duration)
	operations := 0
	for time.Now().Before(deadline) {
		if err := store.Append(event.Message{Kind: "transaction_end", Transaction: fmt.Sprintf("tx-%d", operations)}); err != nil {
			t.Fatal(err)
		}
		if err := store.Publish(fmt.Sprintf("0/%X", operations+2), false); err != nil {
			t.Fatal(err)
		}
		message, _, err = store.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Ack(message.Seq, message.ID); err != nil {
			t.Fatal(err)
		}
		operations++
		if operations%250 == 0 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(dir, 64<<20, 0)
			if err != nil {
				t.Fatal(err)
			}
			store.ConfigureReplay(time.Nanosecond, 1024)
			if err := store.Recover(); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := store.State()
	if err != nil {
		t.Fatal(err)
	}
	if operations == 0 || state.DeliveredSeq != uint64(operations+1) || state.ReadySeq != state.DeliveredSeq || state.NextSeq != state.ReadySeq {
		t.Fatalf("unexpected final state after %d operations: %+v", operations, state)
	}
	t.Logf("completed %d durable append/publish/peek/ack cycles with periodic reopen", operations)
}
