package queue

import (
	"errors"
	"testing"

	"go-sync/internal/event"
)

func TestResetForSnapshotRequiresDrainedStream(t *testing.T) {
	q, err := Open(t.TempDir(), 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Initialize(State{SourceID: "source", Generation: "old", Fingerprint: "fp", Slot: "slot"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish("1/2", true); err != nil {
		t.Fatal(err)
	}
	if err := q.ResetForSnapshot(); err != nil {
		t.Fatal(err)
	}
	state, err := q.State()
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != "snapshot" || state.Generation != "" || state.Slot != "slot" || state.Fingerprint != "fp" {
		t.Fatalf("unexpected reset state: %+v", state)
	}

	if err := q.Initialize(State{SourceID: "source", Generation: "next"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Append(event.Message{Kind: "snapshot_begin"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish("2/3", true); err != nil {
		t.Fatal(err)
	}
	if err := q.ResetForSnapshot(); !errors.Is(err, ErrNotDrained) {
		t.Fatalf("got %v, want ErrNotDrained", err)
	}
}
