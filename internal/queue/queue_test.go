package queue

import (
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"go-sync/internal/event"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	q, e := Open(t.TempDir(), 1<<20, 0)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := q.Close(); e != nil {
			t.Error(e)
		}
	})
	if e := q.Initialize(State{SourceID: "test", Generation: "g"}); e != nil {
		t.Fatal(e)
	}
	return q
}

func TestAcknowledgedMessagesRemainReplayableAndAreBounded(t *testing.T) {
	q := openTest(t)
	q.ConfigureReplay(time.Hour, 1<<20)
	if err := q.Append(event.Message{Kind: "snapshot_end"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish("0/1", true); err != nil {
		t.Fatal(err)
	}
	message, _, err := q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(message.Seq, message.ID); err != nil {
		t.Fatal(err)
	}
	replayed, _, err := q.Get(1)
	if err != nil || replayed.ID != message.ID {
		t.Fatalf("acknowledged message is not replayable: %+v %v", replayed, err)
	}
	q.ConfigureReplay(time.Nanosecond, 1)
	if err := q.Append(event.Message{Kind: "transaction_end"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish("0/2", false); err != nil {
		t.Fatal(err)
	}
	message, _, err = q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(message.Seq, message.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Get(1); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expired replay retained: %v", err)
	}
}

func TestPublishAndRecovery(t *testing.T) {
	q := openTest(t)
	if e := q.Append(event.Message{Kind: "snapshot_end"}); e != nil {
		t.Fatal(e)
	}
	if _, _, e := q.Peek(); !errors.Is(e, ErrEmpty) {
		t.Fatalf("unsealed snapshot visible: %v", e)
	}
	if e := q.Publish("0/10", true); e != nil {
		t.Fatal(e)
	}
	if e := q.Append(event.Message{Kind: "transaction_rows"}); e != nil {
		t.Fatal(e)
	}
	if e := q.Recover(); e != nil {
		t.Fatal(e)
	}
	st, e := q.State()
	if e != nil {
		t.Fatal(e)
	}
	if st.NextSeq != 1 || st.ReadySeq != 1 || st.DurableLSN != "0/10" {
		t.Fatalf("bad recovery: %+v", st)
	}
	m, _, e := q.Peek()
	if e != nil {
		t.Fatal(e)
	}
	if e := q.Ack(m.Seq, "wrong"); e == nil {
		t.Fatal("accepted wrong ack")
	}
	if e := q.Ack(m.Seq+1, m.ID); e == nil {
		t.Fatal("accepted non-contiguous ack")
	}
	if e := q.Ack(m.Seq, m.ID); e != nil {
		t.Fatal(e)
	}
	st, e = q.State()
	if e != nil {
		t.Fatal(e)
	}
	if st.Bytes != 0 || st.DeliveredSeq != 1 {
		t.Fatalf("not reclaimed: %+v", st)
	}
}

func TestFinalizeSnapshotLSNRewritesOnlyInvisibleMessages(t *testing.T) {
	q, err := Open(t.TempDir(), 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if err := q.Initialize(State{SourceID: "source", Generation: "generation"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Append(event.Message{Kind: "snapshot_begin"}); err != nil {
		t.Fatal(err)
	}
	if err := q.FinalizeSnapshotLSN("0x0102"); err != nil {
		t.Fatal(err)
	}
	st, err := q.State()
	if err != nil || st.SnapshotLSN != "0x0102" || st.ReadySeq != 0 {
		t.Fatalf("unexpected state: %+v, %v", st, err)
	}
	if err := q.Publish("0x0102", true); err != nil {
		t.Fatal(err)
	}
	message, _, err := q.Peek()
	if err != nil || message.LSN != "0x0102" {
		t.Fatalf("unexpected message: %+v, %v", message, err)
	}
	if err := q.FinalizeSnapshotLSN("0x0304"); err == nil {
		t.Fatal("rewrote a published snapshot")
	}
}

func TestSourceCleanupCheckpoint(t *testing.T) {
	t.Parallel()
	q, err := Open(t.TempDir(), 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := q.Initialize(State{SourceID: "legacy", Generation: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Append(event.Message{Kind: "transaction_end"}); err != nil {
		t.Fatal(err)
	}
	if err := q.PublishSource("legacy:00000000000000000001", false, "statement-token"); err != nil {
		t.Fatal(err)
	}
	state, err := q.State()
	if err != nil || state.SourceCleanup != "statement-token" || state.ReadySeq != 1 {
		t.Fatalf("unexpected state: %+v, %v", state, err)
	}
	if err := q.ClearSourceCleanup("wrong-token"); err == nil {
		t.Fatal("mismatched cleanup token accepted")
	}
	if err := q.ClearSourceCleanup("statement-token"); err != nil {
		t.Fatal(err)
	}
	state, err = q.State()
	if err != nil || state.SourceCleanup != "" || state.DurableLSN == "" {
		t.Fatalf("cleanup changed checkpoint: %+v, %v", state, err)
	}
}

func TestCapacityRollback(t *testing.T) {
	q := openTest(t)
	q.maxBytes = 1
	if e := q.Append(event.Message{Kind: "snapshot_end"}); !errors.Is(e, ErrFull) {
		t.Fatalf("got %v", e)
	}
	st, e := q.State()
	if e != nil {
		t.Fatal(e)
	}
	if st.NextSeq != 0 || st.Bytes != 0 {
		t.Fatal("capacity failure changed checkpoint")
	}
}

func TestExclusiveOwner(t *testing.T) {
	q := openTest(t)
	other, e := Open(q.dir, 1<<20, 0)
	if e == nil {
		other.Close()
		t.Fatal("second owner acquired queue")
	}
}

// The child exits without closing the database, exercising persisted crash state.
func TestCrashHelper(t *testing.T) {
	dir := os.Getenv("GO_SYNC_CRASH_DIR")
	if dir == "" {
		return
	}
	q, e := Open(dir, 1<<20, 0)
	if e != nil {
		os.Exit(10)
	}
	if e = q.Initialize(State{SourceID: "test", Generation: "generation"}); e != nil {
		os.Exit(11)
	}
	if e = q.Append(event.Message{Kind: "snapshot_end"}); e != nil {
		os.Exit(12)
	}
	mode := os.Getenv("GO_SYNC_CRASH_POINT")
	if mode == "staged" {
		os.Exit(0)
	}
	if e = q.Publish("0/20", true); e != nil {
		os.Exit(13)
	}
	if mode == "published" {
		os.Exit(0)
	}
	m, _, e := q.Peek()
	if e != nil {
		os.Exit(14)
	}
	if e = q.Ack(m.Seq, m.ID); e != nil {
		os.Exit(15)
	}
	os.Exit(0)
}

func TestCrashBoundaries(t *testing.T) {
	for _, point := range []string{"staged", "published", "acked"} {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
			cmd.Env = append(os.Environ(), "GO_SYNC_CRASH_DIR="+dir, "GO_SYNC_CRASH_POINT="+point)
			if out, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("child: %v %s", e, out)
			}
			q, e := Open(dir, 1<<20, 0)
			if e != nil {
				t.Fatal(e)
			}
			defer q.Close()
			if e := q.Recover(); e != nil {
				t.Fatal(e)
			}
			st, e := q.State()
			if e != nil {
				t.Fatal(e)
			}
			m, _, e := q.Peek()
			switch point {
			case "staged":
				if st.DurableLSN != "" || st.Bytes != 0 || !errors.Is(e, ErrEmpty) {
					t.Fatalf("staged survived: %+v %v", st, e)
				}
			case "published":
				if e != nil || m.Seq != 1 || st.DurableLSN != "0/20" {
					t.Fatalf("durable commit lost: %+v %v", st, e)
				}
			case "acked":
				if st.DeliveredSeq != 1 || st.Bytes != 0 || !errors.Is(e, ErrEmpty) {
					t.Fatalf("ack lost: %+v %v", st, e)
				}
			}
		})
	}
}
