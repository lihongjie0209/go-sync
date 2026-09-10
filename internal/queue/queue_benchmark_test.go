package queue

import (
	"testing"
	"time"

	"go-sync/internal/event"
)

func BenchmarkDurableAppendPublishPeekAck(b *testing.B) {
	store, err := Open(b.TempDir(), 1<<30, 0)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	store.ConfigureReplay(time.Nanosecond, 1)
	if err := store.Initialize(State{SourceID: "benchmark", Generation: "generation"}); err != nil {
		b.Fatal(err)
	}
	if err := store.Append(event.Message{Kind: "snapshot_end"}); err != nil {
		b.Fatal(err)
	}
	if err := store.Publish("0/1", true); err != nil {
		b.Fatal(err)
	}
	message, _, err := store.Peek()
	if err != nil {
		b.Fatal(err)
	}
	if err := store.Ack(message.Seq, message.ID); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := store.Append(event.Message{Kind: "transaction_end", Transaction: "tx"}); err != nil {
			b.Fatal(err)
		}
		if err := store.Publish("0/2", false); err != nil {
			b.Fatal(err)
		}
		message, _, err := store.Peek()
		if err != nil {
			b.Fatal(err)
		}
		if err := store.Ack(message.Seq, message.ID); err != nil {
			b.Fatal(err)
		}
	}
}
