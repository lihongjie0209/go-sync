package capture

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/event"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

// batcher is owned by the capture goroutine. Invisible queue chunks may spill to
// disk, but cannot be sent until the snapshot or source transaction is complete.
type batcher struct {
	cfg     config.Config
	q       *queue.Store
	msg     event.Message
	size    int
	metrics *telemetry.Metrics
}

func (b *batcher) append(ctx context.Context, m event.Message) error {
	waiting := false
	defer func() {
		if waiting {
			b.metrics.Backpressure(false)
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := b.q.Append(m)
		if !errors.Is(err, queue.ErrFull) {
			return err
		}
		b.metrics.QueueFull()
		st, e := b.q.State()
		if e != nil {
			return e
		}
		if st.ReadySeq == st.DeliveredSeq {
			return errors.New("insufficient queue/disk capacity for a complete snapshot or transaction; increase capacity")
		}
		if !waiting {
			b.metrics.Backpressure(true)
			waiting = true
		}
		if e := delivery.Wait(ctx, 250*time.Millisecond); e != nil {
			return e
		}
	}
}

func (b *batcher) add(ctx context.Context, row event.Row) error {
	raw, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if len(raw) > b.cfg.MaxRowBytes {
		return errors.New("row exceeds max_row_bytes")
	}
	if len(b.msg.Rows) > 0 && (len(b.msg.Rows) >= b.cfg.BatchRows || b.size+len(raw) > b.cfg.BatchBytes) {
		if err := b.flush(ctx); err != nil {
			return err
		}
	}
	b.msg.Rows = append(b.msg.Rows, row)
	b.size += len(raw)
	if b.size >= b.cfg.BatchBytes || len(b.msg.Rows) >= b.cfg.BatchRows {
		return b.flush(ctx)
	}
	return nil
}
func (b *batcher) flush(ctx context.Context) error {
	if len(b.msg.Rows) == 0 {
		return nil
	}
	if err := b.append(ctx, b.msg); err != nil {
		return err
	}
	b.msg.Rows = nil
	b.size = 0
	b.msg.Chunk++
	return nil
}
