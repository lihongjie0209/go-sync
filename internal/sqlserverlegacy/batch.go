package sqlserverlegacy

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go-sync/internal/config"
	"go-sync/internal/delivery"
	"go-sync/internal/event"
	"go-sync/internal/fileembed"
	"go-sync/internal/queue"
	"go-sync/internal/telemetry"
)

type batch struct {
	cfg     config.Config
	q       *queue.Store
	metrics *telemetry.Metrics
	message event.Message
	size    int
}

func (b *batch) append(ctx context.Context, message event.Message) error {
	defer b.metrics.Backpressure(false)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := b.q.Append(message)
		if !errors.Is(err, queue.ErrFull) {
			return err
		}
		b.metrics.QueueFull()
		state, stateErr := b.q.State()
		if stateErr != nil {
			return stateErr
		}
		if state.ReadySeq == state.DeliveredSeq {
			return errors.New("insufficient queue/disk capacity for a complete legacy snapshot or event")
		}
		b.metrics.Backpressure(true)
		if err := delivery.Wait(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

func (b *batch) add(ctx context.Context, row event.Row) error {
	if err := fileembed.Apply(b.cfg, &row); err != nil {
		return err
	}
	raw, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if len(raw) > b.cfg.MaxRowBytes {
		return errors.New("row exceeds max_row_bytes")
	}
	if len(b.message.Rows) > 0 && b.size+len(raw) > b.cfg.BatchBytes {
		if err := b.flush(ctx); err != nil {
			return err
		}
	}
	b.message.Rows = append(b.message.Rows, row)
	b.size += len(raw)
	if len(b.message.Rows) >= b.cfg.BatchRows || b.size >= b.cfg.BatchBytes {
		return b.flush(ctx)
	}
	return nil
}

func (b *batch) flush(ctx context.Context) error {
	if len(b.message.Rows) == 0 {
		return nil
	}
	if err := b.append(ctx, b.message); err != nil {
		return err
	}
	b.message.Rows = nil
	b.message.Chunk++
	b.size = 0
	return nil
}
