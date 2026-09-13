package app

import (
	"context"
	"errors"
	"sync"

	"go-sync/internal/queue"
	syncv1 "go-sync/internal/rpc/syncv1"
)

type reconcileScanner func(context.Context, *syncv1.ReconcileRequest) (*syncv1.ReconcileResult, error)

// captureController serializes capture lifecycle changes with reconciliation.
// Delivery remains active while capture is paused, allowing it to drain the
// durable queue before the server requests a hash.
type captureController struct {
	parent context.Context
	queue  *queue.Store
	run    func(context.Context) error
	scan   reconcileScanner
	fatal  chan error

	mu        sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}
	pausedFor string
}

func newCaptureController(parent context.Context, q *queue.Store, run func(context.Context) error, scan reconcileScanner) *captureController {
	c := &captureController{parent: parent, queue: q, run: run, scan: scan, fatal: make(chan error, 1)}
	c.startLocked()
	return c
}

func (c *captureController) startLocked() {
	ctx, cancel := context.WithCancel(c.parent)
	done := make(chan struct{})
	c.cancel, c.done = cancel, done
	go func() {
		err := c.run(ctx)
		close(done)
		if err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil {
			select {
			case c.fatal <- err:
			default:
			}
		}
	}()
}

func (c *captureController) stop(ctx context.Context) error {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.cancel = nil
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (c *captureController) Reconcile(ctx context.Context, request *syncv1.ReconcileRequest) (*syncv1.ReconcileResult, error) {
	if c.scan == nil {
		return nil, errors.New("source engine does not support reconciliation")
	}
	c.mu.Lock()
	if c.pausedFor != "" && c.pausedFor != request.RequestId {
		c.mu.Unlock()
		return nil, errors.New("another reconciliation is active")
	}
	c.pausedFor = request.RequestId
	c.mu.Unlock()
	if err := c.stop(ctx); err != nil {
		return nil, err
	}
	state, err := c.queue.State()
	if err != nil {
		return nil, err
	}
	if state.NextSeq != state.ReadySeq || state.DeliveredSeq != state.ReadySeq {
		_ = c.Resume(ctx, request.RequestId)
		return nil, queue.ErrNotDrained
	}
	return c.scan(ctx, request)
}

func (c *captureController) Resume(_ context.Context, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pausedFor != requestID {
		return errors.New("reconciliation request is no longer active")
	}
	c.pausedFor = ""
	if c.parent.Err() == nil && c.cancel == nil {
		c.startLocked()
	}
	return nil
}

func (c *captureController) Repair(_ context.Context, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pausedFor != requestID || c.cancel != nil {
		return errors.New("repair requires the matching paused reconciliation")
	}
	if err := c.queue.ResetForSnapshot(); err != nil {
		return err
	}
	c.pausedFor = ""
	if c.parent.Err() == nil {
		c.startLocked()
	}
	return nil
}

func (c *captureController) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-c.fatal:
		return err
	}
}
