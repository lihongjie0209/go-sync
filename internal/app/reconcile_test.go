package app

import (
	"context"
	"testing"
	"time"

	"go-sync/internal/queue"
	syncv1 "go-sync/internal/rpc/syncv1"
)

func TestCaptureControllerRepairStartsNewSnapshot(t *testing.T) {
	q, err := queue.Open(t.TempDir(), 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.Initialize(queue.State{SourceID: "s", Generation: "g", Fingerprint: "fp"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish("checkpoint", true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	run := func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	scan := func(_ context.Context, request *syncv1.ReconcileRequest) (*syncv1.ReconcileResult, error) {
		return &syncv1.ReconcileResult{RequestId: request.RequestId}, nil
	}
	controller := newCaptureController(ctx, q, run, scan)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("capture did not start")
	}
	request := &syncv1.ReconcileRequest{RequestId: "r1"}
	if _, err := controller.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := controller.Repair(context.Background(), request.RequestId); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("capture did not restart")
	}
	state, err := q.State()
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != "snapshot" || state.Generation != "" {
		t.Fatalf("unexpected repaired state: %+v", state)
	}
	if err := controller.Resume(context.Background(), "stale"); err == nil {
		t.Fatal("stale resume was accepted")
	}
}
