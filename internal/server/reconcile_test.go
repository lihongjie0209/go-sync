package server

import (
	"testing"
	"time"

	"go-sync/internal/reconcile"
	syncv1 "go-sync/internal/rpc/syncv1"
	"go-sync/internal/serverconfig"
)

func TestSameDigests(t *testing.T) {
	source := []*syncv1.BucketDigest{{Bucket: 0, Rows: 2, Digest: "a"}, {Bucket: 1, Rows: 0, Digest: "b"}}
	target := []reconcile.Digest{{Bucket: 0, Rows: 2, Hash: "a"}, {Bucket: 1, Rows: 0, Hash: "b"}}
	if equal, mismatch := sameDigests(source, target); !equal || mismatch != 0 {
		t.Fatalf("equal digests reported %d mismatches", mismatch)
	}
	target[0].Rows++
	if equal, mismatch := sameDigests(source, target); equal || mismatch != 1 {
		t.Fatalf("different digests reported equal=%v mismatches=%d", equal, mismatch)
	}
}

func TestReconcileDueHonorsDailyTime(t *testing.T) {
	service := &Service{cfg: serverconfig.Config{Reconcile: serverconfig.Reconcile{Enabled: true, DailyAt: "02:00", Timezone: "Asia/Shanghai"}}}
	before := time.Date(2026, 9, 13, 1, 59, 0, 0, time.FixedZone("CST", 8*60*60))
	if service.reconcileDue("s", before) {
		t.Fatal("reconciliation ran before configured time")
	}
	if !service.reconcileDue("s", before.Add(2*time.Minute)) {
		t.Fatal("reconciliation did not become due")
	}
	if service.reconcileDue("s", before.Add(3*time.Minute)) {
		t.Fatal("reconciliation ran twice on the same day")
	}
}
