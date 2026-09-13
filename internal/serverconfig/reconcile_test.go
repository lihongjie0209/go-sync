package serverconfig

import "testing"

func TestReconcileDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	c := Defaults()
	if !c.Reconcile.Enabled || c.Reconcile.Buckets != 256 || c.Reconcile.Timeout != "30m" {
		t.Fatalf("unexpected reconcile defaults: %+v", c.Reconcile)
	}
	c.Reconcile.DryRun = true
	if err := c.Reconcile.validate(); err == nil {
		t.Fatal("dry-run and auto-repair were both accepted")
	}
}
