package fileembed

import (
	"os"
	"path/filepath"
	"testing"

	"go-sync/internal/config"
	"go-sync/internal/event"
)

func TestApplyPreservesPathAndEmbedsBase64(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "hello.bin"), []byte{0, 1, 255}, 0600); err != nil {
		t.Fatal(err)
	}
	path := "hello.bin"
	row := event.Row{Schema: "db", Table: "docs", Operation: "insert", Columns: []event.Column{{Name: "path", Type: "varchar", Value: &path}}}
	cfg := config.Config{FileColumns: []config.FileColumn{{Schema: "db", Table: "docs", Column: "path", RootDir: root, MaxBytes: 3}}}
	if err := Apply(cfg, &row); err != nil {
		t.Fatal(err)
	}
	column := row.Columns[0]
	if column.Value == nil || *column.Value != "AAH/" || column.SourceValue == nil || *column.SourceValue != "hello.bin" || column.Encoding != "base64" {
		t.Fatalf("unexpected embedded column: %+v", column)
	}
}

func TestApplyRejectsEscapeAndOversize(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{filepath.Join(outside, "secret"), "missing"} {
		value := value
		t.Run(value, func(t *testing.T) {
			row := event.Row{Schema: "db", Table: "docs", Columns: []event.Column{{Name: "path", Value: &value}}}
			cfg := config.Config{FileColumns: []config.FileColumn{{Schema: "db", Table: "docs", Column: "path", RootDir: root, MaxBytes: 2}}}
			if err := Apply(cfg, &row); err == nil {
				t.Fatal("unsafe file reference accepted")
			}
		})
	}
}
