package serverconfig

import "testing"

func TestFileStorageValidationDefaultsAndFilters(t *testing.T) {
	storage := FileStorage{Backend: "directory", Directory: t.TempDir()}
	if err := storage.validate("files"); err != nil {
		t.Fatal(err)
	}
	if storage.MaxFileBytes != 1<<30 || len(storage.Events) != 3 {
		t.Fatalf("defaults were not applied: %+v", storage)
	}
	storage.Events = []string{"delete", "delete"}
	if err := storage.validate("files"); err == nil {
		t.Fatal("duplicate event was accepted")
	}
}

func TestStorageKeysOverlap(t *testing.T) {
	root := t.TempDir()
	if !storageKeysOverlap(fileStorageKey(FileStorage{Backend: "directory", Directory: root, Prefix: "a"}), fileStorageKey(FileStorage{Backend: "directory", Directory: root, Prefix: "a/nested"})) {
		t.Fatal("nested directories were not detected as overlapping")
	}
	first := fileStorageKey(FileStorage{Backend: "s3", Endpoint: "objects.example", Bucket: "files", Prefix: "tenant"})
	second := fileStorageKey(FileStorage{Backend: "oss", Endpoint: "objects.example", Bucket: "files", Prefix: "tenant/sub"})
	if !storageKeysOverlap(first, second) {
		t.Fatal("nested object prefixes were not detected as overlapping")
	}
}
