// Package fileembed applies explicitly configured path-to-content transforms.
package fileembed

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go-sync/internal/config"
	"go-sync/internal/event"
)

// Apply replaces configured row column values with Base64 while retaining the
// original database path in SourceValue. Keys are deliberately never changed.
func Apply(cfg config.Config, row *event.Row) error {
	for _, rule := range cfg.FileColumns {
		if rule.Schema != row.Schema || rule.Table != row.Table {
			continue
		}
		for i := range row.Columns {
			column := &row.Columns[i]
			if column.Name != rule.Column {
				continue
			}
			if column.Value == nil {
				continue
			}
			encoded, err := read(rule, *column.Value)
			if err != nil {
				return fmt.Errorf("embed file for %s.%s.%s: %w", rule.Schema, rule.Table, rule.Column, err)
			}
			original := *column.Value
			column.Value, column.SourceValue, column.Encoding = &encoded, &original, "base64"
		}
	}
	return nil
}

func read(rule config.FileColumn, value string) (string, error) {
	if value == "" {
		return "", errors.New("path is empty")
	}
	rel := filepath.Clean(value)
	if filepath.IsAbs(rel) {
		var err error
		rel, err = filepath.Rel(filepath.Clean(rule.RootDir), rel)
		if err != nil {
			return "", errors.New("referenced file escapes root_dir")
		}
	}
	if !filepath.IsLocal(rel) || rel == "." || strings.ContainsRune(rel, 0) {
		return "", errors.New("referenced file escapes root_dir")
	}
	root, err := os.OpenRoot(rule.RootDir)
	if err != nil {
		return "", errors.New("root directory is unavailable")
	}
	defer root.Close()
	f, err := root.Open(rel)
	if err != nil {
		return "", errors.New("referenced file cannot be opened")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("reference is not a regular file")
	}
	if info.Size() > rule.MaxBytes {
		return "", errors.New("referenced file exceeds max_bytes")
	}
	data, err := io.ReadAll(io.LimitReader(f, rule.MaxBytes+1))
	if err != nil {
		return "", errors.New("referenced file cannot be read")
	}
	if int64(len(data)) > rule.MaxBytes {
		return "", errors.New("referenced file exceeds max_bytes")
	}
	return base64.StdEncoding.EncodeToString(data), nil
}
