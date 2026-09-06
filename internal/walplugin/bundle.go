// Package walplugin installs verified, embedded Windows wal2json release assets.
package walplugin

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"debug/pe"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
)

const upstreamTag = "wal2json_2_6"
const upstreamCommit = "75629c2e1e81a12350cc9d63782fc53252185d8d"

//go:embed assets/*.zip assets/SHA256SUMS* assets/coverage.json
var assets embed.FS

type manifest struct {
	Tag     string `json:"upstream_tag"`
	Commit  string `json:"upstream_commit"`
	Version string `json:"postgresql_version"`
	Arch    string `json:"architecture"`
	DLLHash string `json:"dll_sha256"`
	Test    string `json:"smoke_test"`
}

func versionName(version int) (string, error) {
	is94 := version >= 90400 && version <= 90426
	is95 := version >= 90500 && version <= 90525
	is96 := version >= 90600 && version <= 90624
	if is94 || is95 || is96 {
		return fmt.Sprintf("9.%d.%d", version/100%100, version%100), nil
	}
	return "", fmt.Errorf("no verified embedded wal2json for server_version_num %d; install a matching plugin manually", version)
}

func bundledDLL(version int, arch string) ([]byte, error) {
	pgVersion, err := versionName(version)
	if err != nil {
		return nil, err
	}
	if arch != "x86" && arch != "x64" {
		return nil, errors.New("unsupported postgres executable architecture")
	}
	coverage, err := assets.ReadFile("assets/coverage.json")
	if err != nil {
		return nil, fmt.Errorf("read embedded coverage: %w", err)
	}
	var targets []struct {
		PG     string `json:"pg"`
		Arch   string `json:"arch"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(coverage, &targets); err != nil {
		return nil, fmt.Errorf("read embedded coverage: %w", err)
	}
	var isVerified bool
	for _, target := range targets {
		if target.PG == pgVersion && target.Arch == arch && target.Status == "verified" {
			isVerified = true
		}
	}
	if !isVerified {
		return nil, errors.New("target was not verified in the embedded release coverage")
	}
	name := upstreamTag + "-pg" + pgVersion + "-windows-" + arch + ".zip"
	b, err := assets.ReadFile("assets/" + name)
	if err != nil {
		return nil, fmt.Errorf("read embedded package: %w", err)
	}
	checksums, err := fs.Glob(assets, "assets/SHA256SUMS*")
	if err != nil {
		return nil, fmt.Errorf("list embedded checksums: %w", err)
	}
	var expected string
	for _, path := range checksums {
		list, err := assets.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read embedded checksums: %w", err)
		}
		for line := range strings.SplitSeq(string(list), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[1] == name {
				if expected != "" && expected != fields[0] {
					return nil, errors.New("conflicting embedded release checksums")
				}
				expected = fields[0]
			}
		}
	}
	if expected == "" || digest(b) != expected {
		return nil, errors.New("embedded release checksum mismatch")
	}
	return unpackDLL(b, manifest{Tag: upstreamTag, Commit: upstreamCommit, Version: pgVersion, Arch: arch})
}

func unpackDLL(archive []byte, expected manifest) ([]byte, error) {
	z, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return nil, fmt.Errorf("open embedded zip: %w", err)
	}
	files := map[string][]byte{}
	for _, f := range z.File {
		// Extract only these exact entries into memory, never archive paths to disk.
		if f.Name != "wal2json.dll" && f.Name != "manifest.json" {
			continue
		}
		if _, exists := files[f.Name]; exists {
			return nil, errors.New("duplicate embedded zip entry")
		}
		if f.UncompressedSize64 > 4<<20 {
			return nil, errors.New("embedded zip entry exceeds size limit")
		}
		r, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("open embedded entry: %w", err)
		}
		b, readErr := io.ReadAll(io.LimitReader(r, (4<<20)+1))
		if err := errors.Join(readErr, r.Close()); err != nil {
			return nil, fmt.Errorf("read embedded entry: %w", err)
		}
		if len(b) > 4<<20 {
			return nil, errors.New("embedded zip entry exceeds size limit")
		}
		files[f.Name] = b
	}
	var m manifest
	if err := json.Unmarshal(bytes.TrimPrefix(files["manifest.json"], []byte{0xef, 0xbb, 0xbf}), &m); err != nil {
		return nil, fmt.Errorf("decode embedded manifest: %w", err)
	}
	identityMatches := m.Tag == expected.Tag && m.Commit == expected.Commit
	targetMatches := m.Version == expected.Version && m.Arch == expected.Arch
	if !identityMatches || !targetMatches || m.Test != "passed" {
		return nil, errors.New("embedded manifest target or provenance mismatch")
	}
	dll := files["wal2json.dll"]
	if len(dll) == 0 || digest(dll) != m.DLLHash {
		return nil, errors.New("embedded dll checksum mismatch")
	}
	p, err := pe.NewFile(bytes.NewReader(dll))
	if err != nil {
		return nil, fmt.Errorf("inspect embedded dll: %w", err)
	}
	arch, err := machineArch(p.Machine)
	if err != nil {
		return nil, err
	}
	if arch != expected.Arch || p.Characteristics&pe.IMAGE_FILE_DLL == 0 {
		return nil, errors.New("embedded dll pe target mismatch")
	}
	return dll, nil
}

func machineArch(machine uint16) (string, error) {
	switch machine {
	case pe.IMAGE_FILE_MACHINE_I386:
		return "x86", nil
	case pe.IMAGE_FILE_MACHINE_AMD64:
		return "x64", nil
	default:
		return "", errors.New("postgres executable must be x86 or x64")
	}
}

func digest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// installAbsent publishes a complete file atomically without replacing any entry.
// A hard link is deliberately used: filesystems without this guarantee fail closed.
func installAbsent(dir string, dll []byte) (installed bool, result error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return false, fmt.Errorf("open postgres library directory: %w", err)
	}
	defer func() { result = errors.Join(result, root.Close()) }()
	if _, err := root.Lstat("wal2json.dll"); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect existing plugin: %w", err)
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return false, fmt.Errorf("generate temporary plugin name: %w", err)
	}
	name := ".go-sync-wal2json-" + hex.EncodeToString(random[:])
	f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return false, fmt.Errorf("create temporary plugin (check directory permissions): %w", err)
	}
	defer func() { result = errors.Join(result, root.Remove(name)) }()
	_, writeErr := f.Write(dll)
	err = errors.Join(writeErr, f.Sync(), f.Close())
	if err != nil {
		return false, fmt.Errorf("write plugin: %w", err)
	}
	if err := root.Link(name, "wal2json.dll"); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("publish plugin without overwrite (requires hard-link support): %w", err)
	}
	return true, nil
}
