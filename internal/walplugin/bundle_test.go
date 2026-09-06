package walplugin

import (
	"archive/zip"
	"bytes"
	"debug/pe"
	"os"
	"path/filepath"
	"testing"
)

func TestBundledTargets(t *testing.T) {
	t.Parallel()
	versions := []int{}
	for minor, last := range map[int]int{4: 26, 5: 25, 6: 24} {
		for patch := 0; patch <= last; patch++ {
			versions = append(versions, 90000+minor*100+patch)
		}
	}
	for _, version := range versions {
		for _, arch := range []string{"x86", "x64"} {
			name, err := versionName(version)
			if err != nil {
				t.Fatal(err)
			}
			t.Run(name+"/"+arch, func(t *testing.T) {
				t.Parallel()
				dll, err := bundledDLL(version, arch)
				if err != nil {
					t.Fatal(err)
				}
				if len(dll) < 1024 {
					t.Fatal("empty dll")
				}
			})
		}
	}
}

func TestUnverifiedTargetsRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		version int
		arch    string
	}{
		{name: "unverified patch", version: 90526, arch: "x86"},
		{name: "new major", version: 170000, arch: "x64"},
		{name: "arm", version: 90502, arch: "arm64"},
		{name: "path injection", version: 90502, arch: "../x86"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := bundledDLL(tc.version, tc.arch); err == nil {
				t.Fatal("unsupported target accepted")
			}
		})
	}
}

func TestMachineArch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		machine  uint16
		expected string
	}{
		{name: "x86", machine: pe.IMAGE_FILE_MACHINE_I386, expected: "x86"},
		{name: "x64", machine: pe.IMAGE_FILE_MACHINE_AMD64, expected: "x64"},
		{name: "arm", machine: pe.IMAGE_FILE_MACHINE_ARM64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := machineArch(tc.machine)
			if actual != tc.expected || (err != nil) != (tc.expected == "") {
				t.Fatalf("got %q, %v", actual, err)
			}
		})
	}
}

func TestInstallAbsentDoesNotOverwrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	installed, err := installAbsent(dir, []byte("original"))
	if err != nil || !installed {
		t.Fatalf("first installation: %v %v", installed, err)
	}
	installed, err = installAbsent(dir, []byte("replacement"))
	if err != nil || installed {
		t.Fatalf("repeat installation: %v %v", installed, err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "wal2json.dll"))
	if err != nil || string(got) != "original" {
		t.Fatalf("existing file changed: %q %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary files leaked: %v %v", entries, err)
	}
}

func TestInstallAbsentPreservesDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "wal2json.dll"), 0700); err != nil {
		t.Fatal(err)
	}
	installed, err := installAbsent(dir, []byte("data"))
	if err != nil || installed {
		t.Fatalf("existing directory replaced: %v %v", installed, err)
	}
}

func TestInstallAbsentMissingDirectory(t *testing.T) {
	t.Parallel()
	if _, err := installAbsent(filepath.Join(t.TempDir(), "missing"), []byte("data")); err == nil {
		t.Fatal("silently created an unverified directory")
	}
}

func TestUnpackRejectsTampering(t *testing.T) {
	t.Parallel()
	archive, err := assets.ReadFile("assets/wal2json_2_6-pg9.5.2-windows-x86.zip")
	if err != nil {
		t.Fatal(err)
	}
	expected := manifest{Tag: upstreamTag, Commit: upstreamCommit, Version: "9.5.2", Arch: "x86"}
	for _, name := range []string{"wrong version", "wrong architecture", "wrong commit", "corrupt zip", "duplicate dll", "modified dll"} {
		t.Run(name, func(t *testing.T) {
			want := expected
			data := archive
			switch name {
			case "wrong version":
				want.Version = "9.5.25"
			case "wrong architecture":
				want.Arch = "x64"
			case "wrong commit":
				want.Commit = "not-upstream"
			case "corrupt zip":
				data = []byte("invalid")
			default:
				z, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
				if err != nil {
					t.Fatal(err)
				}
				var b bytes.Buffer
				w := zip.NewWriter(&b)
				for _, f := range z.File {
					if name == "modified dll" && f.Name == "wal2json.dll" {
						continue
					}
					if err := w.Copy(f); err != nil {
						t.Fatal(err)
					}
				}
				entry := "wal2json.dll"
				f, err := w.Create(entry)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte("tampered")); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				data = b.Bytes()
			}
			if _, err := unpackDLL(data, want); err == nil {
				t.Fatal("tampered package accepted")
			}
		})
	}
}
