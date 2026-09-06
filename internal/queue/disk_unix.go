//go:build !windows

package queue

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func FreeBytes(path string) (uint64, error) {
	var st unix.Statfs_t
	err := unix.Statfs(path, &st)
	return st.Bavail * uint64(st.Bsize), err
}
