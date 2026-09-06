package queue

import "golang.org/x/sys/windows"

// bbolt flushes the file handle on Windows. Deployment guarantees additionally
// require a local filesystem with durable file creation (NTFS), not a network share.
func syncDirectory(string) error { return nil }

func FreeBytes(path string) (uint64, error) {
	p, e := windows.UTF16PtrFromString(path)
	if e != nil {
		return 0, e
	}
	var free uint64
	e = windows.GetDiskFreeSpaceEx(p, &free, nil, nil)
	return free, e
}
