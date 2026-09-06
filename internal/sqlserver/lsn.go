// Package sqlserver captures a locking baseline followed by committed CDC
// transactions. It never provisions CDC or changes database settings.
package sqlserver

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
)

type lsn [10]byte

func parseLSN(s string) (lsn, error) {
	var value lsn
	if len(s) != 22 || !strings.HasPrefix(s, "0x") {
		return value, errors.New("invalid sqlserver lsn")
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return value, errors.New("invalid sqlserver lsn")
	}
	copy(value[:], b)
	return value, nil
}

func binaryLSN(b []byte) (lsn, error) {
	var value lsn
	if len(b) != len(value) {
		return value, errors.New("missing or malformed sqlserver lsn")
	}
	copy(value[:], b)
	return value, nil
}

func (l lsn) String() string        { return "0x" + hex.EncodeToString(l[:]) }
func (l lsn) compare(other lsn) int { return bytes.Compare(l[:], other[:]) }
