// Package reconcile defines a database-independent, order-independent row
// digest used by collectors and the PostgreSQL target.
package reconcile

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"math/big"
	"sort"
	"strings"

	"go-sync/internal/event"
)

type Digest struct {
	Bucket uint32 `json:"bucket"`
	Rows   uint64 `json:"rows"`
	Hash   string `json:"digest"`
}

type accumulator struct {
	rows uint64
	xor  [sha256.Size]byte
	sum  [sha256.Size]byte
}

// Builder incrementally computes all bucket digests using fixed memory.
type Builder struct{ states []accumulator }

func NewBuilder(buckets uint32) (*Builder, error) {
	if buckets == 0 {
		return nil, errors.New("reconcile bucket count must be positive")
	}
	return &Builder{states: make([]accumulator, buckets)}, nil
}

func (b *Builder) Add(row event.Row) error {
	bucket, err := Bucket(row, uint32(len(b.states)))
	if err != nil {
		return err
	}
	b.states[bucket].add(rowHash(row))
	return nil
}

func (b *Builder) Digests() []Digest {
	result := make([]Digest, len(b.states))
	for i := range b.states {
		result[i] = Digest{Bucket: uint32(i), Rows: b.states[i].rows, Hash: b.states[i].digest()}
	}
	return result
}

func Bucket(row event.Row, buckets uint32) (uint32, error) {
	if buckets == 0 {
		return 0, errors.New("reconcile bucket count must be positive")
	}
	identity := row.Key
	if row.Identity == "full_row" {
		identity = row.Columns
	}
	if len(identity) == 0 {
		return 0, errors.New("reconcile row has no identity")
	}
	digest := columnsHash(identity)
	return uint32(binary.BigEndian.Uint64(digest[:8]) % uint64(buckets)), nil
}

func Digests(rows []event.Row, buckets uint32) ([]Digest, error) {
	builder, err := NewBuilder(buckets)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err := builder.Add(row); err != nil {
			return nil, err
		}
	}
	return builder.Digests(), nil
}

func (a *accumulator) add(value [sha256.Size]byte) {
	a.rows++
	carry := uint16(0)
	for i := sha256.Size - 1; i >= 0; i-- {
		a.xor[i] ^= value[i]
		total := uint16(a.sum[i]) + uint16(value[i]) + carry
		a.sum[i], carry = byte(total), total>>8
	}
}

func (a accumulator) digest() string {
	h := sha256.New()
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], a.rows)
	_, _ = h.Write(count[:])
	_, _ = h.Write(a.xor[:])
	_, _ = h.Write(a.sum[:])
	return hex.EncodeToString(h.Sum(nil))
}

func rowHash(row event.Row) [sha256.Size]byte {
	h := sha256.New()
	writePart(h, []byte(row.Schema))
	writePart(h, []byte(row.Table))
	columns := append([]event.Column(nil), row.Columns...)
	sort.Slice(columns, func(i, j int) bool { return columns[i].Name < columns[j].Name })
	for _, column := range columns {
		writePart(h, []byte(column.Name))
		if column.Value == nil {
			writePart(h, nil)
		} else {
			writePart(h, []byte{1})
			writePart(h, []byte(normalize(column.Type, *column.Value)))
		}
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func columnsHash(columns []event.Column) [sha256.Size]byte {
	h := sha256.New()
	copyColumns := append([]event.Column(nil), columns...)
	sort.Slice(copyColumns, func(i, j int) bool { return copyColumns[i].Name < copyColumns[j].Name })
	for _, column := range copyColumns {
		writePart(h, []byte(column.Name))
		if column.Value == nil {
			writePart(h, nil)
		} else {
			writePart(h, []byte{1})
			writePart(h, []byte(normalize(column.Type, *column.Value)))
		}
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func normalize(typeName, value string) string {
	t := strings.ToLower(strings.TrimSpace(typeName))
	if strings.Contains(t, "binary") || strings.Contains(t, "blob") || t == "bytea" || t == "image" || t == "rowversion" {
		hexValue := value
		if strings.HasPrefix(hexValue, `\x`) || strings.HasPrefix(strings.ToLower(hexValue), "0x") {
			hexValue = hexValue[2:]
		}
		if decoded, err := hex.DecodeString(hexValue); err == nil {
			return "hex:" + hex.EncodeToString(decoded)
		}
	}
	if t == "boolean" || t == "bool" || t == "bit" {
		switch strings.ToLower(value) {
		case "1", "t", "true":
			return "true"
		case "0", "f", "false":
			return "false"
		}
	}
	if strings.HasPrefix(t, "decimal") || strings.HasPrefix(t, "numeric") {
		if number, ok := new(big.Rat).SetString(value); ok {
			return number.RatString()
		}
	}
	if t == "json" || t == "jsonb" {
		var document any
		if json.Unmarshal([]byte(value), &document) == nil {
			if canonical, err := json.Marshal(document); err == nil {
				return string(canonical)
			}
		}
	}
	if strings.Contains(t, "datetime") || strings.HasPrefix(t, "timestamp") {
		return strings.Replace(value, "T", " ", 1)
	}
	return value
}

func writePart(h hash.Hash, value []byte) {
	var length [8]byte
	if value == nil {
		binary.BigEndian.PutUint64(length[:], ^uint64(0))
	} else {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	}
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}
