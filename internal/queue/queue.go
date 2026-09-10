// Package queue stores staged and deliverable messages in one synchronous bbolt database.
package queue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-sync/internal/event"
	bolt "go.etcd.io/bbolt"
)

var ErrFull = errors.New("queue capacity reached")
var ErrEmpty = errors.New("queue empty")

var metaBucket = []byte("meta")
var itemsBucket = []byte("items")
var stateKey = []byte("state")

type State struct {
	ProtocolVersion string `json:"protocol_version,omitempty"`
	SourceID        string `json:"source_id"`
	Generation      string `json:"generation"`
	Fingerprint     string `json:"fingerprint"`
	SystemID        string `json:"system_id"`
	Timeline        int32  `json:"timeline"`
	Database        string `json:"database"`
	SchemaHash      string `json:"schema_hash"`
	Slot            string `json:"slot"`
	Phase           string `json:"phase"`
	SnapshotLSN     string `json:"snapshot_lsn"`
	DurableLSN      string `json:"durable_lsn"`
	SourceCleanup   string `json:"source_cleanup,omitempty"`
	NextSeq         uint64 `json:"next_seq"`
	ReadySeq        uint64 `json:"ready_seq"`
	DeliveredSeq    uint64 `json:"delivered_seq"`
	Bytes           int64  `json:"bytes"`
	EarliestSeq     uint64 `json:"earliest_seq,omitempty"`
	ArchiveBytes    int64  `json:"archive_bytes,omitempty"`
}

type Store struct {
	db          *bolt.DB
	maxBytes    int64
	reserve     uint64
	dir         string
	replayAge   time.Duration
	replayBytes int64
}

func Open(dir string, maxBytes int64, reserve uint64) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "queue.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open queue (another collector may hold the lock): %w", err)
	}
	s := &Store{db: db, maxBytes: maxBytes, reserve: reserve, dir: dir, replayAge: 7 * 24 * time.Hour, replayBytes: 20 << 30}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{metaBucket, itemsBucket} {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return e
			}
		}
		if tx.Bucket(metaBucket).Get(stateKey) == nil {
			return save(tx, State{})
		}
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := syncDirectory(dir); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := syncDirectory(filepath.Dir(filepath.Clean(dir))); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }

// ConfigureReplay sets retention for acknowledged messages. It does not affect
// unacknowledged queue capacity and never evicts an undelivered message.
func (s *Store) ConfigureReplay(maxAge time.Duration, maxBytes int64) {
	s.replayAge, s.replayBytes = maxAge, maxBytes
}
func key(n uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, n); return b }
func read(tx *bolt.Tx) (State, error) {
	var st State
	err := json.Unmarshal(tx.Bucket(metaBucket).Get(stateKey), &st)
	return st, err
}
func save(tx *bolt.Tx, st State) error {
	b, e := json.Marshal(st)
	if e != nil {
		return e
	}
	return tx.Bucket(metaBucket).Put(stateKey, b)
}
func (s *Store) State() (State, error) {
	var st State
	err := s.db.View(func(tx *bolt.Tx) error { var e error; st, e = read(tx); return e })
	return st, err
}

// MetricsSnapshot reads queue counters and the oldest deliverable timestamp in
// one read transaction, without copying or decoding row payloads.
func (s *Store) MetricsSnapshot() (State, time.Time, error) {
	var st State
	var oldest time.Time
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		st, err = read(tx)
		if err != nil || st.DeliveredSeq >= st.ReadySeq {
			return err
		}
		v := tx.Bucket(itemsBucket).Get(key(st.DeliveredSeq + 1))
		if v == nil {
			return errors.New("queue gap")
		}
		var header struct {
			CreatedAt time.Time `json:"created_at"`
		}
		if err := json.Unmarshal(v, &header); err != nil {
			return err
		}
		oldest = header.CreatedAt
		return nil
	})
	return st, oldest, err
}

func (s *Store) Initialize(st State) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		old, e := read(tx)
		if e != nil {
			return e
		}
		if old.Phase != "" && old.Phase != "snapshot" {
			return errors.New("cannot reset a sealed capture")
		}
		if e := tx.DeleteBucket(itemsBucket); e != nil {
			return e
		}
		if _, e := tx.CreateBucket(itemsBucket); e != nil {
			return e
		}
		st.Phase = "snapshot"
		st.Bytes = 0
		st.NextSeq = 0
		st.ReadySeq = 0
		st.DeliveredSeq = 0
		st.EarliestSeq = 1
		st.ArchiveBytes = 0
		return save(tx, st)
	})
}
func (s *Store) SnapshotPoint(lsn string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		st.SnapshotLSN = lsn
		return save(tx, st)
	})
}

// FinalizeSnapshotLSN assigns the boundary to every invisible snapshot message
// after source locks have been released. Publication remains a separate atomic
// operation, so a crash here can only leave discardable staged data.
func (s *Store) FinalizeSnapshotLSN(lsn string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, err := read(tx)
		if err != nil {
			return err
		}
		if st.Phase != "snapshot" || st.ReadySeq != 0 {
			return errors.New("snapshot boundary cannot be finalized")
		}
		bucket := tx.Bucket(itemsBucket)
		var bytes int64
		for seq := uint64(1); seq <= st.NextSeq; seq++ {
			value := bucket.Get(key(seq))
			if value == nil {
				return errors.New("queue gap")
			}
			var message event.Message
			if err := json.Unmarshal(value, &message); err != nil {
				return err
			}
			message.LSN = lsn
			updated, err := json.Marshal(message)
			if err != nil {
				return err
			}
			if bytes+int64(len(updated)) > s.maxBytes {
				return ErrFull
			}
			if err := bucket.Put(key(seq), updated); err != nil {
				return err
			}
			bytes += int64(len(updated))
		}
		st.Bytes = bytes
		st.SnapshotLSN = lsn
		return save(tx, st)
	})
}

// Append writes an invisible chunk. Publish is the only operation that exposes it.
func (s *Store) Append(m event.Message) error {
	free, err := FreeBytes(s.dir)
	if err != nil {
		return err
	}
	if free < s.reserve+uint64(len(m.Rows))*1024 {
		return ErrFull
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		m.Version = st.ProtocolVersion
		if m.Version == "" {
			m.Version = "v1"
		}
		m.SourceID = st.SourceID
		m.Generation = st.Generation
		if m.SchemaVersion == "" {
			m.SchemaVersion = st.SchemaHash
		}
		m.Seq = st.NextSeq + 1
		m.ID = fmt.Sprintf("%s/%s/%d", st.SourceID, st.Generation, m.Seq)
		m.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		b, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if st.Bytes+int64(len(b)) > s.maxBytes || free < s.reserve+uint64(len(b))*4 {
			return ErrFull
		}
		if e := tx.Bucket(itemsBucket).Put(key(m.Seq), b); e != nil {
			return e
		}
		st.NextSeq = m.Seq
		st.Bytes += int64(len(b))
		return save(tx, st)
	})
}

// Publish atomically exposes every staged chunk and advances the replay checkpoint.
func (s *Store) Publish(lsn string, sealSnapshot bool) error {
	return s.PublishSource(lsn, sealSnapshot, "")
}

// PublishSchema exposes a complete schema refresh and advances its binlog
// checkpoint and schema hash in the same local transaction.
func (s *Store) PublishSchema(lsn, schemaHash string) error {
	if lsn == "" || schemaHash == "" {
		return errors.New("schema checkpoint and hash are required")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		st, err := read(tx)
		if err != nil {
			return err
		}
		if st.Phase != "stream" {
			return errors.New("schema can only be published while streaming")
		}
		st.ReadySeq, st.DurableLSN, st.SchemaHash = st.NextSeq, lsn, schemaHash
		return save(tx, st)
	})
}

// PublishSource also records source-side cleanup that must happen only after
// this local publication is durable. It is used by trigger-backed outboxes.
func (s *Store) PublishSource(lsn string, sealSnapshot bool, cleanup string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		if sealSnapshot {
			if st.Phase != "snapshot" {
				return errors.New("snapshot already sealed")
			}
			st.Phase = "stream"
		}
		st.ReadySeq = st.NextSeq
		st.DurableLSN = lsn
		st.SourceCleanup = cleanup
		return save(tx, st)
	})
}

// ClearSourceCleanup acknowledges an idempotent source cleanup without
// changing message visibility or the capture checkpoint.
func (s *Store) ClearSourceCleanup(expected string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, err := read(tx)
		if err != nil {
			return err
		}
		if st.SourceCleanup != expected {
			return errors.New("source cleanup token changed")
		}
		st.SourceCleanup = ""
		return save(tx, st)
	})
}

// Recover discards only chunks which have never been published or acknowledged to PG.
func (s *Store) Recover() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		cur := tx.Bucket(itemsBucket).Cursor()
		for k, v := cur.Seek(key(st.ReadySeq + 1)); k != nil; k, v = cur.Next() {
			st.Bytes -= int64(len(v))
			if e := cur.Delete(); e != nil {
				return e
			}
		}
		st.NextSeq = st.ReadySeq
		return save(tx, st)
	})
}
func (s *Store) Peek() (event.Message, []byte, error) {
	var m event.Message
	var b []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		if st.DeliveredSeq >= st.ReadySeq {
			return ErrEmpty
		}
		v := tx.Bucket(itemsBucket).Get(key(st.DeliveredSeq + 1))
		if v == nil {
			return errors.New("queue gap")
		}
		b = append([]byte(nil), v...)
		return json.Unmarshal(b, &m)
	})
	return m, b, err
}

// Get returns a published message by sequence, including acknowledged history.
func (s *Store) Get(seq uint64) (event.Message, []byte, error) {
	var m event.Message
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		st, err := read(tx)
		if err != nil {
			return err
		}
		earliest := st.EarliestSeq
		if earliest == 0 {
			earliest = st.DeliveredSeq + 1
		}
		if seq < earliest || seq > st.ReadySeq {
			return ErrEmpty
		}
		value := tx.Bucket(itemsBucket).Get(key(seq))
		if value == nil {
			return errors.New("queue gap")
		}
		raw = append([]byte(nil), value...)
		return json.Unmarshal(raw, &m)
	})
	return m, raw, err
}

func (s *Store) Bounds() (generation string, earliest, ready uint64, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		generation, earliest, ready = st.Generation, st.EarliestSeq, st.ReadySeq
		if earliest == 0 {
			earliest = st.DeliveredSeq + 1
		}
		return nil
	})
	return
}
func (s *Store) Ack(seq uint64, id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := read(tx)
		if e != nil {
			return e
		}
		if seq != st.DeliveredSeq+1 || seq > st.ReadySeq {
			return errors.New("non-contiguous ack")
		}
		b := tx.Bucket(itemsBucket)
		v := b.Get(key(seq))
		if v == nil {
			return errors.New("ack references missing message")
		}
		var m event.Message
		if e := json.Unmarshal(v, &m); e != nil {
			return e
		}
		if m.ID != id {
			return errors.New("ack id mismatch")
		}
		st.Bytes -= int64(len(v))
		st.ArchiveBytes += int64(len(v))
		st.DeliveredSeq = seq
		if st.EarliestSeq == 0 {
			st.EarliestSeq = 1
		}
		for st.EarliestSeq <= st.DeliveredSeq {
			old := b.Get(key(st.EarliestSeq))
			if old == nil {
				st.EarliestSeq++
				continue
			}
			expired := false
			var header struct {
				CreatedAt string `json:"created_at"`
			}
			if json.Unmarshal(old, &header) == nil {
				if created, err := time.Parse(time.RFC3339Nano, header.CreatedAt); err == nil {
					expired = time.Since(created) > s.replayAge
				}
			}
			if !expired && st.ArchiveBytes <= s.replayBytes {
				break
			}
			st.ArchiveBytes -= int64(len(old))
			if e := b.Delete(key(st.EarliestSeq)); e != nil {
				return e
			}
			st.EarliestSeq++
		}
		return save(tx, st)
	})
}
