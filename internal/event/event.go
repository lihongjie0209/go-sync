// Package event defines the versioned, lossless HTTP wire format.
package event

import "encoding/json"

// Column distinguishes an absent value (not in Columns) from SQL NULL.
// Non-null values use the source's lossless text representation (see protocol).
type Column struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Value       *string `json:"value"`
	Encoding    string  `json:"encoding,omitempty"`
	SourceValue *string `json:"source_value,omitempty"`
}

type Row struct {
	// Identity is full_row for keyless tables; omitted means primary-key matching.
	Identity  string   `json:"identity,omitempty"`
	Schema    string   `json:"schema"`
	Table     string   `json:"table"`
	Operation string   `json:"operation"`
	Key       []Column `json:"key"`
	OldKey    []Column `json:"old_key,omitempty"`
	Columns   []Column `json:"columns,omitempty"`
	Ordinal   uint64   `json:"ordinal,omitempty"`
}

type TableRef struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// FileChange is chunked so large files stay below the configured transport
// limit. Path always uses slash separators and is relative to the watched root.
type FileChange struct {
	Path            string `json:"path"`
	Operation       string `json:"operation"`
	Size            int64  `json:"size,omitempty"`
	ModTimeUnixNano int64  `json:"mod_time_unix_nano,omitempty"`
	Mode            uint32 `json:"mode,omitempty"`
	SHA256          string `json:"sha256,omitempty"`
	Chunk           uint32 `json:"chunk,omitempty"`
	Chunks          uint32 `json:"chunks,omitempty"`
	Data            string `json:"data,omitempty"`
}

type Message struct {
	Version       string          `json:"version"`
	SourceID      string          `json:"source_id"`
	Generation    string          `json:"generation"`
	SchemaVersion string          `json:"schema_version,omitempty"`
	Seq           uint64          `json:"seq"`
	ID            string          `json:"message_id"`
	Kind          string          `json:"kind"`
	Transaction   string          `json:"transaction,omitempty"`
	Chunk         uint64          `json:"chunk,omitempty"`
	LSN           string          `json:"lsn,omitempty"`
	Tables        []TableRef      `json:"tables,omitempty"`
	Schema        json.RawMessage `json:"schema,omitempty"`
	Rows          []Row           `json:"rows,omitempty"`
	File          *FileChange     `json:"file,omitempty"`
	CreatedAt     string          `json:"created_at"`
}

type Ack struct {
	ID  string `json:"message_id"`
	Seq uint64 `json:"ack_seq"`
}

// Text preserves numbers without passing through float64.
func Text(raw json.RawMessage) (*string, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var s string
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
	} else {
		if !json.Valid(raw) {
			return nil, &json.SyntaxError{}
		}
		s = string(raw)
	}
	return &s, nil
}
