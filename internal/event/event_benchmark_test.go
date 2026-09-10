package event

import (
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkMessageJSONRoundTrip(b *testing.B) {
	for _, rowCount := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
			message := benchmarkMessage(rowCount)
			encoded, err := json.Marshal(message)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			for b.Loop() {
				payload, err := json.Marshal(message)
				if err != nil {
					b.Fatal(err)
				}
				var decoded Message
				if err := json.Unmarshal(payload, &decoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkMessage(rowCount int) Message {
	rows := make([]Row, rowCount)
	for i := range rows {
		id, name, amount := fmt.Sprint(i), "customer", "123456789.1234"
		rows[i] = Row{
			Schema: "public", Table: "orders", Operation: "insert",
			Key:     []Column{{Name: "id", Type: "bigint", Value: &id}},
			Columns: []Column{{Name: "id", Type: "bigint", Value: &id}, {Name: "name", Type: "text", Value: &name}, {Name: "amount", Type: "numeric", Value: &amount}},
		}
	}
	return Message{Version: "v2", SourceID: "benchmark", Generation: "generation", Seq: 1, ID: "benchmark/generation/1", Kind: "transaction_rows", Transaction: "tx", Rows: rows}
}
