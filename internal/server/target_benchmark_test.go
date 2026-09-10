package server

import (
	"fmt"
	"testing"
)

func BenchmarkBuildRowSQL(b *testing.B) {
	for _, columnCount := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("columns_%d", columnCount), func(b *testing.B) {
			columns := make([]appliedColumn, columnCount)
			types := make(map[string]string, columnCount)
			for i := range columns {
				name, value := fmt.Sprintf("column_%d", i), "value"
				columns[i] = appliedColumn{name: name, value: &value}
				types[name] = "text"
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := expressions(columns, types, 1); err != nil {
					b.Fatal(err)
				}
				if _, _, err := predicates(columns, types, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
