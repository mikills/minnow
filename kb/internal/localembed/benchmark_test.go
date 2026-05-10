package localembed

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkVector(b *testing.B) {
	inputs := map[string]string{
		"short":  "vector database indexing query planning",
		"medium": strings.Repeat("semantic retrieval code indexing graph shard merge ranking ", 20),
		"long":   strings.Repeat("semantic retrieval code indexing graph shard merge ranking ", 200),
	}
	for name, input := range inputs {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := Vector(input, 384, 3, 5)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkTokenize(b *testing.B) {
	input := strings.Repeat("semantic retrieval code indexing graph shard merge ranking ", 200)
	b.ReportAllocs()
	for b.Loop() {
		_ = Tokenize(input)
	}
}
