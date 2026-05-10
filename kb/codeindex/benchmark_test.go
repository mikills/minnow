package codeindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkChunkText(b *testing.B) {
	for _, lines := range []int{200, 2000, 8000} {
		text := benchGoSource(lines)
		b.Run(fmt.Sprintf("go_lines=%d", lines), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = ChunkText(text, "go", 1200, 120)
			}
		})
	}
}

func BenchmarkBuildDocuments(b *testing.B) {
	root := b.TempDir()
	path := filepath.Join(root, "bench.go")
	text := benchGoSource(2000)
	require.NoError(b, os.WriteFile(path, []byte(text), 0o644))
	hashBytes := sha256.Sum256([]byte(text))
	file := ScannedFile{
		AbsPath:   path,
		RelPath:   "bench.go",
		Hash:      hex.EncodeToString(hashBytes[:]),
		SizeBytes: int64(len(text)),
		Language:  "go",
	}
	opts := Options{ChunkSize: 1200, ChunkOverlap: 120}
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := BuildDocuments(ctx, root, "repo", file, opts)
		require.NoError(b, err)
	}
}

func benchGoSource(lines int) string {
	var b strings.Builder
	for i := 0; i < lines/8; i++ {
		fmt.Fprintf(&b, "func Function%d(input string) string {\n", i)
		fmt.Fprintf(&b, "\tvalue := input + %q\n", fmt.Sprintf("-%d", i))
		b.WriteString("\tif value == \"\" {\n")
		b.WriteString("\t\treturn input\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn value\n")
		b.WriteString("}\n\n")
	}
	return b.String()
}
