package fileingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.txt")
	require.NoError(t, os.WriteFile(path, []byte(" hello "), 0o644))
	pages, err := SourceText(context.Background(), path, "text/plain")
	require.NoError(t, err)
	require.Equal(t, []Page{{Number: 1, Text: "hello"}}, pages)
}

func TestSourceTextEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.txt")
	require.NoError(t, os.WriteFile(path, []byte(" "), 0o644))
	_, err := SourceText(context.Background(), path, "text/plain")
	require.Error(t, err)
}
