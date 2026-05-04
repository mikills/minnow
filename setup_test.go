package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetup(t *testing.T) {
	t.Run("path contains dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "go", "bin")
		pathValue := filepath.Join(t.TempDir(), "bin") + string(os.PathListSeparator) + dir

		require.True(t, pathContainsDir(pathValue, dir))
		require.False(t, pathContainsDir(pathValue, filepath.Join(t.TempDir(), "other")))
	})

	t.Run("append is idempotent", func(t *testing.T) {
		profile := filepath.Join(t.TempDir(), ".zshrc")
		line := `export PATH="$PATH:/Users/example/go/bin"`

		require.NoError(t, appendPathExport(profile, line))
		require.NoError(t, appendPathExport(profile, line))

		data, err := os.ReadFile(profile)
		require.NoError(t, err)
		require.Equal(t, 1, strings.Count(string(data), line))
		require.Contains(t, string(data), "# Added by minnow setup")
	})
}
