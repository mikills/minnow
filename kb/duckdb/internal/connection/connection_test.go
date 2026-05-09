package connection

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeMemoryLimit(t *testing.T) {
	got, err := NormalizeMemoryLimit("")
	require.NoError(t, err)
	require.Equal(t, "128MB", got)

	got, err = NormalizeMemoryLimit(" 256 mb ")
	require.NoError(t, err)
	require.Equal(t, "256 mb", got)

	_, err = NormalizeMemoryLimit("abc")
	require.Error(t, err)
}

func TestNormalizeExtensionDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	_, err := NormalizeExtensionDir(missing, true)
	require.Error(t, err)

	got, err := NormalizeExtensionDir(missing, false)
	require.NoError(t, err)
	require.Equal(t, filepath.Clean(missing), got)

	got, err = NormalizeExtensionDir("", true)
	require.NoError(t, err)
	require.Empty(t, got)
}
