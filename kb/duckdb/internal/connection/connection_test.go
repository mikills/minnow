package connection

import (
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestRuntime(t *testing.T) {
	db, err := Open(t.Context(), "", Config{
		ExtensionDir: ResolveExtensionDir(),
		OfflineExt:   true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	t.Run("native", func(t *testing.T) {
		var version, sourceID, codename string
		require.NoError(t, db.QueryRowContext(t.Context(), `PRAGMA version`).Scan(&version, &sourceID, &codename))
		require.Equal(t, NativeVersion, version)
		require.Equal(t, "d54cbbe3df", sourceID)
		t.Logf("DuckDB %s (%s)", version, sourceID)
	})

	t.Run("extension_policy", func(t *testing.T) {
		for _, setting := range []string{"allow_unsigned_extensions", "autoinstall_known_extensions", "autoload_known_extensions"} {
			var enabled bool
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT current_setting(?)`, setting).Scan(&enabled))
			require.False(t, enabled, "%s must remain disabled", setting)
		}
	})
}

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

func TestTempDir(t *testing.T) {
	t.Run("empty keeps the default", func(t *testing.T) {
		got, err := NormalizeTempDir("  ")
		require.NoError(t, err)
		require.Equal(t, "", got)
	})

	t.Run("creates a missing directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "spill")
		got, err := NormalizeTempDir(dir)
		require.NoError(t, err)
		require.Equal(t, dir, got)
		require.DirExists(t, dir)
	})

	t.Run("rejects a file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
		_, err := NormalizeTempDir(file)
		require.Error(t, err)
	})

	t.Run("rejects a NUL", func(t *testing.T) {
		_, err := NormalizeTempDir("bad\x00dir")
		require.Error(t, err)
	})

	t.Run("reaches duckdb", func(t *testing.T) {
		dir := t.TempDir()
		spill := filepath.Join(dir, "spill")
		db, err := Open(t.Context(), filepath.Join(dir, "t.duckdb"), Config{
			MemoryLimit:  "128MB",
			TempDir:      spill,
			ExtensionDir: ResolveExtensionDir(),
			OfflineExt:   true,
		})
		require.NoError(t, err)
		defer db.Close()

		var got string
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT current_setting('temp_directory')`).Scan(&got))
		require.Equal(t, spill, got)
	})
}
