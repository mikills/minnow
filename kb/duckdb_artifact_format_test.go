package kb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type errQueryer struct{}

func (errQueryer) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("boom")
}

func TestDuckDBArtifactFormat(t *testing.T) {
	t.Run("memory_limit_parse", testDuckDBMemoryLimitParse)
	t.Run("extension_dir_parse", testDuckDBExtensionDirParse)
	t.Run("table_exists_error", testDuckDBTableExistsError)
	t.Run("table_exists_missing", testDuckDBTableExistsMissing)
}

func testDuckDBMemoryLimitParse(t *testing.T) {
	val, err := normalizeDuckDBMemoryLimit("")
	require.NoError(t, err)
	assert.Equal(t, "128MB", val)

	val, err = normalizeDuckDBMemoryLimit(" 256 mb ")
	require.NoError(t, err)
	assert.Equal(t, "256 mb", val)

	_, err = normalizeDuckDBMemoryLimit("abc")
	require.Error(t, err)
}

func testDuckDBTableExistsError(t *testing.T) {
	ok, err := tableExists(context.Background(), errQueryer{}, "docs")
	require.Error(t, err)
	assert.False(t, ok)
}

func testDuckDBTableExistsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table-exists.duckdb")
	db, err := sql.Open("duckdb", path)
	require.NoError(t, err)
	defer db.Close()

	ok, err := tableExists(context.Background(), db, "missing_table")
	require.NoError(t, err)
	assert.False(t, ok)
}

func testDuckDBExtensionDirParse(t *testing.T) {
	t.Run("offline_missing", func(t *testing.T) {
		_, err := normalizeDuckDBExtensionDir(filepath.Join(t.TempDir(), "missing"), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "offline mode")
	})

	t.Run("online_missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing")
		val, err := normalizeDuckDBExtensionDir(path, false)
		require.NoError(t, err)
		assert.Equal(t, filepath.Clean(path), val)
	})

	t.Run("not_directory", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "ext-file")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))
		_, err := normalizeDuckDBExtensionDir(file, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a directory")
	})
}
