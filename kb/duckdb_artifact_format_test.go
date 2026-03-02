package kb

import (
	"context"
	"database/sql"
	"fmt"
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
