package reconstruct

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mikills/minnow/kb/duckdb/internal/connection"
	"github.com/stretchr/testify/require"
)

func TestAttachedTableExists(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `CREATE TABLE docs (id TEXT)`)
	require.NoError(t, err)

	got, err := AttachedTableExists(context.Background(), db, "main", "docs")
	require.NoError(t, err)
	require.True(t, got)

	got, err = AttachedTableExists(context.Background(), db, "main", "missing")
	require.NoError(t, err)
	require.False(t, got)
}
