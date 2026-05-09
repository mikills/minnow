package graph

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestEnsureTables(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, EnsureTables(context.Background(), db))
	ok, err := tableExists(context.Background(), db, "entities")
	require.NoError(t, err)
	require.True(t, ok)
}
