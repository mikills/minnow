package duckdb

import (
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/connection"
	"github.com/stretchr/testify/require"
)

func TestGraphExpansion(t *testing.T) {
	t.Run("bfs_without_pgq", testBFSWithoutPGQ)
}

func testBFSWithoutPGQ(t *testing.T) {
	ctx := t.Context()
	db, err := connection.Open(ctx, "", connection.Config{
		ExtensionDir: ResolveExtensionDir(),
		OfflineExt:   true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, EnsureGraphTables(ctx, db))
	_, err = db.ExecContext(ctx, `INSERT INTO edges (src, dst, weight, rel_type) VALUES
		('a', 'b', 1, 'related'), ('b', 'c', 1, 'related'), ('a', 'ignored', 1, 'other')`)
	require.NoError(t, err)

	options := kb.NormalizeExpansionOptions(10, nil)
	require.False(t, options.UseDuckPGQ, "default graph expansion must not require PGQ")
	options.Decay = 0.5
	options.EdgeTypes = []string{"related"}
	scores, err := expandEntityScores(ctx, db, map[string]float64{"a": 1}, options)
	require.NoError(t, err)
	// BFS traverses edges in both directions, so hop two also adds b -> a.
	require.Equal(t, map[string]float64{"a": 1.25, "b": 0.5, "c": 0.25}, scores)

	var pgqLoaded bool
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COALESCE(bool_or(loaded), false) FROM duckdb_extensions() WHERE extension_name = 'duckpgq'`).Scan(&pgqLoaded))
	require.False(t, pgqLoaded)
}
