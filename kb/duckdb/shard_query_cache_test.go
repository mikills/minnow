package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mikills/minnow/kb/search"
	"github.com/stretchr/testify/require"
)

func shardQueryFixture(t *testing.T, dim int) (*DuckDBArtifactFormat, *shardConn, string) {
	t.Helper()
	f := newHandleReuseTestFormat(t)
	path := filepath.Join(t.TempDir(), "shard.duckdb")
	db, err := f.openConfiguredDB(t.Context(), path)
	require.NoError(t, err)
	sc := &shardConn{db: db}
	t.Cleanup(func() { require.NoError(t, sc.close()) })
	_, err = db.ExecContext(t.Context(), fmt.Sprintf(`CREATE TABLE docs
		(id TEXT, content TEXT, embedding FLOAT[%d], media_refs TEXT, metadata TEXT)`, dim))
	require.NoError(t, err)
	for i := range 32 {
		vector := make([]float32, dim)
		vector[i%dim] = 1 + float32(i)/1000
		_, err := db.ExecContext(t.Context(), "INSERT INTO docs VALUES (?, ?, ?, NULL, ?)",
			fmt.Sprintf("doc-%d", i), fmt.Sprintf("search document %d", i), vector,
			fmt.Sprintf(`{"parity":%d}`, i%2))
		require.NoError(t, err)
	}
	_, err = db.ExecContext(t.Context(), "CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding)")
	require.NoError(t, err)
	return f, sc, path
}

func TestShardQueryCache(t *testing.T) {
	for _, dim := range []int{8, 32, 512} {
		t.Run(fmt.Sprintf("bindings_%d", dim), func(t *testing.T) {
			_, sc, _ := shardQueryFixture(t, dim)
			var first *sql.Stmt
			for _, axis := range []int{0, 1, 2, 0} {
				vector := make([]float32, dim)
				vector[axis] = 1
				vector[(axis+1)%dim] = 0.00000031
				cached, err := queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
				require.NoError(t, err)
				literal, err := queryTopKWithDB(t.Context(), sc.db, vector, 1, vectorQueryOpts{})
				require.NoError(t, err)
				require.Equal(t, literal, cached)
				require.Len(t, cached, 1)
				require.Equal(t, fmt.Sprintf("doc-%d", axis), cached[0].ID)
				require.Len(t, sc.vectorStatements, 1)
				if first == nil {
					first = sc.vectorStatements[0].stmt
				}
				require.Same(t, first, sc.vectorStatements[0].stmt)
				refs, err := queryTopKRefsWithDB(t.Context(), sc, vector, 2, vectorQueryOpts{})
				require.NoError(t, err)
				require.Len(t, refs, 2)
				results, err := hydrateRankedResults(t.Context(), sc, refs)
				require.NoError(t, err)
				require.Equal(t, cached[0], results[0])
				// Ref-only and payload projections are different prepared SQL.
				require.Len(t, sc.vectorStatements, 2)
				// Remove the ref entry so the next full-result query still checks
				// the exact same full-result statement identity.
				require.NoError(t, sc.vectorStatements[1].stmt.Close())
				sc.vectorStatements = sc.vectorStatements[:1]
			}
		})
	}

	t.Run("filters_and_scopes_stay_literal", func(t *testing.T) {
		_, sc, _ := shardQueryFixture(t, 32)
		vector := make([]float32, 32)
		vector[0] = 1
		for _, opts := range []vectorQueryOpts{
			{filter: &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: float64(1)}},
			{documentIDs: []string{"doc-7"}},
			{documentIDs: []string{}},
			{filter: &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: float64(0)}, documentIDs: []string{"doc-7"}},
		} {
			cached, err := queryTopKWithDB(t.Context(), sc, vector, 4, opts)
			require.NoError(t, err)
			literal, err := queryTopKWithDB(t.Context(), sc.db, vector, 4, opts)
			require.NoError(t, err)
			require.Equal(t, literal, cached)
			require.Empty(t, sc.vectorStatements)
		}
	})

	t.Run("lru_and_close", func(t *testing.T) {
		_, sc, _ := shardQueryFixture(t, 32)
		vector := make([]float32, 32)
		vector[0] = 1
		for k := 1; k <= maxShardVectorStatements; k++ {
			_, err := queryTopKWithDB(t.Context(), sc, vector, k, vectorQueryOpts{})
			require.NoError(t, err)
		}
		first, second := sc.vectorStatements[0].stmt, sc.vectorStatements[1].stmt
		_, err := queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
		_, err = queryTopKWithDB(t.Context(), sc, vector, maxShardVectorStatements+1, vectorQueryOpts{})
		require.NoError(t, err)
		require.Len(t, sc.vectorStatements, maxShardVectorStatements)
		_, err = second.QueryContext(t.Context(), vector)
		require.ErrorContains(t, err, "statement is closed")
		_, err = queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
		require.Same(t, first, sc.vectorStatements[len(sc.vectorStatements)-1].stmt)
		require.NoError(t, sc.close())
		require.Empty(t, sc.vectorStatements)
		require.Empty(t, sc.metadataExpr)
		_, err = first.QueryContext(t.Context(), vector)
		require.ErrorContains(t, err, "statement is closed")
	})

	t.Run("cancellation_and_failure_recovery", func(t *testing.T) {
		_, sc, _ := shardQueryFixture(t, 32)
		vector := make([]float32, 32)
		vector[0] = 1
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := queryTopKWithDB(ctx, sc, vector, 1, vectorQueryOpts{})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, sc.metadataExpr)
		require.Empty(t, sc.vectorStatements)
		_, err = queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
		first := sc.vectorStatements[0].stmt
		_, err = queryTopKWithDB(ctx, sc, vector, 1, vectorQueryOpts{})
		require.ErrorIs(t, err, context.Canceled)
		_, err = queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
		require.Same(t, first, sc.vectorStatements[0].stmt)
		_, err = sc.queryPreparedVector(t.Context(), "SELECT error('forced execution failure') WHERE $1::FLOAT[32] IS NOT NULL", vector)
		require.ErrorContains(t, err, "forced execution failure")
		require.Len(t, sc.vectorStatements, 1)
		_, err = queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
	})

	t.Run("eviction_resets_statements_and_schema", func(t *testing.T) {
		f, fixture, path := shardQueryFixture(t, 32)
		var pool shardConnPool
		t.Cleanup(pool.CloseAll)
		sc, err := pool.GetOrOpen(t.Context(), path, 1, func(context.Context, string) (*sql.DB, error) { return fixture.db, nil })
		require.NoError(t, err)
		vector := make([]float32, 32)
		vector[0] = 1
		_, err = queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
		stmt := sc.vectorStatements[0].stmt
		require.Equal(t, "metadata", sc.metadataExpr)
		sc.mu.Unlock()
		pool.BeginEviction(filepath.Dir(path))()
		require.Empty(t, sc.vectorStatements)
		require.Empty(t, sc.metadataExpr)
		_, err = stmt.QueryContext(t.Context(), vector)
		require.ErrorContains(t, err, "statement is closed")
		require.Zero(t, f.budget().LiveDatabases())
		// A replacement/legacy shard at the same path must not inherit schema
		// facts or prepared SQL from the former handle.
		db, err := f.openConfiguredDB(t.Context(), path)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		_, err = db.ExecContext(t.Context(), "DROP INDEX docs_vec_idx; ALTER TABLE docs DROP metadata; CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding)")
		require.NoError(t, err)
		require.NoError(t, db.Close())
		fresh, err := pool.GetOrOpen(t.Context(), path, 1, f.openConfiguredDB)
		require.NoError(t, err)
		defer fresh.mu.Unlock()
		require.NotSame(t, sc, fresh)
		results, err := queryTopKWithDB(t.Context(), fresh, vector, 1, vectorQueryOpts{})
		require.NoError(t, err)
		require.Nil(t, results[0].Metadata)
		require.Equal(t, "NULL AS metadata", fresh.metadataExpr)
	})

	t.Run("schema_cache_does_not_affect_mutable_helpers", func(t *testing.T) {
		_, sc, _ := shardQueryFixture(t, 32)
		expr, err := docsMetadataSelectExpr(t.Context(), sc)
		require.NoError(t, err)
		require.Equal(t, "metadata", expr)
		_, err = sc.db.ExecContext(t.Context(), "DROP INDEX docs_vec_idx; ALTER TABLE docs DROP metadata")
		require.NoError(t, err)
		expr, err = docsMetadataSelectExpr(t.Context(), sc.db)
		require.NoError(t, err)
		require.Equal(t, "NULL AS metadata", expr)
		// Deliberately violate immutability only to prove schema is inspected
		// once per pooled handle, not cached globally for mutable databases.
		expr, err = docsMetadataSelectExpr(t.Context(), sc)
		require.NoError(t, err)
		require.Equal(t, "metadata", expr)
	})
}

func TestBoundVector(t *testing.T) {
	t.Run("literal_precision", func(t *testing.T) {
		vector := []float32{0, float32(math.Copysign(0, -1)), 0.0078125, -0.0078125,
			1.2345678, 0.00000031, math.SmallestNonzeroFloat32, math.MaxFloat32}
		before := FormatVectorForSQL(vector)
		bound, err := boundQueryVector(vector)
		require.NoError(t, err)
		require.Equal(t, before, FormatVectorForSQL(bound))
		require.Equal(t, before, FormatVectorForSQL(vector), "must not mutate caller's vector")
	})
	t.Run("non_finite", func(t *testing.T) {
		for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
			_, err := boundQueryVector([]float32{value})
			require.ErrorContains(t, err, "non-finite")
		}
	})
}

func TestPreparedVectorPlan(t *testing.T) {
	for _, dim := range []int{32, 512} {
		t.Run(fmt.Sprint(dim), func(t *testing.T) {
			_, sc, _ := shardQueryFixture(t, dim)
			for _, refsOnly := range []bool{false, true} {
				for _, axis := range []int{0, 1} {
					vector := make([]float32, dim)
					vector[axis] = 1
					if refsOnly {
						_, err := queryTopKRefsWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
						require.NoError(t, err)
					} else {
						_, err := queryTopKWithDB(t.Context(), sc, vector, 1, vectorQueryOpts{})
						require.NoError(t, err)
					}
					query := sc.vectorStatements[len(sc.vectorStatements)-1].query
					rows, err := sc.db.QueryContext(t.Context(), "EXPLAIN "+query, vector)
					require.NoError(t, err)
					var plan strings.Builder
					for rows.Next() {
						var key, value string
						require.NoError(t, rows.Scan(&key, &value))
						plan.WriteString(value)
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
					require.Contains(t, strings.ReplaceAll(strings.ToUpper(plan.String()), " ", "_"), "HNSW_INDEX_SCAN")
				}
			}
		})
	}
}
