package duckdb

// Frozen pre-fix SQL is retained as the control for paired CI comparisons and
// correctness checks against the production materialized query.
import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
	"github.com/stretchr/testify/require"
)

func bm25Original(ctx context.Context, db docQueryer, text string, k int, filter *search.FilterExpr, ids []string) ([]kb.QueryResult, error) {
	if k <= 0 || strings.TrimSpace(text) == "" {
		return []kb.QueryResult{}, nil
	}
	metadata, err := docsMetadataSelectExpr(ctx, db)
	if err != nil {
		return nil, err
	}
	where, err := buildWhereClause(filter, ids != nil)
	if err != nil {
		return nil, err
	}
	escaped := quoteSQLStringLiteral(text)
	condition := "fts_main_docs.match_bm25(id, " + escaped + ", fields := 'content') IS NOT NULL"
	if where != "" {
		where += " AND " + condition
	} else {
		where = " WHERE " + condition
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, content, fts_main_docs.match_bm25(id, %s, fields := 'content') AS bm25_score, media_refs, %s
		FROM docs%s
		ORDER BY bm25_score DESC
		LIMIT %d
	`, escaped, metadata, where, k), documentScopeArgs(ids)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanQueryResults(rows, k)
}

type bm25Recorder struct {
	docQueryer
	query string
	args  []any
}

func (r *bm25Recorder) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	r.query, r.args = query, args
	return r.docQueryer.QueryContext(ctx, query, args...)
}

func BenchmarkBM25Shape(b *testing.B) {
	if os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("RUNNER_ENVIRONMENT") != "github-hosted" || runtime.GOOS != "linux" {
		b.Skip("performance measurement is restricted to GitHub-hosted Linux runners")
	}
	for _, workload := range []string{"full", "filtered", "scoped"} {
		for _, shape := range []string{"current", "materialized"} {
			b.Run(workload+"_"+shape, func(b *testing.B) {
				f, shard := warmShardBenchmarkFixture(b, 512)
				ctx := context.Background()
				sc, err := f.openCachedShardConn(ctx, "fixture", shard)
				require.NoError(b, err)
				defer sc.mu.Unlock()
				check := func() {
					for _, setting := range []struct{ name, value string }{
						{"threads", "1"}, {"memory_limit", "122.0 MiB"},
						{"allow_unsigned_extensions", "false"}, {"autoload_known_extensions", "false"},
						{"autoinstall_known_extensions", "false"},
					} {
						var value string
						require.NoError(b, sc.QueryRowContext(ctx, "SELECT current_setting(?)::VARCHAR", setting.name).Scan(&value))
						require.Equal(b, setting.value, value, setting.name)
					}
					var version string
					require.NoError(b, sc.QueryRowContext(ctx, "SELECT version()").Scan(&version))
					require.Equal(b, "v2.0.0", version)
				}
				check()
				defer check()
				var filter *search.FilterExpr
				var ids []string
				if workload == "filtered" {
					filter = &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: float64(1)}
				}
				if workload == "scoped" {
					ids = []string{"doc-0", "doc-1"}
				}
				query := bm25Original
				if shape == "materialized" {
					query = queryBM25WithDB
				}
				// Capture the exact SQL passed to database/sql and its plan outside
				// measurement. The recorder's metadata lookup is not benchmarked.
				recorder := &bm25Recorder{docQueryer: sc}
				_, err = query(ctx, recorder, "search", 10, filter, ids)
				require.NoError(b, err)
				rows, err := sc.QueryContext(ctx, "EXPLAIN "+recorder.query, recorder.args...)
				require.NoError(b, err)
				var plan strings.Builder
				for rows.Next() {
					var key, value string
					if err := rows.Scan(&key, &value); err != nil {
						rows.Close()
						b.Fatal(err)
					}
					plan.WriteString(value)
				}
				require.NoError(b, rows.Err())
				require.NoError(b, rows.Close())
				encoded, err := json.Marshal(map[string]any{"case": workload + "_" + shape, "sql": recorder.query, "plan": plan.String()})
				require.NoError(b, err)
				fmt.Printf("BM25_PLAN %s\n", encoded)
				run := func() {
					result, err := query(ctx, sc, "search", 10, filter, ids)
					require.NoError(b, err)
					want := 10
					if workload == "scoped" {
						want = 2
					}
					require.Len(b, result, want)
					for _, r := range result {
						require.Greater(b, r.Distance, float64(0))
						if workload == "filtered" {
							require.Equal(b, float64(1), r.Metadata["parity"])
						}
						if workload == "scoped" {
							require.Contains(b, ids, r.ID)
						}
					}
				}
				for range 10 {
					run()
				}
				b.ReportAllocs()
				for b.Loop() {
					run()
				}
			})
		}
	}
}

func TestBM25Shape(t *testing.T) {
	_, sc, _ := shardQueryFixture(t, 8)
	ctx := t.Context()
	// Vary lengths and term frequency so equivalence checks exercise ranking,
	// rather than just comparing a top-k subset of tied documents.
	_, err := sc.db.ExecContext(ctx, `UPDATE docs SET content = repeat('alpha ', CAST(substr(id, 5) AS INTEGER) + 1) || 'beta gamma';
		PRAGMA create_fts_index('docs', 'id', 'content', stemmer='none', stopwords='none')`)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, text string
		k          int
		filter     *search.FilterExpr
		ids        []string
	}{
		{name: "rank", text: "alpha", k: 3}, {name: "all", text: "alpha beta", k: 100},
		{name: "absent", text: "missing", k: 10}, {name: "quoted", text: "alpha' beta", k: 100},
		{name: "scoped", text: "alpha", k: 10, ids: []string{"doc-2", "doc-7"}},
		{name: "empty_scope", text: "alpha", k: 10, ids: []string{}},
		{name: "filtered", text: "alpha", k: 3, filter: &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: float64(1)}},
		{name: "combined", text: "alpha", k: 10, ids: []string{"doc-2", "doc-7"}, filter: &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: float64(1)}},
		{name: "empty", text: " ", k: 10}, {name: "zero", text: "alpha", k: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := bm25Original(ctx, sc.db, tc.text, tc.k, tc.filter, tc.ids)
			require.NoError(t, err)
			got, err := queryBM25WithDB(ctx, sc.db, tc.text, tc.k, tc.filter, tc.ids)
			require.NoError(t, err)
			if tc.name == "rank" || tc.name == "all" {
				require.NotEmpty(t, want)
			}
			// Tie order is not specified by SQL. Compare memberships and payloads;
			// separately ensure returned scores remain in descending order.
			require.True(t, sort.SliceIsSorted(got, func(i, j int) bool { return got[i].Distance > got[j].Distance }))
			require.ElementsMatch(t, want, got)
		})
	}
	t.Run("or_filter", func(t *testing.T) {
		filter := &search.FilterExpr{Or: []search.FilterExpr{
			{Field: "parity", Op: search.FilterOpEq, Value: float64(0)},
			{Field: "parity", Op: search.FilterOpEq, Value: float64(1)},
		}}
		// Record the existing precedence defect rather than asserting equivalence
		// to it: current SQL is p0 OR (p1 AND score IS NOT NULL). The candidate
		// correctly applies the non-NULL condition to every filtered row.
		_, err := bm25Original(ctx, sc.db, "missing", 10, filter, nil)
		require.ErrorContains(t, err, "converting NULL")
		got, err := queryBM25WithDB(ctx, sc.db, "missing", 10, filter, nil)
		require.NoError(t, err)
		require.Empty(t, got)
		want, err := bm25Original(ctx, sc.db, "alpha", 3, filter, nil)
		require.NoError(t, err)
		got, err = queryBM25WithDB(ctx, sc.db, "alpha", 3, filter, nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("duplicate_ids", func(t *testing.T) {
		db := openHandleReuseTestDB(t, newHandleReuseTestFormat(t))
		_, err := db.ExecContext(ctx, `CREATE TABLE docs (id TEXT, content TEXT, media_refs TEXT, metadata TEXT);
			INSERT INTO docs VALUES ('same', 'alpha', NULL, '{"parity":1}'), ('same', 'gamma', NULL, '{"parity":0}');
			PRAGMA create_fts_index('docs', 'id', 'content', stemmer='none', stopwords='none')`)
		require.NoError(t, err)
		for _, tc := range []struct {
			text   string
			parity float64
		}{{"alpha", 1}, {"gamma", 0}} {
			filter := &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: tc.parity}
			want, err := bm25Original(ctx, db, tc.text, 1, filter, nil)
			require.NoError(t, err)
			require.Len(t, want, 1)
			got, err := queryBM25WithDB(ctx, db, tc.text, 1, filter, nil)
			require.NoError(t, err)
			require.Equal(t, want, got, "hydration must retain physical row identity, scope and top-k")
		}
	})
	t.Run("without_metadata", func(t *testing.T) {
		_, err := sc.db.ExecContext(ctx, "DROP INDEX docs_vec_idx; ALTER TABLE docs DROP metadata")
		require.NoError(t, err)
		want, err := bm25Original(ctx, sc.db, "alpha", 3, nil, nil)
		require.NoError(t, err)
		got, err := queryBM25WithDB(ctx, sc.db, "alpha", 3, nil, nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}
