package main

import "testing"

// These are pure fixture/control checks, not local database or performance runs.
func TestQuerySplit(t *testing.T) {
	cases := splitCases()
	t.Run("arguments", func(t *testing.T) {
		for _, q := range cases {
			for i := 0; i < 4; i++ {
				args := q.args(i)
				if !q.parameterized {
					if len(args) != 0 {
						t.Fatal("fixed SQL acquired parameters")
					}
					continue
				}
				vector := args[0].([]float32)
				if len(vector) != 32 || vector[i%2] != 1 || vector[(i+1)%2] != 0 {
					t.Fatalf("iteration %d: incorrect bound direction %v", i, vector)
				}
			}
		}
	})
	t.Run("results", func(t *testing.T) {
		fixed, bm25, bound := cases[0], cases[1], cases[2]
		if fixed.checkID("doc-7-32", 1) != nil || bm25.checkID("doc-0-1", 1) != nil || bound.checkID("doc-0-33", 1) != nil {
			t.Fatal("valid fixture result rejected")
		}
		for _, id := range []string{"doc-0-32", "doc-0-33junk", "doc-9-33", "missing"} {
			if bound.checkID(id, 1) == nil {
				t.Fatalf("invalid/stale result accepted: %s", id)
			}
		}
	})
	t.Run("timers", func(t *testing.T) {
		for _, stage := range []string{"vector_query", "bm25_query", "prepare_vector", "fresh_execute_bm25", "reuse_execute_bound_vector", "stmt_close_vector", "adhoc_vector"} {
			if !hotQueryStage(stage) {
				t.Fatalf("memory snapshots would contaminate %s", stage)
			}
		}
		if hotQueryStage("open") || hotQueryStage("after_gc") {
			t.Fatal("lifecycle memory snapshots would be omitted")
		}
	})
}
