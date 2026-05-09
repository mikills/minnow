package duckdb

import (
	"context"
	"database/sql"

	kb "github.com/mikills/minnow/kb"
	graph "github.com/mikills/minnow/kb/duckdb/internal/graph"
)

func EnsureGraphTables(ctx context.Context, db *sql.DB) error {
	return graph.EnsureTables(ctx, db)
}

func InsertGraphBuildResult(ctx context.Context, db *sql.DB, result *kb.GraphBuildResult) error {
	return graph.InsertBuildResult(ctx, db, result)
}

func InsertGraphBuildResultTx(ctx context.Context, tx *sql.Tx, result *kb.GraphBuildResult) error {
	return graph.InsertBuildResultTx(ctx, tx, result)
}
