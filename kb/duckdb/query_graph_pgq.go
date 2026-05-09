package duckdb

import (
	"context"
	"database/sql"

	kb "github.com/mikills/minnow/kb"
	graph "github.com/mikills/minnow/kb/duckdb/internal/graph"
	"github.com/mikills/minnow/kb/duckdb/internal/pgq"
)

func expandEntityScoresDuckPGQ(
	ctx context.Context,
	db *sql.DB,
	scores map[string]float64,
	options kb.ExpansionOptions,
) (map[string]float64, error) {
	if err := graph.EnsureEntitiesTable(ctx, db); err != nil {
		return nil, err
	}
	return pgq.ExpandEntityScores(ctx, db, scores, options)
}
