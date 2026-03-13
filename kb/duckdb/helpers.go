package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	kb "github.com/mikills/kbcore/kb"
)

func FormatVectorForSQL(vec []float32) string {
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = fmt.Sprintf("%.6f", v)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func tableExists(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, tableName string) (bool, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", tableName))
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found") {
			return false, nil
		}
		return false, err
	}
	return true, rows.Close()
}

func QueryTopKWithDB(ctx context.Context, db *sql.DB, queryVec []float32, k int) ([]kb.QueryResult, error) {
	if k <= 0 {
		return []kb.QueryResult{}, nil
	}
	if len(queryVec) == 0 {
		return nil, fmt.Errorf("query vector cannot be empty")
	}

	vecStr := FormatVectorForSQL(queryVec)
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, content, array_distance(embedding, %s::FLOAT[%d]) as distance
		FROM docs
		ORDER BY distance
		LIMIT %d
	`, vecStr, len(queryVec), k))
	if err != nil {
		return nil, kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("query failed: %w", err),
			"vector query dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()

	results := make([]kb.QueryResult, 0, k)
	for rows.Next() {
		var r kb.QueryResult
		if err := rows.Scan(&r.ID, &r.Content, &r.Distance); err != nil {
			return nil, fmt.Errorf("failed to scan result: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

type docMatch struct {
	Content  string
	Distance float64
}

func queryDocMatchesForIDs(ctx context.Context, db *sql.DB, queryVec []float32, ids []string) (map[string]docMatch, error) {
	if len(ids) == 0 {
		return map[string]docMatch{}, nil
	}

	vecStr := FormatVectorForSQL(queryVec)
	placeholders := kb.BuildInClausePlaceholders(len(ids))
	query := fmt.Sprintf(`
		SELECT id, content, array_distance(embedding, %s::FLOAT[%d]) as distance
		FROM docs
		WHERE id IN (%s)
	`, vecStr, len(queryVec), placeholders)

	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, kb.WrapEmbeddingDimensionMismatch(
			fmt.Errorf("distance query failed: %w", err),
			"distance query vector dimension is incompatible with stored vectors",
		)
	}
	defer rows.Close()

	results := make(map[string]docMatch, len(ids))
	for rows.Next() {
		var id string
		var content string
		var distance float64
		if err := rows.Scan(&id, &content, &distance); err != nil {
			return nil, fmt.Errorf("failed to scan query result: %w", err)
		}
		results[id] = docMatch{Content: content, Distance: distance}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows iteration error: %w", err)
	}

	return results, nil
}

func ensureGraphQueryReady(ctx context.Context, db *sql.DB) error {
	requiredTables := []string{"edges", "doc_entities"}
	for _, tableName := range requiredTables {
		ok, err := tableExists(ctx, db, tableName)
		if err != nil {
			return err
		}
		if !ok {
			return kb.ErrGraphQueryUnavailable
		}
	}

	var edgeCount int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM edges`).Scan(&edgeCount); err != nil {
		return fmt.Errorf("count edges: %w", err)
	}
	var docEntityCount int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM doc_entities`).Scan(&docEntityCount); err != nil {
		return fmt.Errorf("count doc_entities: %w", err)
	}
	if edgeCount == 0 || docEntityCount == 0 {
		return kb.ErrGraphQueryUnavailable
	}

	return nil
}

func ActiveDocCount(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM docs d
		WHERE NOT EXISTS (SELECT 1 FROM doc_tombstones t WHERE t.doc_id = d.id)
	`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active docs: %w", err)
	}
	return count, nil
}

func ensureDocTombstonesTable(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS doc_tombstones (
			doc_id TEXT PRIMARY KEY,
			deleted_at TIMESTAMP
		)
	`); err != nil {
		return fmt.Errorf("create doc_tombstones table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_doc_tombstones_doc_id ON doc_tombstones(doc_id)`); err != nil {
		return fmt.Errorf("create doc_tombstones index: %w", err)
	}
	return nil
}

func CheckpointDB(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CHECKPOINT`); err != nil {
		return fmt.Errorf("checkpoint db: %w", err)
	}
	return nil
}

func CheckpointAndCloseDB(ctx context.Context, db *sql.DB, closeContext string) error {
	if err := CheckpointDB(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("%s: %w", closeContext, err)
	}
	return nil
}
