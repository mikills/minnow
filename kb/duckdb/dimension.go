package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	kb "github.com/mikills/minnow/kb"
)

func duckDBEmbeddingDimension(ctx context.Context, db *sql.DB) (int, error) {
	var dataType string
	err := db.QueryRowContext(ctx, `
		SELECT data_type
		FROM information_schema.columns
		WHERE table_name = 'docs' AND column_name = 'embedding'
		LIMIT 1
	`).Scan(&dataType)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("docs.embedding column not found")
		}
		return 0, fmt.Errorf("read docs.embedding dimension: %w", err)
	}

	dim, err := parseDuckDBArrayDimension(dataType)
	if err != nil {
		return 0, fmt.Errorf("read docs.embedding dimension: %w", err)
	}
	return dim, nil
}

func parseDuckDBArrayDimension(dataType string) (int, error) {
	trimmed := strings.TrimSpace(dataType)
	open := strings.LastIndex(trimmed, "[")
	close := strings.LastIndex(trimmed, "]")
	if open < 0 || close <= open+1 {
		return 0, fmt.Errorf("expected fixed ARRAY type, got %q", dataType)
	}
	dim, err := strconv.Atoi(strings.TrimSpace(trimmed[open+1 : close]))
	if err != nil || dim <= 0 {
		if err == nil {
			err = fmt.Errorf("dimension must be > 0")
		}
		return 0, fmt.Errorf("invalid ARRAY dimension in %q: %w", dataType, err)
	}
	return dim, nil
}

func validateVectorDimension(vec []float32, expected int, operation string) error {
	if expected <= 0 || len(vec) == expected {
		return nil
	}
	return fmt.Errorf(
		"%w: %s: got %d dimensions, expected %d; existing KB vectors were built with a different embedding configuration, rebuild/re-ingest this KB",
		kb.ErrEmbeddingDimensionMismatch,
		operation,
		len(vec),
		expected,
	)
}

func validatePreparedDocDimensions(docs []preparedUpsertDoc, expected int, operation string) error {
	for _, doc := range docs {
		if err := validateVectorDimension(doc.Embedding, expected, fmt.Sprintf("%s for doc %q", operation, doc.Doc.ID)); err != nil {
			return err
		}
	}
	return nil
}

func validateQueryVectorDimensionForShards(queryVec []float32, shards []kb.SnapshotShardMetadata) error {
	var expected int
	for _, shard := range shards {
		if len(shard.Centroid) == 0 {
			continue
		}
		if expected == 0 {
			expected = len(shard.Centroid)
			continue
		}
		if len(shard.Centroid) != expected {
			return fmt.Errorf("manifest has inconsistent shard centroid dimensions")
		}
	}
	return validateVectorDimension(queryVec, expected, "query vector dimension is incompatible with stored vectors")
}
