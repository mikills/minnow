package dimension

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	kb "github.com/mikills/minnow/kb"
)

func ReadDuckDBEmbedding(ctx context.Context, db *sql.DB) (int, error) {
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
	dim, err := ParseDuckDBArray(dataType)
	if err != nil {
		return 0, fmt.Errorf("read docs.embedding dimension: %w", err)
	}
	return dim, nil
}

func ParseDuckDBArray(dataType string) (int, error) {
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

func ValidateVector(vec []float32, expected int, operation string) error {
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

func ValidateQueryVectorForShards(queryVec []float32, shards []kb.SnapshotShardMetadata) error {
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
	return ValidateVector(queryVec, expected, "query vector dimension is incompatible with stored vectors")
}
