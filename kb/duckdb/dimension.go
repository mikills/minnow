package duckdb

import (
	"context"
	"database/sql"
	"fmt"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/dimension"
)

func duckDBEmbeddingDimension(ctx context.Context, db *sql.DB) (int, error) {
	return dimension.ReadDuckDBEmbedding(ctx, db)
}

func validatePreparedDocDimensions(docs []preparedUpsertDoc, expected int, operation string) error {
	for _, doc := range docs {
		if err := dimension.ValidateVector(doc.Embedding, expected, fmt.Sprintf("%s for doc %q", operation, doc.Doc.ID)); err != nil {
			return err
		}
	}
	return nil
}

func validateQueryVectorDimensionForShards(queryVec []float32, shards []kb.SnapshotShardMetadata) error {
	return dimension.ValidateQueryVectorForShards(queryVec, shards)
}
