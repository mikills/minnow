package manifest

import (
	"context"
	"time"
)

const ShardManifestLayoutDuckDBs = "duckdb_shard_files"

type ShardMetadata struct {
	ShardID        string    `json:"shard_id"`
	Key            string    `json:"key"`
	Version        string    `json:"version,omitempty"`
	SizeBytes      int64     `json:"size_bytes"`
	VectorRows     int64     `json:"vector_rows"`
	CreatedAt      time.Time `json:"created_at"`
	SealedAt       time.Time `json:"sealed_at"`
	TombstoneRatio float64   `json:"tombstone_ratio"`
	GraphAvailable bool      `json:"graph_available"`
	Centroid       []float32 `json:"centroid,omitempty"`
	SHA256         string    `json:"sha256,omitempty"`
	MediaIDs       []string  `json:"media_ids,omitempty"`
}

type ShardManifest struct {
	SchemaVersion  int             `json:"schema_version"`
	Layout         string          `json:"layout,omitempty"`
	FormatKind     string          `json:"format_kind,omitempty"`
	FormatVersion  int             `json:"format_version,omitempty"`
	KBID           string          `json:"kb_id"`
	CreatedAt      time.Time       `json:"created_at"`
	TotalSizeBytes int64           `json:"total_size_bytes"`
	Shards         []ShardMetadata `json:"shards"`
}

type Document struct {
	Manifest ShardManifest
	Version  string
}

type Store interface {
	Get(ctx context.Context, kbID string) (*Document, error)
	HeadVersion(ctx context.Context, kbID string) (string, error)
	UpsertIfMatch(ctx context.Context, kbID string, manifest ShardManifest, expectedVersion string) (string, error)
	Delete(ctx context.Context, kbID string) error
}

func ShardManifestKey(kbID string) string {
	return kbID + ".duckdb.manifest.json"
}
