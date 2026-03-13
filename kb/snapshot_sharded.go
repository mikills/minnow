// Snapshot publishing and reconstruction for sharded knowledge bases.
//
// This file implements the two-direction transfer of a KB between local DuckDB
// files and BlobStore-backed shard objects:
//
//   - Upload: a mutable local DuckDB is split into fixed-size, independently
//     queryable shard files, each uploaded as an immutable blob, and a manifest
//     JSON is published to replace the previous snapshot.
//   - Download: the manifest is fetched, every shard is downloaded and verified,
//     and all shard tables are merged into a single reconstructed DuckDB file
//     with a fresh HNSW index.

package kb

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	DefaultSnapshotShardSize   int64  = 16 * 1024 * 1024
	ShardManifestLayoutDuckDBs string = "duckdb_shard_files"
)

// SnapshotShardMetadata describes one logical shard file in the manifest.
type SnapshotShardMetadata struct {
	ShardID        string    `json:"shard_id"`
	Key            string    `json:"key"`
	Version        string    `json:"version,omitempty"`
	SizeBytes      int64     `json:"size_bytes"`
	VectorRows     int64     `json:"vector_rows"`
	CreatedAt      time.Time `json:"created_at"`
	SealedAt       time.Time `json:"sealed_at,omitempty"`
	TombstoneRatio float64   `json:"tombstone_ratio"`
	GraphAvailable bool      `json:"graph_available"`
	Centroid       []float32 `json:"centroid,omitempty"`
	SHA256         string    `json:"sha256,omitempty"`
}

// SnapshotShardManifest describes a sharded snapshot using one manifest key.
type SnapshotShardManifest struct {
	SchemaVersion  int                     `json:"schema_version"`
	Layout         string                  `json:"layout,omitempty"`
	FormatKind     string                  `json:"format_kind,omitempty"`
	FormatVersion  int                     `json:"format_version,omitempty"`
	KBID           string                  `json:"kb_id"`
	CreatedAt      time.Time               `json:"created_at"`
	TotalSizeBytes int64                   `json:"total_size_bytes"`
	Shards         []SnapshotShardMetadata `json:"shards"`
}

// ShardManifestKey returns the blob key for a KB's shard manifest.
// The ".duckdb" segment is a historical artifact name; the key is format-agnostic.
func ShardManifestKey(kbID string) string {
	return kbID + ".duckdb.manifest.json"
}

// UploadSnapshotShardedIfMatch uploads a DB snapshot as immutable shard objects
// and updates the manifest with optimistic version matching.
func (l *KB) UploadSnapshotShardedIfMatch(ctx context.Context, kbID, localDBPath, expectedManifestVersion string, partSize int64) (*BlobObjectInfo, error) {
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}

	if localDBPath == "" {
		return nil, fmt.Errorf("localDBPath cannot be empty")
	}

	if partSize <= 0 {
		partSize = DefaultSnapshotShardSize
	}

	leaseManager, lease, err := l.AcquireWriteLease(ctx, kbID)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = leaseManager.Release(context.Background(), lease)
	}()

	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}

	artifacts, err := format.BuildArtifacts(ctx, kbID, localDBPath, partSize)
	if err != nil {
		return nil, err
	}

	totalSize := int64(0)
	for _, a := range artifacts {
		totalSize += a.SizeBytes
	}

	manifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         ShardManifestLayoutDuckDBs,
		FormatKind:     format.Kind(),
		FormatVersion:  format.Version(),
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: totalSize,
		Shards:         artifacts,
	}

	newVersion, err := l.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
	if err != nil {
		if errors.Is(err, ErrBlobVersionMismatch) {
			l.recordManifestCASConflict(kbID)
		}
		return nil, err
	}

	l.recordShardCount(kbID, len(artifacts))
	return &BlobObjectInfo{
		Key:     ShardManifestKey(kbID),
		Version: newVersion,
	}, nil
}
