package duckdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	kb "github.com/mikills/minnow/kb"
)

func (f *DuckDBArtifactFormat) CompactIfNeeded(ctx context.Context, kbID string) (*kb.CompactionPublishResult, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}

	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	// Acquire cluster-wide write lease to prevent concurrent compactors
	// from duplicating expensive shard builds.
	if f.deps.AcquireWriteLease != nil {
		leaseManager, lease, err := f.deps.AcquireWriteLease(ctx, kbID)
		if err != nil {
			return nil, err
		}
		defer func() { _ = leaseManager.Release(context.Background(), lease) }()
	}

	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return nil, kb.ErrKBUninitialized
		}
		return nil, err
	}

	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return nil, err
	}

	candidates, _ := selectCompactionCandidatesWithReason(f.deps.ShardingPolicy, manifest)
	if len(candidates) < 2 {
		return &kb.CompactionPublishResult{
			Performed:          false,
			ManifestVersionOld: doc.Version,
		}, nil
	}

	replacement, err := f.buildAndUploadCompactionReplacement(ctx, kbID, candidates)
	if err != nil {
		return nil, err
	}

	nextManifest := buildCompactedManifest(kbID, manifest, candidates, replacement)
	newVersion, err := f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, nextManifest, doc.Version)
	if err != nil {
		return nil, err
	}

	if f.deps.EnqueueReplacedShardsForGC != nil {
		f.deps.EnqueueReplacedShardsForGC(kbID, candidates, time.Now().UTC())
	}

	f.deps.Metrics.RecordShardCount(kbID, len(nextManifest.Shards))

	return &kb.CompactionPublishResult{
		Performed:          true,
		ReplacedShards:     append([]kb.SnapshotShardMetadata(nil), candidates...),
		ReplacementShards:  []kb.SnapshotShardMetadata{replacement},
		ManifestVersionOld: doc.Version,
		ManifestVersionNew: newVersion,
	}, nil
}

func (f *DuckDBArtifactFormat) buildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []kb.SnapshotShardMetadata) (kb.SnapshotShardMetadata, error) {
	tmpDir, err := os.MkdirTemp("", "minnow-compact-*")
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	defer os.RemoveAll(tmpDir)

	combinedPath := filepath.Join(tmpDir, "replacement.duckdb")
	db, err := f.openConfiguredDB(ctx, combinedPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	defer db.Close()

	vectorRows := int64(0)
	for i, shard := range shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("in-%05d.duckdb", i))
		if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return kb.SnapshotShardMetadata{}, err
		}

		alias := fmt.Sprintf("s%d", i)
		if err := mergeShardIntoDB(ctx, db, alias, partPath, i == 0); err != nil {
			return kb.SnapshotShardMetadata{}, err
		}

		vectorRows += shard.VectorRows
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&vectorRows); err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	centroid, err := computeShardCentroid(ctx, db)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	graphAvailable := hasGraphQueryData(ctx, db)
	if err := CheckpointAndCloseDB(ctx, db, "close compacted shard db"); err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	sha, err := kb.FileContentSHA256(combinedPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	info, err := os.Stat(combinedPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	now := time.Now().UTC()
	replacementID := fmt.Sprintf("compact-%d", now.UnixNano())
	replacementKey := fmt.Sprintf("%s.duckdb.compacted/%s/part-00000", kbID, replacementID)
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, replacementKey, combinedPath, "")
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	return kb.SnapshotShardMetadata{
		ShardID:        replacementID,
		Key:            replacementKey,
		Version:        uploadInfo.Version,
		SizeBytes:      info.Size(),
		VectorRows:     vectorRows,
		CreatedAt:      now,
		SealedAt:       now,
		TombstoneRatio: 0,
		GraphAvailable: graphAvailable,
		Centroid:       centroid,
		SHA256:         sha,
	}, nil
}

// BuildAndUploadCompactionReplacement merges the given shards into one replacement shard and uploads it.
func (f *DuckDBArtifactFormat) BuildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []kb.SnapshotShardMetadata) (kb.SnapshotShardMetadata, error) {
	return f.buildAndUploadCompactionReplacement(ctx, kbID, shards)
}
