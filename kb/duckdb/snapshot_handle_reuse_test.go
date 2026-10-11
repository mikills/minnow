package duckdb

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
	kb "github.com/mikills/minnow/kb"
)

func newHandleReuseTestFormat(t *testing.T) *DuckDBArtifactFormat {
	t.Helper()
	return &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
		MemoryLimit: "64MB", ExtensionDir: ResolveExtensionDir(), OfflineExt: true,
		Budget: budget.New(memlimit.Plan{}, false),
	}}
}

func openHandleReuseTestDB(t *testing.T, f *DuckDBArtifactFormat) *sql.DB {
	t.Helper()
	db, err := f.openConfiguredDB(t.Context(), filepath.Join(t.TempDir(), "source.duckdb"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestOpenMutableShardDBMigratesOnReturnedHandle(t *testing.T) {
	f := newHandleReuseTestFormat(t)
	path := filepath.Join(t.TempDir(), "legacy.duckdb")
	db, err := f.openConfiguredDB(t.Context(), path)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE TABLE docs (id TEXT); INSERT INTO docs VALUES ('old')")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db, err = f.openMutableShardDB(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.EqualValues(t, 1, f.budget().LiveDatabases())
	var metadata sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT metadata FROM docs WHERE id = 'old'").Scan(&metadata))
	require.False(t, metadata.Valid)
	_, err = db.ExecContext(t.Context(), "UPDATE docs SET metadata = '{\"tag\":\"new\"}' WHERE id = 'old'")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Zero(t, f.budget().LiveDatabases())

	reopened, err := f.openMutableShardDB(t.Context(), path)
	require.NoError(t, err)
	defer reopened.Close()
	var stored string
	require.NoError(t, reopened.QueryRowContext(t.Context(), "SELECT metadata FROM docs WHERE id = 'old'").Scan(&stored))
	require.Equal(t, `{"tag":"new"}`, stored)
}

func TestOpenMutableShardDBClosesOnMigrationFailure(t *testing.T) {
	f := newHandleReuseTestFormat(t)
	db, err := f.openMutableShardDB(t.Context(), filepath.Join(t.TempDir(), "missing-docs.duckdb"))
	require.Error(t, err)
	require.Nil(t, db)
	require.Zero(t, f.budget().LiveDatabases(), "failed migration retained a database share")
}

func TestCollectShardMediaIDsBorrowsHandle(t *testing.T) {
	f := newHandleReuseTestFormat(t)
	db := openHandleReuseTestDB(t, f)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "CREATE TABLE docs (id TEXT, media_refs TEXT)")
	require.NoError(t, err)
	for _, ids := range [][]string{{"a", "b"}, {"b", "c"}, {}} {
		refs, err := encodeMediaRefs(ids, nil)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO docs VALUES ('doc', ?)", refs)
		require.NoError(t, err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO docs VALUES ('empty', ''), ('bad', 'not-json'), ('null', NULL)")
	require.NoError(t, err)
	ids, err := collectShardMediaIDs(ctx, db)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, ids)
	require.NoError(t, db.PingContext(ctx))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = collectShardMediaIDs(canceled, db)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, db.PingContext(ctx))
}

func TestInspectSnapshotSourceBorrowsHandle(t *testing.T) {
	db := openHandleReuseTestDB(t, newHandleReuseTestFormat(t))
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "CREATE TABLE docs (id TEXT); INSERT INTO docs VALUES ('a'), ('b')")
	require.NoError(t, err)
	info, err := inspectSnapshotSource(ctx, db)
	require.NoError(t, err)
	require.Equal(t, snapshotSourceInfo{activeRows: 2}, info)
	_, err = db.ExecContext(ctx, "CREATE TABLE doc_tombstones (doc_id TEXT); INSERT INTO doc_tombstones VALUES ('a'), ('absent')")
	require.NoError(t, err)
	info, err = inspectSnapshotSource(ctx, db)
	require.NoError(t, err)
	require.Equal(t, snapshotSourceInfo{hasTombstones: true, activeRows: 1}, info)
	_, err = db.ExecContext(ctx, "INSERT INTO doc_tombstones VALUES ('b')")
	require.NoError(t, err)
	info, err = inspectSnapshotSource(ctx, db)
	require.NoError(t, err)
	require.Equal(t, snapshotSourceInfo{hasTombstones: true}, info)
	require.NoError(t, db.PingContext(ctx))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = inspectSnapshotSource(canceled, db)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, db.PingContext(ctx))
	_, err = db.ExecContext(ctx, "DROP TABLE docs")
	require.NoError(t, err)
	_, err = inspectSnapshotSource(ctx, db)
	require.Error(t, err)
	require.NoError(t, db.PingContext(ctx), "inspection failure must not close its borrowed handle")
}

type retrySnapshotManifestStore struct {
	kb.ManifestStore
	liveDatabases func() int64
	manifests     []kb.SnapshotShardManifest
	liveAtPublish []int64
}

func (s *retrySnapshotManifestStore) UpsertIfMatch(ctx context.Context, kbID string, manifest kb.SnapshotShardManifest, version string) (string, error) {
	s.manifests = append(s.manifests, manifest)
	s.liveAtPublish = append(s.liveAtPublish, s.liveDatabases())
	if len(s.manifests) == 1 {
		return "", kb.ErrBlobVersionMismatch
	}
	return s.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, version)
}

type observedSnapshotBlobStore struct {
	kb.BlobStore
	liveDatabases func() int64
	liveAtUpload  []int64
}

func (s *observedSnapshotBlobStore) UploadIfMatch(ctx context.Context, key, path, version string) (*kb.BlobObjectInfo, error) {
	s.liveAtUpload = append(s.liveAtUpload, s.liveDatabases())
	return s.BlobStore.UploadIfMatch(ctx, key, path, version)
}

func TestSnapshotPublishRetryPreservesSourceAndMediaMetadata(t *testing.T) {
	ctx := t.Context()
	kbID := "snapshot-retry-metadata"
	harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
	t.Cleanup(harness.Cleanup)
	f := registerFormatReturning(t, harness)
	f.deps.Budget = budget.New(memlimit.Plan{}, false)
	f.deps.ShardingPolicy.TargetShardBytes = 1 // Force one document per shard.
	manifestStore := &retrySnapshotManifestStore{ManifestStore: f.deps.ManifestStore, liveDatabases: f.budget().LiveDatabases}
	blobStore := &observedSnapshotBlobStore{BlobStore: f.deps.BlobStore, liveDatabases: f.budget().LiveDatabases}
	f.deps.ManifestStore = manifestStore
	f.deps.BlobStore = blobStore
	result, err := f.PublishPrepared(ctx, kb.PreparedPublishRequest{
		KBID: kbID, Upload: true,
		Docs: []kb.EmbeddedDocument{
			{ID: "a", Text: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, MediaIDs: []string{"m1", "m2"}},
			{ID: "b", Text: "beta", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, MediaIDs: []string{"m2", "m3"}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, result.MutatedCount)
	require.Len(t, manifestStore.manifests, 2, "expected an actual manifest CAS retry")
	for _, manifest := range manifestStore.manifests {
		require.Len(t, manifest.Shards, 2)
		var rows int64
		media := map[string]bool{}
		for _, shard := range manifest.Shards {
			rows += shard.VectorRows
			for _, id := range shard.MediaIDs {
				media[id] = true
			}
		}
		require.EqualValues(t, 2, rows)
		require.Equal(t, map[string]bool{"m1": true, "m2": true, "m3": true}, media)
	}
	require.NotEmpty(t, blobStore.liveAtUpload)
	for _, live := range append(blobStore.liveAtUpload, manifestStore.liveAtPublish...) {
		require.Zero(t, live, "checkpointed source/build handles must close before upload or publish")
	}
	require.Zero(t, f.budget().LiveDatabases())
	// Validate final uploaded files, not only in-memory manifest fields. Each
	// shard must carry only its own media refs and the post-checkpoint checksum.
	for i, shard := range manifestStore.manifests[1].Shards {
		path := filepath.Join(t.TempDir(), "download.duckdb")
		require.NoError(t, blobStore.Download(ctx, shard.Key, path))
		hash, err := kb.FileContentSHA256(ctx, path)
		require.NoError(t, err)
		require.Equal(t, shard.SHA256, hash)
		stat, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, shard.SizeBytes, stat.Size())
		db, err := f.openConfiguredDB(ctx, path)
		require.NoError(t, err)
		ids, readErr := collectShardMediaIDs(ctx, db)
		closeErr := db.Close()
		require.NoError(t, readErr)
		require.NoError(t, closeErr)
		require.ElementsMatch(t, shard.MediaIDs, ids)
		want := [][]string{{"m1", "m2"}, {"m2", "m3"}}
		require.ElementsMatch(t, want[i], ids)
	}
	require.Zero(t, f.budget().LiveDatabases())
}

func TestBuildArtifactsSourceInspectionModes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		empty      bool
		tombstones []string
		wantRows   int64
		wantMedia  []string
	}{
		{name: "empty", empty: true},
		{name: "no_tombstones", wantRows: 2, wantMedia: []string{"a", "b"}},
		{name: "partly_tombstoned", tombstones: []string{"a", "absent"}, wantRows: 1, wantMedia: []string{"b"}},
		{name: "all_tombstoned", tombstones: []string{"a", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			kbID := "inspect-" + tc.name
			harness := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).Setup()
			t.Cleanup(harness.Cleanup)
			f := registerFormatReturning(t, harness)
			f.deps.Budget = budget.New(memlimit.Plan{}, false)
			path := filepath.Join(t.TempDir(), "source.duckdb")
			db, err := f.openConfiguredDB(ctx, path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			_, err = db.ExecContext(ctx, "CREATE TABLE docs (id TEXT, content TEXT, embedding FLOAT[8], media_refs TEXT)")
			require.NoError(t, err)
			if !tc.empty {
				for _, id := range []string{"a", "b"} {
					refs, err := encodeMediaRefs([]string{id}, nil)
					require.NoError(t, err)
					_, err = db.ExecContext(ctx, "INSERT INTO docs VALUES (?, ?, [1,0,0,0,0,0,0,0], ?)", id, id, refs)
					require.NoError(t, err)
				}
			}
			if tc.tombstones != nil {
				require.NoError(t, ensureDocTombstonesTable(ctx, db))
				for _, id := range tc.tombstones {
					_, err = db.ExecContext(ctx, "INSERT INTO doc_tombstones (doc_id) VALUES (?)", id)
					require.NoError(t, err)
				}
			}
			info, err := inspectSnapshotSource(ctx, db)
			require.NoError(t, err)
			require.NoError(t, CheckpointAndCloseDB(ctx, db, "close test source"))
			for _, preinspected := range []*snapshotSourceInfo{nil, &info} {
				var shards []kb.SnapshotShardMetadata
				if preinspected == nil {
					shards, err = f.BuildArtifacts(ctx, kbID, path, 1)
				} else {
					shards, err = f.buildArtifacts(ctx, kbID, path, 1, preinspected)
				}
				require.NoError(t, err)
				var rows int64
				var ids []string
				for _, shard := range shards {
					rows += shard.VectorRows
					ids = append(ids, shard.MediaIDs...)
				}
				require.Equal(t, tc.wantRows, rows)
				require.ElementsMatch(t, tc.wantMedia, ids)
				require.Zero(t, f.budget().LiveDatabases())
			}
		})
	}
}
