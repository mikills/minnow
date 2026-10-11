package duckdb

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func integrityDocument(id string) kb.EmbeddedDocument {
	return kb.EmbeddedDocument{ID: id, Text: "document " + id, Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}}
}

func publishedIntegrityIDs(t *testing.T, f *DuckDBArtifactFormat, kbID string) []string {
	t.Helper()
	doc, err := f.deps.ManifestStore.Get(t.Context(), kbID)
	require.NoError(t, err)
	var ids []string
	for _, shard := range doc.Manifest.Shards {
		path := filepath.Join(t.TempDir(), "shard.duckdb")
		require.NoError(t, f.deps.BlobStore.Download(t.Context(), shard.Key, path))
		db, err := f.openConfiguredDB(t.Context(), path)
		require.NoError(t, err)
		ids = append(ids, queryStrings(t, db, "SELECT id FROM docs ORDER BY id")...)
		require.NoError(t, db.Close())
	}
	return ids
}

func TestCompaction(t *testing.T) {
	t.Run("media_gc", testCompactionMediaGC)
}

func TestPublication(t *testing.T) {
	t.Run("conflict", testPublishConflict)
	t.Run("operation_retry", testOperationRetry)
	t.Run("refresh_version", testRefreshVersion)
	t.Run("deferred_conflict", testDeferredConflict)
	t.Run("empty_refresh", testEmptyRefresh)
}

func testCompactionMediaGC(t *testing.T) {
	ctx := t.Context()
	kbID := "compaction-media"
	store := kb.NewInMemoryMediaStore()
	h := kb.NewTestHarness(t, kbID).WithEmbedder(newFixtureEmbedder(8)).WithOptions(
		kb.WithMediaStore(store),
		kb.WithShardingPolicy(kb.ShardingPolicy{
			TargetShardBytes: 1, CompactionEnabled: true, CompactionEnabledSet: true,
			CompactionMinShardCount: 2,
		}),
	).Setup()
	t.Cleanup(h.Cleanup)
	f := registerFormatReturning(t, h)
	docs := []kb.EmbeddedDocument{integrityDocument("a"), integrityDocument("b")}
	docs[0].MediaIDs = []string{"media-a", "shared"}
	docs[1].MediaRefs = []kb.ChunkMediaRef{{MediaID: "media-b"}, {MediaID: "shared"}}
	want := []string{"media-a", "media-b", "shared"}
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range append(append([]string{}, want...), "unreferenced") {
		key := "media/" + id
		_, err := h.KB().BlobStore.UploadBytesIfMatch(ctx, key, []byte(id), "")
		require.NoError(t, err)
		require.NoError(t, store.Put(ctx, kb.MediaObject{
			ID: id, KBID: kbID, BlobKey: key, Filename: id,
			State: kb.MediaStatePending, CreatedAtUnixMs: now.Add(-48 * time.Hour).UnixMilli(),
		}))
	}
	_, err := f.PublishPrepared(ctx, kb.PreparedPublishRequest{KBID: kbID, Docs: docs, Upload: true})
	require.NoError(t, err)
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	require.NoError(t, err)
	require.Len(t, doc.Manifest.Shards, 2)
	result, err := f.CompactIfNeeded(ctx, kbID)
	require.NoError(t, err)
	require.True(t, result.Performed)
	require.Len(t, result.ReplacementShards, 1)
	// GC must use the replacement manifest to promote these pending uploads,
	// not tombstone and delete media that surviving documents still reference.
	mark, err := h.KB().SweepMediaGCMark(ctx, now)
	require.NoError(t, err)
	require.Equal(t, 1, mark.MarkedTombstone)
	deleted, err := h.KB().SweepMediaGCDelete(ctx, now.Add(2*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, deleted.Deleted)
	for _, id := range want {
		media, err := store.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, kb.MediaStateActive, media.State)
		_, err = h.KB().BlobStore.Head(ctx, media.BlobKey)
		require.NoError(t, err)
	}
	require.ElementsMatch(t, want, result.ReplacementShards[0].MediaIDs)
	doc, err = f.deps.ManifestStore.Get(ctx, kbID)
	require.NoError(t, err)
	require.ElementsMatch(t, want, doc.Manifest.Shards[0].MediaIDs)
	require.ElementsMatch(t, []string{"a", "b"}, publishedIntegrityIDs(t, f, kbID))
}

// Pauses one publication at its CAS boundary and lets an independent writer
// commit first. No sleeps or scheduler timing are needed to force the conflict.
type interleavedPublicationStore struct {
	kb.ManifestStore
	beforePublish func()
	beforeHead    func()
	beforeGet     func()
	headReads     int
	expected      []string
}

func (s *interleavedPublicationStore) HeadVersion(ctx context.Context, kbID string) (string, error) {
	s.headReads++
	// First HEAD prepares the mutation; the next HEAD checks publication.
	if hook := s.beforeHead; hook != nil && s.headReads == 2 {
		s.beforeHead = nil
		hook()
	}
	return s.ManifestStore.HeadVersion(ctx, kbID)
}

func (s *interleavedPublicationStore) Get(ctx context.Context, kbID string) (*kb.ManifestDocument, error) {
	if hook := s.beforeGet; hook != nil {
		s.beforeGet = nil
		hook()
	}
	return s.ManifestStore.Get(ctx, kbID)
}

func (s *interleavedPublicationStore) UpsertIfMatch(ctx context.Context, kbID string, manifest kb.SnapshotShardManifest, expected string) (string, error) {
	s.expected = append(s.expected, expected)
	if hook := s.beforePublish; hook != nil {
		s.beforePublish = nil
		hook()
	}
	return s.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expected)
}

func testPublishConflict(t *testing.T) {
	for _, tc := range []struct {
		name       string
		seeded     bool
		beforeHead bool
		delete     bool
		hardDelete bool
	}{
		{name: "existing_manifest_at_cas", seeded: true},
		{name: "first_publish_at_cas"},
		{name: "existing_manifest_before_publish_head", seeded: true, beforeHead: true},
		{name: "first_publish_before_publish_head", beforeHead: true},
		{name: "soft_delete", seeded: true, delete: true},
		{name: "hard_delete", seeded: true, delete: true, hardDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			kbID := "conflicting-writers"
			root := kb.SharedBlobRoot(t)
			newWriter := func() *DuckDBArtifactFormat {
				h := kb.NewTestHarness(t, kbID).WithBlobRoot(root).WithEmbedder(newFixtureEmbedder(8)).Setup()
				t.Cleanup(h.Cleanup)
				return registerFormatReturning(t, h)
			}
			loser, winner := newWriter(), newWriter()
			publish := func(f *DuckDBArtifactFormat, id string) error {
				_, err := f.PublishPrepared(ctx, kb.PreparedPublishRequest{KBID: kbID, Docs: []kb.EmbeddedDocument{integrityDocument(id)}, Upload: true})
				return err
			}
			var base string
			if tc.seeded {
				require.NoError(t, publish(loser, "seed"))
				var err error
				base, err = loser.deps.ManifestStore.HeadVersion(ctx, kbID)
				require.NoError(t, err)
			}
			var winningVersion string
			interleaved := &interleavedPublicationStore{ManifestStore: loser.deps.ManifestStore}
			hook := func() {
				require.NoError(t, publish(winner, "winner"))
				var err error
				winningVersion, err = winner.deps.ManifestStore.HeadVersion(ctx, kbID)
				require.NoError(t, err)
			}
			if tc.beforeHead {
				interleaved.beforeHead = hook
			} else {
				interleaved.beforePublish = hook
			}
			loser.deps.ManifestStore = interleaved
			mutate := func() error {
				if tc.delete {
					_, err := loser.Delete(ctx, kb.IngestDeleteRequest{
						KBID: kbID, DocIDs: []string{"seed"}, Upload: true,
						Options: kb.DeleteDocsOptions{HardDelete: tc.hardDelete},
					})
					return err
				}
				return publish(loser, "loser")
			}
			err := mutate()
			require.ErrorIs(t, err, kb.ErrBlobVersionMismatch, "a stale snapshot must not be republished over the winning writer")
			current, err := winner.deps.ManifestStore.HeadVersion(ctx, kbID)
			require.NoError(t, err)
			require.Equal(t, winningVersion, current)
			want := []string{"winner"}
			if tc.seeded {
				want = append(want, "seed")
			}
			require.ElementsMatch(t, want, publishedIntegrityIDs(t, winner, kbID))
			for _, expected := range interleaved.expected {
				require.Equal(t, base, expected, "publish retry changed the source snapshot's CAS base")
			}
			// Retrying the whole operation refreshes/reconstructs first, so this
			// time both writers' mutations can be retained safely.
			require.NoError(t, mutate())
			if tc.delete {
				want = []string{"winner"}
			} else {
				want = append(want, "loser")
			}
			require.ElementsMatch(t, want, publishedIntegrityIDs(t, loser, kbID))
		})
	}
}

func testOperationRetry(t *testing.T) {
	ctx := t.Context()
	kbID := "operation-retry"
	root := kb.SharedBlobRoot(t)
	newWriter := func() (*kb.KB, *DuckDBArtifactFormat) {
		h := kb.NewTestHarness(t, kbID).WithBlobRoot(root).WithEmbedder(newFixtureEmbedder(8)).Setup()
		t.Cleanup(h.Cleanup)
		return h.KB(), registerFormatReturning(t, h)
	}
	loader, loser := newWriter()
	winnerLoader, _ := newWriter()
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{ID: "seed", Text: "seed"}}))
	var observed []kb.MutationRetryStats
	loader.SetMutationRetryObserver(kb.MutationRetryObserverFunc(func(stats kb.MutationRetryStats) {
		observed = append(observed, stats)
	}))
	interleaved := &interleavedPublicationStore{ManifestStore: loser.deps.ManifestStore}
	interleaved.beforePublish = func() {
		require.NoError(t, winnerLoader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{ID: "winner", Text: "winner"}}))
	}
	loser.deps.ManifestStore = interleaved
	require.NoError(t, loader.UpsertDocsAndUploadWithRetry(ctx, kbID, []kb.Document{{ID: "loser", Text: "loser"}}, 1))
	require.Len(t, observed, 1)
	require.True(t, observed[0].Success)
	require.Equal(t, 2, observed[0].Attempts)
	require.Equal(t, 1, observed[0].ConflictCount)
	require.ElementsMatch(t, []string{"seed", "winner", "loser"}, publishedIntegrityIDs(t, loser, kbID))
}

func testRefreshVersion(t *testing.T) {
	ctx := t.Context()
	kbID := "refresh-version-race"
	root := kb.SharedBlobRoot(t)
	newWriter := func() *DuckDBArtifactFormat {
		h := kb.NewTestHarness(t, kbID).WithBlobRoot(root).WithEmbedder(newFixtureEmbedder(8)).Setup()
		t.Cleanup(h.Cleanup)
		return registerFormatReturning(t, h)
	}
	winner, refreshing := newWriter(), newWriter()
	publish := func(f *DuckDBArtifactFormat, id string) {
		_, err := f.PublishPrepared(ctx, kb.PreparedPublishRequest{KBID: kbID, Docs: []kb.EmbeddedDocument{integrityDocument(id)}, Upload: true})
		require.NoError(t, err)
	}
	publish(winner, "seed")
	interleaved := &interleavedPublicationStore{ManifestStore: refreshing.deps.ManifestStore}
	var sourceVersion string
	interleaved.beforeGet = func() {
		publish(winner, "winner")
		var err error
		sourceVersion, err = winner.deps.ManifestStore.HeadVersion(ctx, kbID)
		require.NoError(t, err)
	}
	refreshing.deps.ManifestStore = interleaved
	publish(refreshing, "refreshed")
	require.Equal(t, []string{sourceVersion}, interleaved.expected)
	require.ElementsMatch(t, []string{"seed", "winner", "refreshed"}, publishedIntegrityIDs(t, refreshing, kbID))
}

func testDeferredConflict(t *testing.T) {
	ctx := t.Context()
	kbID := "deferred-conflict"
	root := kb.SharedBlobRoot(t)
	newWriter := func() (*DuckDBArtifactFormat, string) {
		h := kb.NewTestHarness(t, kbID).WithBlobRoot(root).WithEmbedder(newFixtureEmbedder(8)).Setup()
		t.Cleanup(h.Cleanup)
		return registerFormatReturning(t, h), filepath.Join(h.CacheDir(), kbID)
	}
	pending, dir := newWriter()
	winner, _ := newWriter()
	publish := func(f *DuckDBArtifactFormat, id string, deferPublish bool) {
		_, err := f.PublishPrepared(ctx, kb.PreparedPublishRequest{
			KBID: kbID, Docs: []kb.EmbeddedDocument{integrityDocument(id)}, Upload: !deferPublish,
			Options: kb.UpsertDocsOptions{DeferPublish: deferPublish},
		})
		require.NoError(t, err)
	}
	publish(pending, "seed", false)
	base, err := readLocalShardManifestVersion(localShardManifestVersionPath(dir))
	require.NoError(t, err)
	publish(pending, "pending", true)
	publish(winner, "winner", false)
	require.ErrorIs(t, pending.CommitPrepared(ctx, kbID), kb.ErrBlobVersionMismatch)
	require.True(t, kb.HasPendingSession(dir))
	retainedBase, err := readLocalShardManifestVersion(localShardManifestVersionPath(dir))
	require.NoError(t, err)
	require.Equal(t, base, retainedBase)
	db, err := pending.openConfiguredDB(ctx, filepath.Join(dir, vectorsDuckDBFileName))
	require.NoError(t, err)
	ids := queryStrings(t, db, "SELECT id FROM docs ORDER BY id")
	require.NoError(t, db.Close())
	require.ElementsMatch(t, []string{"seed", "pending"}, ids)
	require.ElementsMatch(t, []string{"seed", "winner"}, publishedIntegrityIDs(t, winner, kbID))
}

func testEmptyRefresh(t *testing.T) {
	ctx := t.Context()
	kbID := "empty-manifest-refresh"
	root := kb.SharedBlobRoot(t)
	newWriter := func() *DuckDBArtifactFormat {
		h := kb.NewTestHarness(t, kbID).WithBlobRoot(root).WithEmbedder(newFixtureEmbedder(8)).Setup()
		t.Cleanup(h.Cleanup)
		return registerFormatReturning(t, h)
	}
	stale, deleting := newWriter(), newWriter()
	_, err := stale.PublishPrepared(ctx, kb.PreparedPublishRequest{KBID: kbID, Docs: []kb.EmbeddedDocument{integrityDocument("deleted")}, Upload: true})
	require.NoError(t, err)
	_, err = deleting.Delete(ctx, kb.IngestDeleteRequest{KBID: kbID, DocIDs: []string{"deleted"}, Upload: true})
	require.NoError(t, err)
	_, err = stale.PublishPrepared(ctx, kb.PreparedPublishRequest{KBID: kbID, Docs: []kb.EmbeddedDocument{integrityDocument("new")}, Upload: true})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"new"}, publishedIntegrityIDs(t, stale, kbID))
}
