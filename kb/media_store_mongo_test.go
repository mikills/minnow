package kb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestMongoMediaStore(t *testing.T) *MongoMediaStore {
	t.Helper()
	coll, _ := newTestMongoCollection(t, "media")
	s, err := NewMongoMediaStore(context.Background(), coll)
	require.NoError(t, err)
	return s
}

func mkMedia(id, kbID string) MediaObject {
	return MediaObject{
		ID: id, KBID: kbID, Filename: id + ".bin", ContentType: "application/octet-stream",
		BlobKey:   "kbs/" + kbID + "/media/" + id + "/" + id + ".bin",
		SizeBytes: 10, Checksum: "deadbeef", CreatedAtUnixMs: time.Now().UnixMilli(),
		State: MediaStatePending,
	}
}

func TestMongoMediaStore(t *testing.T) {
	t.Run("put get delete round-trip", func(t *testing.T) {
		s := newTestMongoMediaStore(t)
		ctx := context.Background()
		require.NoError(t, s.Put(ctx, mkMedia("m1", "kb")))

		got, err := s.Get(ctx, "m1")
		require.NoError(t, err)
		require.Equal(t, "kb", got.KBID)

		require.NoError(t, s.Delete(ctx, "m1"))
		_, err = s.Get(ctx, "m1")
		require.ErrorIs(t, err, ErrMediaNotFound)
	})

	t.Run("list filters by kb_id and filename prefix", func(t *testing.T) {
		s := newTestMongoMediaStore(t)
		ctx := context.Background()
		require.NoError(t, s.Put(ctx, MediaObject{ID: "m1", KBID: "kb", Filename: "reports/q1.pdf", BlobKey: "x", CreatedAtUnixMs: 1, State: MediaStatePending}))
		require.NoError(t, s.Put(ctx, MediaObject{ID: "m2", KBID: "kb", Filename: "reports/q2.pdf", BlobKey: "x", CreatedAtUnixMs: 1, State: MediaStatePending}))
		require.NoError(t, s.Put(ctx, MediaObject{ID: "m3", KBID: "kb", Filename: "photo.png", BlobKey: "x", CreatedAtUnixMs: 1, State: MediaStatePending}))
		require.NoError(t, s.Put(ctx, MediaObject{ID: "m4", KBID: "other", Filename: "report.pdf", BlobKey: "x", CreatedAtUnixMs: 1, State: MediaStatePending}))

		all, err := s.List(ctx, "kb", "", "", 0)
		require.NoError(t, err)
		require.Len(t, all.Items, 3)

		filtered, err := s.List(ctx, "kb", "reports/", "", 0)
		require.NoError(t, err)
		require.Len(t, filtered.Items, 2)
	})

	t.Run("update state and list by state", func(t *testing.T) {
		s := newTestMongoMediaStore(t)
		ctx := context.Background()
		require.NoError(t, s.Put(ctx, mkMedia("m1", "kb")))
		require.NoError(t, s.Put(ctx, mkMedia("m2", "kb")))
		require.NoError(t, s.UpdateState(ctx, "m1", MediaStateTombstoned, time.Now().UnixMilli()))

		tomb, err := s.ListByState(ctx, "", MediaStateTombstoned, "", 0)
		require.NoError(t, err)
		require.Len(t, tomb.Items, 1)
		require.Equal(t, "m1", tomb.Items[0].ID)

		pending, err := s.ListByState(ctx, "kb", MediaStatePending, "", 0)
		require.NoError(t, err)
		require.Len(t, pending.Items, 1)
		require.Equal(t, "m2", pending.Items[0].ID)

		err = s.UpdateState(ctx, "no-such-id", MediaStateActive, 0)
		require.ErrorIs(t, err, ErrMediaNotFound)
	})

	t.Run("find by idempotency key returns the original media", func(t *testing.T) {
		s := newTestMongoMediaStore(t)
		ctx := context.Background()
		m := mkMedia("m1", "kb")
		m.IdempotencyKey = "idem-1"
		require.NoError(t, s.Put(ctx, m))

		got, err := s.FindByIdempotency(ctx, "kb", "idem-1")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, "m1", got.ID)
	})
}
