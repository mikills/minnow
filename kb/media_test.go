package kb

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newMediaHarness returns a TestHarness wired with an in-memory MediaStore
// and registered cleanup - the shared setup across every media test.
func newMediaHarness(t *testing.T, kbID string) (*TestHarness, MediaStore) {
	t.Helper()
	h := NewTestHarness(t, kbID).Setup()
	t.Cleanup(h.Cleanup)
	store := NewInMemoryMediaStore()
	h.KB().MediaStore = store
	return h, store
}

func TestSanitizeMediaFilename(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"doc.pdf", "doc.pdf", false},
		{"/etc/passwd", "passwd", false},
		{"../../etc/passwd", "", true},
		{"nested/path/file.png", "file.png", false},
		{"", "", true},
		{"  ", "", true},
		{"name\x00null", "namenull", false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := SanitizeMediaFilename(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMediaBlobKey(t *testing.T) {
	require.Equal(t, "kbs/my-kb/media/med-1/doc.pdf", MediaBlobKey("my-kb", "med-1", "doc.pdf"))
}

func TestUploadMedia(t *testing.T) {
	t.Run("rejects empty body", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-empty")
		_, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID:     "kb1",
			Filename: "empty.bin",
			Body:     bytes.NewReader(nil),
		}, 0)
		require.ErrorContains(t, err, "empty")
	})

	t.Run("enforces max size", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-too-big")
		big := bytes.Repeat([]byte("a"), 1024)
		_, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID:     "kb1",
			Filename: "big.bin",
			Body:     bytes.NewReader(big),
		}, 512)
		require.ErrorContains(t, err, "max size")
	})

	t.Run("happy path stores pending media with metadata", func(t *testing.T) {
		h, store := newMediaHarness(t, "media-happy")
		res, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID:        "kb1",
			Filename:    "hello.txt",
			ContentType: "text/plain",
			Body:        strings.NewReader("hello world"),
		}, 0)
		require.NoError(t, err)
		require.NotEmpty(t, res.MediaID)
		require.Equal(t, int64(11), res.SizeBytes)
		require.Equal(t, "hello.txt", res.Filename)
		require.Contains(t, res.BlobKey, "kbs/kb1/media/")

		got, err := store.Get(context.Background(), res.MediaID)
		require.NoError(t, err)
		require.Equal(t, MediaStatePending, got.State)
		require.Equal(t, int64(11), got.SizeBytes)
		require.Equal(t, "text/plain", got.ContentType)
	})

	t.Run("idempotency key returns the existing media", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-idempotent")
		first, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID:           "kb1",
			Filename:       "hello.txt",
			ContentType:    "text/plain",
			Body:           strings.NewReader("hello world"),
			IdempotencyKey: "idem-1",
		}, 0)
		require.NoError(t, err)

		second, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID:           "kb1",
			Filename:       "different.txt",
			ContentType:    "text/plain",
			Body:           strings.NewReader("different body"),
			IdempotencyKey: "idem-1",
		}, 0)
		require.NoError(t, err)
		require.Equal(t, first.MediaID, second.MediaID)
		require.Equal(t, first.BlobKey, second.BlobKey)
	})

	t.Run("rejects path traversal filenames", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-sanitize")
		res, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID:     "kb1",
			Filename: "/etc/../passwd",
			Body:     strings.NewReader("x"),
		}, 0)
		require.Error(t, err, "traversal filename must be rejected")
		require.Nil(t, res)
	})
}

func TestMediaLifecycle(t *testing.T) {
	t.Run("PromoteReferencedMedia transitions pending media to active idempotently", func(t *testing.T) {
		h, store := newMediaHarness(t, "media-promote")

		res, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID: "kb1", Filename: "a.bin", Body: strings.NewReader("x"),
		}, 0)
		require.NoError(t, err)

		require.NoError(t, h.KB().PromoteReferencedMedia(context.Background(), "kb1", []string{res.MediaID}))
		got, _ := store.Get(context.Background(), res.MediaID)
		require.Equal(t, MediaStateActive, got.State)

		// Idempotent.
		require.NoError(t, h.KB().PromoteReferencedMedia(context.Background(), "kb1", []string{res.MediaID}))
	})

	t.Run("ValidateDocumentReferences rejects unknown media_id", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-validate-unknown")
		err := h.KB().ValidateDocumentReferences(context.Background(), "kb1", []Document{{ID: "d", Text: "x", MediaIDs: []string{"missing"}}})
		require.ErrorContains(t, err, "unknown media_id")
	})

	t.Run("ValidateDocumentReferences rejects caller-supplied blob_key", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-validate-blobkey")
		err := h.KB().ValidateDocumentReferences(context.Background(), "kb1", []Document{{ID: "d", Text: "x", MediaRefs: []ChunkMediaRef{{MediaID: "m1", BlobKey: "evil"}}}})
		require.ErrorContains(t, err, "blob_key")
	})
}

func TestMediaGC(t *testing.T) {
	t.Run("mark tombstones unreferenced pending media past TTL", func(t *testing.T) {
		h, store := newMediaHarness(t, "media-gc-mark")

		res, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID: "kb1", Filename: "unreferenced.bin", Body: strings.NewReader("x"),
		}, 0)
		require.NoError(t, err)

		// Back-date so it crosses the TTL.
		rec, _ := store.Get(context.Background(), res.MediaID)
		rec.CreatedAtUnixMs = time.Now().Add(-48 * time.Hour).UnixMilli()
		require.NoError(t, store.Put(context.Background(), *rec))

		gc, err := h.KB().SweepMediaGCMark(context.Background(), time.Now().UTC())
		require.NoError(t, err)
		require.Equal(t, 1, gc.MarkedTombstone)

		got, _ := store.Get(context.Background(), res.MediaID)
		require.Equal(t, MediaStateTombstoned, got.State)
	})

	t.Run("sweep deletes tombstoned media past the grace window", func(t *testing.T) {
		h, store := newMediaHarness(t, "media-gc-delete")

		res, err := h.KB().UploadMedia(context.Background(), MediaUploadInput{
			KBID: "kb1", Filename: "victim.bin", Body: strings.NewReader("x"),
		}, 0)
		require.NoError(t, err)

		require.NoError(t, store.UpdateState(context.Background(), res.MediaID,
			MediaStateTombstoned, time.Now().Add(-2*time.Hour).UnixMilli()))

		gc, err := h.KB().SweepMediaGCDelete(context.Background(), time.Now().UTC())
		require.NoError(t, err)
		require.Equal(t, 1, gc.Deleted)

		_, err = store.Get(context.Background(), res.MediaID)
		require.ErrorIs(t, err, ErrMediaNotFound)
	})

	t.Run("scheduler registers mark and sweep jobs", func(t *testing.T) {
		h, _ := newMediaHarness(t, "media-gc-scheduler")

		s := NewScheduler(NewInMemoryWriteLeaseManager(), time.Minute, nil, nil)
		require.NoError(t, h.KB().RegisterDefaultJobs(s))
		require.Contains(t, s.JobIDs(), MediaGCMarkJobID)
		require.Contains(t, s.JobIDs(), MediaGCSweepJobID)
	})
}
