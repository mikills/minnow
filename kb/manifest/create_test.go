package manifest

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/kb/blobstore"
)

// Hides optional create methods while retaining the ordinary Copy contract.
type copyOnlyManifestBlob struct{ blobstore.Store }

func testManifestCreate(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "file_create"
		if wrapped {
			name = "copy_create"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			root := t.TempDir()
			newStore := func() *BlobStoreManifest {
				var blobs blobstore.Store = &blobstore.LocalBlobStore{Root: root}
				if wrapped {
					blobs = copyOnlyManifestBlob{Store: blobs}
				}
				return &BlobStoreManifest{Store: blobs}
			}
			const writers = 8
			versions := make([]string, writers)
			errs := make([]error, writers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range writers {
				store := newStore() // Independent instances, no shared mutex.
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					versions[i], errs[i] = store.UpsertIfMatch(ctx, "kb", ShardManifest{KBID: "kb", TotalSizeBytes: int64(i)}, "")
				}()
			}
			close(start)
			wg.Wait()
			wins := 0
			var winnerVersion string
			var winner int
			for i, err := range errs {
				if err == nil {
					wins++
					winner, winnerVersion = i, versions[i]
				} else {
					require.ErrorIs(t, err, blobstore.ErrVersionMismatch)
				}
			}
			require.Equal(t, 1, wins)
			store := newStore()
			doc, err := store.Get(ctx, "kb")
			require.NoError(t, err)
			require.Equal(t, winnerVersion, doc.Version)
			require.EqualValues(t, winner, doc.Manifest.TotalSizeBytes)
			_, err = store.UpsertIfMatch(ctx, "kb", ShardManifest{KBID: "kb", TotalSizeBytes: 999}, "")
			require.ErrorIs(t, err, blobstore.ErrVersionMismatch)
			_, err = store.UpsertIfMatch(ctx, "kb", ShardManifest{KBID: "kb", TotalSizeBytes: 999}, winnerVersion)
			require.NoError(t, err)
			objects, err := (&blobstore.LocalBlobStore{Root: root}).List(ctx, "")
			require.NoError(t, err)
			require.Len(t, objects, 1, "temporary manifest staging objects were not cleaned up")
		})
	}
}

type basicManifestBlob struct{ BlobStore }

func testMissingCreateSupport(t *testing.T) {
	blobs := &blobstore.LocalBlobStore{Root: t.TempDir()}
	store := &BlobStoreManifest{Store: basicManifestBlob{BlobStore: blobs}}
	_, err := store.UpsertIfMatch(t.Context(), "kb", ShardManifest{KBID: "kb"}, "")
	require.ErrorContains(t, err, "atomic create support")
	_, err = blobs.Head(t.Context(), ShardManifestKey("kb"))
	require.True(t, errors.Is(err, os.ErrNotExist))
}
