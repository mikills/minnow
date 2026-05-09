package manifest

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/stretchr/testify/require"
)

func TestManifest(t *testing.T) {
	t.Run("key is format agnostic", func(t *testing.T) {
		require.Equal(t, "kb.duckdb.manifest.json", ShardManifestKey("kb"))
	})

	t.Run("blob store applies legacy defaults", func(t *testing.T) {
		ctx := context.Background()
		store := &blobstore.LocalBlobStore{Root: t.TempDir()}
		manifest := ShardManifest{SchemaVersion: 1, KBID: "kb", CreatedAt: time.Now().UTC()}
		data, err := json.Marshal(manifest)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "manifest.json")
		require.NoError(t, os.WriteFile(path, data, 0o644))
		_, err = store.UploadIfMatch(ctx, ShardManifestKey("kb"), path, "")
		require.NoError(t, err)

		doc, err := (&BlobStoreManifest{Store: store}).Get(ctx, "kb")

		require.NoError(t, err)
		require.Equal(t, "duckdb_sharded", doc.Manifest.FormatKind)
		require.Equal(t, 1, doc.Manifest.FormatVersion)
	})
}
