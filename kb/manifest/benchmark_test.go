package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
	"github.com/stretchr/testify/require"
)

func BenchmarkBlobStoreManifestGet(b *testing.B) {
	ctx := context.Background()
	for _, shards := range []int{8, 128, 1024} {
		data := benchmarkManifestJSON(b, shards)
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			store := newBenchmarkManifestBlob(data)
			benchManifestGet(b, ctx, &BlobStoreManifest{Store: store})
		})
	}
}

func benchManifestGet(b *testing.B, ctx context.Context, store *BlobStoreManifest) {
	b.Helper()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		doc, err := store.Get(ctx, "kb")
		require.NoError(b, err)
		require.NotEmpty(b, doc.Manifest.Shards)
	}
}

func benchmarkManifestJSON(b *testing.B, shardCount int) []byte {
	b.Helper()
	manifest := ShardManifest{
		SchemaVersion:  1,
		Layout:         ShardManifestLayoutDuckDBs,
		FormatKind:     "duckdb_sharded",
		FormatVersion:  1,
		KBID:           "kb",
		CreatedAt:      time.Unix(1_700_000_000, 0).UTC(),
		TotalSizeBytes: int64(shardCount * 1024),
		Shards:         make([]ShardMetadata, 0, shardCount),
	}
	for i := range shardCount {
		manifest.Shards = append(manifest.Shards, ShardMetadata{
			ShardID:        fmt.Sprintf("shard-%06d", i),
			Key:            fmt.Sprintf("kb/shard-%06d.duckdb", i),
			Version:        fmt.Sprintf("version-%06d", i),
			SizeBytes:      1024,
			VectorRows:     256,
			CreatedAt:      manifest.CreatedAt,
			SealedAt:       manifest.CreatedAt.Add(time.Minute),
			TombstoneRatio: 0.01,
			GraphAvailable: i%2 == 0,
			Centroid:       []float32{0.1, 0.2, 0.3, 0.4},
			SHA256:         fmt.Sprintf("sha256-%06d", i),
			MediaIDs:       []string{fmt.Sprintf("media-%06d", i)},
		})
	}
	data, err := json.Marshal(manifest)
	require.NoError(b, err)
	return data
}

type benchmarkManifestBlob struct {
	data    []byte
	version string
}

func newBenchmarkManifestBlob(data []byte) *benchmarkManifestBlob {
	return &benchmarkManifestBlob{data: data, version: "v1"}
}

func (b *benchmarkManifestBlob) Head(context.Context, string) (*blobstore.ObjectInfo, error) {
	return &blobstore.ObjectInfo{Version: b.version, Size: int64(len(b.data))}, nil
}

func (b *benchmarkManifestBlob) DownloadBytes(context.Context, string) ([]byte, error) {
	out := make([]byte, len(b.data))
	copy(out, b.data)
	return out, nil
}

func (b *benchmarkManifestBlob) Download(_ context.Context, _ string, dest string) error {
	return os.WriteFile(dest, b.data, 0o644)
}

func (b *benchmarkManifestBlob) UploadIfMatch(context.Context, string, string, string) (*blobstore.ObjectInfo, error) {
	return nil, nil
}

func (b *benchmarkManifestBlob) Delete(context.Context, string) error { return nil }
