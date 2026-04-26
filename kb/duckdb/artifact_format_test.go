package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestDuckDBArtifactFormat(t *testing.T) {
	t.Run("memory_limit_parse", testDuckDBMemoryLimitParse)
	t.Run("extension_dir_parse", testDuckDBExtensionDirParse)
	t.Run("graph_mode_availability", testGraphModeAvailability)
	t.Run("validate_manifest_format_requires_exact_version", testValidateManifestFormatRequiresExactVersion)
	t.Run("validate_manifest_rejects_unsupported_version", testValidateManifestFormatRejectsUnsupportedVersion)
	t.Run("validate_manifest_shard_consistency", testValidateManifestFormatShardConsistency)
	t.Run("embedding_dimension_validation", testEmbeddingDimensionValidation)
	t.Run("assert_safe_identifier", testAssertSafeIdentifier)
	t.Run("is_transient_blob_error", testIsTransientBlobError)
}

type stubManifestStore struct {
	doc *kb.ManifestDocument
	err error
}

func (s *stubManifestStore) Get(context.Context, string) (*kb.ManifestDocument, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.doc, nil
}

func (s *stubManifestStore) HeadVersion(context.Context, string) (string, error) { return "", nil }
func (s *stubManifestStore) UpsertIfMatch(context.Context, string, kb.SnapshotShardManifest, string) (string, error) {
	return "", nil
}
func (s *stubManifestStore) Delete(context.Context, string) error { return nil }

func testGraphModeAvailability(t *testing.T) {
	newFormat := func(manifest kb.SnapshotShardManifest) *DuckDBArtifactFormat {
		return &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{ManifestStore: &stubManifestStore{
			doc: &kb.ManifestDocument{Manifest: manifest, Version: "v1"},
		}}}
	}

	t.Run("allows_mixed_shards_when_any_graph_available", func(t *testing.T) {
		f := newFormat(kb.SnapshotShardManifest{
			FormatKind:    DuckDBFormatKind,
			FormatVersion: DuckDBFormatVersion,
			Shards: []kb.SnapshotShardMetadata{
				{ShardID: "a", GraphAvailable: false},
				{ShardID: "b", GraphAvailable: true},
			},
		})
		err := f.ensureGraphModeAvailable(context.Background(), "kb")
		require.NoError(t, err)
	})

	t.Run("rejects_when_no_graph_shards_available", func(t *testing.T) {
		f := newFormat(kb.SnapshotShardManifest{
			FormatKind:    DuckDBFormatKind,
			FormatVersion: DuckDBFormatVersion,
			Shards: []kb.SnapshotShardMetadata{
				{ShardID: "a", GraphAvailable: false},
			},
		})
		err := f.ensureGraphModeAvailable(context.Background(), "kb")
		require.Error(t, err)
		require.True(t, errors.Is(err, kb.ErrGraphQueryUnavailable))
	})
}

func testValidateManifestFormatRequiresExactVersion(t *testing.T) {
	f := &DuckDBArtifactFormat{}
	require.NoError(t, f.validateManifestFormat(&kb.SnapshotShardManifest{
		FormatKind:    DuckDBFormatKind,
		FormatVersion: DuckDBFormatVersion,
	}))
	err := f.validateManifestFormat(&kb.SnapshotShardManifest{
		FormatKind:    DuckDBFormatKind,
		FormatVersion: 1,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, kb.ErrArtifactFormatNotConfigured)
}

func testValidateManifestFormatRejectsUnsupportedVersion(t *testing.T) {
	f := &DuckDBArtifactFormat{}
	err := f.validateManifestFormat(&kb.SnapshotShardManifest{
		FormatKind:    DuckDBFormatKind,
		FormatVersion: 99,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, kb.ErrArtifactFormatNotConfigured)
	require.Contains(t, err.Error(), "is not supported")
}

func testValidateManifestFormatShardConsistency(t *testing.T) {
	f := &DuckDBArtifactFormat{}
	// Valid: shards with shard_id set.
	require.NoError(t, f.validateManifestFormat(&kb.SnapshotShardManifest{
		FormatKind:    DuckDBFormatKind,
		FormatVersion: DuckDBFormatVersion,
		Shards: []kb.SnapshotShardMetadata{
			{ShardID: "shard-00000", Key: "k1"},
			{ShardID: "shard-00001", Key: "k2"},
		},
	}))
	// Invalid: a shard with empty shard_id signals a malformed manifest.
	err := f.validateManifestFormat(&kb.SnapshotShardManifest{
		FormatKind:    DuckDBFormatKind,
		FormatVersion: DuckDBFormatVersion,
		Shards: []kb.SnapshotShardMetadata{
			{ShardID: "shard-00000", Key: "k1"},
			{ShardID: "", Key: "k2"},
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, kb.ErrArtifactFormatNotConfigured)
}

func testEmbeddingDimensionValidation(t *testing.T) {
	ctx := context.Background()
	kbID := "kb-dimension-validation"
	harness := kb.NewTestHarness(t, kbID).
		WithEmbedder(newFixtureEmbedder(4)).
		Setup()
	t.Cleanup(harness.Cleanup)
	registerFormatOnHarness(t, harness)
	loader := harness.KB()

	require.NoError(t, loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{ID: "seed", Text: "seed document"}}))

	loader.Embedder = newFixtureEmbedder(3)
	err := loader.UpsertDocsAndUpload(ctx, kbID, []kb.Document{{ID: "changed", Text: "changed dimension"}})
	require.Error(t, err)
	require.ErrorIs(t, err, kb.ErrEmbeddingDimensionMismatch)
	require.Contains(t, err.Error(), "got 3 dimensions, expected 4")

	af := requireDuckDBFormat(t, loader)
	_, err = af.PublishPrepared(ctx, kb.PreparedPublishRequest{
		KBID:   kbID,
		Docs:   []kb.EmbeddedDocument{{ID: "prepared", Text: "prepared dimension", Embedding: []float32{1, 2, 3}}},
		Upload: true,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, kb.ErrEmbeddingDimensionMismatch)
	require.Contains(t, err.Error(), "got 3 dimensions, expected 4")

	_, err = loader.Search(ctx, kbID, []float32{1, 2, 3}, &kb.SearchOptions{TopK: 1})
	require.Error(t, err)
	require.ErrorIs(t, err, kb.ErrEmbeddingDimensionMismatch)
	require.Contains(t, err.Error(), "got 3 dimensions, expected 4")
}

func testAssertSafeIdentifier(t *testing.T) {
	// Valid identifiers.
	for _, good := range []string{"alias", "s0", "tbl_name", "ABC123"} {
		require.NotPanics(t, func() { AssertSafeIdentifier(good) }, "should not panic on %q", good)
	}
	// Invalid identifiers.
	for _, bad := range []string{"", "has space", "tbl;--", "tbl`x", "'inj'", "dotted.name"} {
		require.Panics(t, func() { AssertSafeIdentifier(bad) }, "should panic on %q", bad)
	}
}

func testIsTransientBlobError(t *testing.T) {
	require.False(t, isTransientBlobError(nil))
	require.False(t, isTransientBlobError(context.DeadlineExceeded))
	require.True(t, isTransientBlobError(errors.New("read tcp 127.0.0.1:1234: i/o timeout")))
	require.True(t, isTransientBlobError(errors.New("write: connection reset by peer")))
	require.True(t, isTransientBlobError(errors.New("temporary failure in name resolution")))
	require.False(t, isTransientBlobError(errors.New("permission denied")))
}

func testDuckDBMemoryLimitParse(t *testing.T) {
	val, err := normalizeDuckDBMemoryLimit("")
	require.NoError(t, err)
	assert.Equal(t, "128MB", val)

	val, err = normalizeDuckDBMemoryLimit(" 256 mb ")
	require.NoError(t, err)
	assert.Equal(t, "256 mb", val)

	_, err = normalizeDuckDBMemoryLimit("abc")
	require.Error(t, err)
}

func testDuckDBExtensionDirParse(t *testing.T) {
	t.Run("offline_missing", func(t *testing.T) {
		_, err := normalizeDuckDBExtensionDir(filepath.Join(t.TempDir(), "missing"), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "offline mode")
	})

	t.Run("online_missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing")
		val, err := normalizeDuckDBExtensionDir(path, false)
		require.NoError(t, err)
		assert.Equal(t, filepath.Clean(path), val)
	})

	t.Run("not_directory", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "ext-file")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))
		_, err := normalizeDuckDBExtensionDir(file, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a directory")
	})
}

// TestOfflineExtensionLoadAll verifies that every pre-downloaded extension
// can be loaded offline without downloading.
func TestOfflineExtensionLoadAll(t *testing.T) {
	localExtDir := ResolveExtensionDir()
	if localExtDir == "" {
		t.Skip("pre-downloaded extensions not found")
	}

	// Discover extensions from the resolved directory by scanning for
	// *.duckdb_extension files under the platform subdirectory.
	entries, err := filepath.Glob(filepath.Join(localExtDir, "*", "*", "*.duckdb_extension"))
	require.NoError(t, err)

	seen := map[string]bool{}
	for _, e := range entries {
		name := strings.TrimSuffix(filepath.Base(e), ".duckdb_extension")
		seen[name] = true
	}
	require.NotEmpty(t, seen, "expected at least one extension in %s", localExtDir)

	for ext := range seen {
		t.Run(ext, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test.duckdb")

			db, err := sql.Open("duckdb", dbPath)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec(fmt.Sprintf(`SET extension_directory = '%s'`, quoteSQLString(localExtDir)))
			require.NoError(t, err)

			_, err = db.Exec(`SET autoinstall_known_extensions = false`)
			require.NoError(t, err)

			_, err = db.Exec(fmt.Sprintf(`LOAD %s`, ext))
			require.NoError(t, err, "%s extension should load offline without downloading", ext)

			var installed bool
			err = db.QueryRow(fmt.Sprintf(
				`SELECT installed FROM duckdb_extensions() WHERE extension_name = '%s'`, ext,
			)).Scan(&installed)
			require.NoError(t, err)
			require.True(t, installed, "%s extension should be installed", ext)
		})
	}
}

// TestOfflineExtensionLoad verifies openConfiguredDB behavior with offline
// extension loading. This catches regressions where DuckDB's autoinstall
// silently downloads extensions.
func TestOfflineExtensionLoad(t *testing.T) {
	localExtDir := ResolveExtensionDir()

	tests := []struct {
		name         string
		extDir       string
		offline      bool
		wantErr      bool
		errContains  string
		skipIfNoExts bool
	}{
		{
			name:         "offline with valid local extensions",
			extDir:       localExtDir,
			offline:      true,
			wantErr:      false,
			skipIfNoExts: true,
		},
		{
			name:        "offline with empty extension dir fails",
			extDir:      "", // replaced with t.TempDir() below
			offline:     true,
			wantErr:     true,
			errContains: "offline mode",
		},
		{
			name:         "online with valid local extensions",
			extDir:       localExtDir,
			offline:      false,
			wantErr:      false,
			skipIfNoExts: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipIfNoExts && localExtDir == "" {
				t.Skip("pre-downloaded extensions not found")
			}

			tmpDir := t.TempDir()
			dbPath := filepath.Join(tmpDir, "test.duckdb")

			extDir := tt.extDir
			if extDir == "" {
				extDir = t.TempDir()
			}

			f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
				MemoryLimit:  "128MB",
				ExtensionDir: extDir,
				OfflineExt:   tt.offline,
			}}

			ctx := context.Background()
			db, err := f.openConfiguredDB(ctx, dbPath)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			defer db.Close()

			var loaded bool
			err = db.QueryRow(`SELECT installed FROM duckdb_extensions() WHERE extension_name = 'vss'`).Scan(&loaded)
			require.NoError(t, err)
			require.True(t, loaded, "vss extension should be loaded")
		})
	}
}
