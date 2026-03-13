package duckdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/kbcore/kb"
)

func TestDuckDBArtifactFormat(t *testing.T) {
	t.Run("memory_limit_parse", testDuckDBMemoryLimitParse)
	t.Run("extension_dir_parse", testDuckDBExtensionDirParse)
	t.Run("graph_mode_availability", testGraphModeAvailability)
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
