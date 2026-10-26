package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestMutableOpen(t *testing.T) {
	setup := func(t *testing.T) (*DuckDBArtifactFormat, mutableShardRequest) {
		t.Helper()
		f := newHandleReuseTestFormat(t)
		f.deps.ManifestStore = &stubManifestStore{}
		dir := t.TempDir()
		return f, mutableShardRequest{kbID: "kb", kbDir: dir,
			dbPath: filepath.Join(dir, vectorsDuckDBFileName), embeddingDim: 8, allowBootstrap: true}
	}
	closeDB := func(t *testing.T, f *DuckDBArtifactFormat, db *sql.DB) {
		t.Helper()
		require.NoError(t, db.Close())
		require.Zero(t, f.budget().LiveDatabases())
	}
	countRows := func(t *testing.T, db *sql.DB, want int) {
		t.Helper()
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM docs").Scan(&count))
		require.Equal(t, want, count)
	}

	t.Run("bootstrap_returns_mutation_handle", func(t *testing.T) {
		f, req := setup(t)
		db, err := f.prepareMutableShardDBLocked(t.Context(), req)
		require.NoError(t, err)
		t.Cleanup(func() { closeDB(t, f, db) })
		require.EqualValues(t, 1, f.budget().DatabaseOpenAttempts())
		require.EqualValues(t, 1, f.budget().LiveDatabases())
		_, err = db.ExecContext(t.Context(), "INSERT INTO docs VALUES ('first', 'text', [1,0,0,0,0,0,0,0], NULL, NULL)")
		require.NoError(t, err)
		require.NoError(t, CheckpointDB(t.Context(), db))
		countRows(t, db, 1)
	})

	t.Run("session_repair_returns_mutation_handle", func(t *testing.T) {
		f, req := setup(t)
		db, err := f.openConfiguredDB(t.Context(), req.dbPath)
		require.NoError(t, err)
		// A legacy unpublished database also needs metadata migration.
		_, err = db.ExecContext(t.Context(), `CREATE TABLE docs (id TEXT, content TEXT, embedding FLOAT[8], media_refs TEXT);
			INSERT INTO docs VALUES ('saved', 'text', [1,0,0,0,0,0,0,0], NULL)`)
		require.NoError(t, err)
		closeDB(t, f, db)
		require.NoError(t, kb.MarkPendingSession(req.kbDir))
		req.joinsSession = true
		before := f.budget().DatabaseOpenAttempts()
		db, err = f.prepareMutableShardDBLocked(t.Context(), req)
		require.NoError(t, err)
		t.Cleanup(func() { closeDB(t, f, db) })
		require.EqualValues(t, 1, f.budget().DatabaseOpenAttempts()-before)
		countRows(t, db, 1)
		_, err = db.ExecContext(t.Context(), `UPDATE docs SET metadata = '{"kept":true}' WHERE id = 'saved'`)
		require.NoError(t, err)
		require.True(t, kb.HasPendingSession(req.kbDir), "opening a handle must not publish or end the session")
	})

	t.Run("staging_can_mutate_persisted_indexes_without_loading_fts", func(t *testing.T) {
		f, req := setup(t)
		db, err := f.openBootstrapMutableShardDB(t.Context(), req.kbDir, req.dbPath, req.embeddingDim)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		var loaded bool
		require.NoError(t, db.QueryRow(`SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'fts'`).Scan(&loaded))
		require.False(t, loaded)
		_, err = db.Exec(`INSERT INTO docs VALUES ('a', 'original', [1,0,0,0,0,0,0,0], NULL, NULL),
			('b', 'other', [0,1,0,0,0,0,0,0], NULL, NULL)`)
		require.NoError(t, err)
		closeDB(t, f, db)

		// A reconstructed single-shard source can carry both persisted indexes.
		db, err = f.openConfiguredDB(t.Context(), req.dbPath)
		require.NoError(t, err)
		require.NoError(t, buildShardIndexes(t.Context(), db))
		require.NoError(t, checkpointAndCloseMutationDB(t.Context(), db))
		db, err = f.openMutableShardDB(t.Context(), req.dbPath)
		require.NoError(t, err)
		require.NoError(t, db.QueryRow(`SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'fts'`).Scan(&loaded))
		require.False(t, loaded)
		_, err = db.Exec(`UPDATE docs SET content = 'revised' WHERE id = 'a'; DELETE FROM docs WHERE id = 'b'`)
		require.NoError(t, err)
		require.NoError(t, checkpointAndCloseMutationDB(t.Context(), db))

		// Builders/readers still load FTS and can rebuild/query the changed rows.
		db, release, err := f.openBuildDB(t.Context(), req.dbPath)
		require.NoError(t, err)
		defer release()
		require.NoError(t, buildShardIndexes(t.Context(), db))
		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM docs WHERE
			fts_main_docs.match_bm25(id, 'revised', fields := 'content') IS NOT NULL`).Scan(&count))
		require.Equal(t, 1, count)
		closeDB(t, f, db)
	})

	t.Run("public_direct_access_keeps_fts", func(t *testing.T) {
		f, req := setup(t)
		db, err := f.prepareMutableShardDBLocked(t.Context(), req)
		require.NoError(t, err)
		closeDB(t, f, db)
		require.NoError(t, writeLocalShardManifestVersion(localShardManifestVersionPath(req.kbDir), "same"))
		f.deps.ManifestStore = &stubManifestStore{head: "same"}
		f.deps.CacheDir = filepath.Dir(req.kbDir)
		f.deps.EvictCacheIfNeeded = func(context.Context, string) error { return nil }
		var lock sync.Mutex
		f.deps.LockFor = func(string) *sync.Mutex { return &lock }
		db, err = f.PrepareAndOpenDB(t.Context(), filepath.Base(req.kbDir))
		require.NoError(t, err)
		t.Cleanup(func() { closeDB(t, f, db) })
		var loaded bool
		require.NoError(t, db.QueryRow(`SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'fts'`).Scan(&loaded))
		require.True(t, loaded)
	})

	for _, published := range []bool{false, true} {
		name := "deleted_manifest_replaces_stale_rows"
		if published {
			name = "empty_manifest_replaces_stale_rows"
		}
		t.Run(name, func(t *testing.T) {
			f, req := setup(t)
			db, err := f.prepareMutableShardDBLocked(t.Context(), req)
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), "INSERT INTO docs VALUES ('stale', 'old', [1,0,0,0,0,0,0,0], NULL, NULL)")
			require.NoError(t, err)
			closeDB(t, f, db)
			require.NoError(t, writeLocalShardManifestVersion(localShardManifestVersionPath(req.kbDir), "old"))
			version := ""
			if published {
				version = "empty"
				f.deps.ManifestStore = &stubManifestStore{head: version, doc: &kb.ManifestDocument{Version: version}}
			}
			before := f.budget().DatabaseOpenAttempts()
			db, err = f.prepareMutableShardDBLocked(t.Context(), req)
			require.NoError(t, err)
			t.Cleanup(func() { closeDB(t, f, db) })
			// Replacement still checkpoints/closes the temporary file before
			// rename; the second open at its final path is intentional.
			require.EqualValues(t, 2, f.budget().DatabaseOpenAttempts()-before)
			require.EqualValues(t, 1, f.budget().LiveDatabases())
			countRows(t, db, 0)
			got, err := readLocalShardManifestVersion(localShardManifestVersionPath(req.kbDir))
			require.NoError(t, err)
			require.Equal(t, version, got)
		})
	}

	t.Run("version_write_failure_closes_handle", func(t *testing.T) {
		f, req := setup(t)
		// Reading a dangling marker reports absence; writing it cannot succeed.
		require.NoError(t, os.Symlink(filepath.Join(req.kbDir, "missing", "version"), localShardManifestVersionPath(req.kbDir)))
		db, err := f.prepareMutableShardDBLocked(t.Context(), req)
		require.Error(t, err)
		require.Nil(t, db)
		require.EqualValues(t, 1, f.budget().DatabaseOpenAttempts())
		require.Zero(t, f.budget().LiveDatabases())
	})

	t.Run("schema_failure_closes_handle", func(t *testing.T) {
		f, req := setup(t)
		db, err := f.openConfiguredDB(t.Context(), req.dbPath)
		require.NoError(t, err)
		_, err = db.ExecContext(t.Context(), "CREATE TABLE doc_tombstones (wrong_column TEXT)")
		require.NoError(t, err)
		closeDB(t, f, db)
		db, err = f.openBootstrapMutableShardDB(t.Context(), req.kbDir, req.dbPath, req.embeddingDim)
		require.Error(t, err)
		require.Nil(t, db)
		require.Zero(t, f.budget().LiveDatabases())
	})

	t.Run("direct_access_eviction_failure_closes_handle", func(t *testing.T) {
		f, req := setup(t)
		db, err := f.prepareMutableShardDBLocked(t.Context(), req)
		require.NoError(t, err)
		closeDB(t, f, db)
		require.NoError(t, writeLocalShardManifestVersion(localShardManifestVersionPath(req.kbDir), "same"))
		f.deps.ManifestStore = &stubManifestStore{head: "same"}
		f.deps.CacheDir = filepath.Dir(req.kbDir)
		failure := errors.New("eviction failed")
		f.deps.EvictCacheIfNeeded = func(context.Context, string) error { return failure }
		var lock sync.Mutex
		f.deps.LockFor = func(string) *sync.Mutex { return &lock }
		db, err = f.PrepareAndOpenDB(t.Context(), filepath.Base(req.kbDir))
		require.ErrorIs(t, err, failure)
		require.Nil(t, db)
		require.Zero(t, f.budget().LiveDatabases())
	})
}
