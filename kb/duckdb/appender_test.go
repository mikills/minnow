package duckdb

import (
	"database/sql"
	"fmt"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/connection"
	"github.com/stretchr/testify/require"
)

func TestAppender(t *testing.T) {
	open := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := connection.Open(t.Context(), "", connection.Config{
			ExtensionDir: ResolveExtensionDir(), OfflineExt: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		_, err = db.ExecContext(t.Context(), `
			CREATE TABLE docs (id TEXT, content TEXT, embedding FLOAT[2], media_refs TEXT, metadata TEXT);
			CREATE TABLE doc_tombstones (doc_id TEXT);
			INSERT INTO docs VALUES ('doc-0', 'original', [1,0], NULL, NULL);
			INSERT INTO doc_tombstones VALUES ('doc-1')`)
		require.NoError(t, err)
		return db
	}
	batch := func(n int) []preparedUpsertDoc {
		docs := make([]preparedUpsertDoc, n)
		for i := range docs {
			docs[i] = preparedUpsertDoc{
				Doc:       kb.Document{ID: fmt.Sprintf("doc-%d", i), Text: "replacement"},
				Embedding: []float32{1, 0},
			}
		}
		return docs
	}

	t.Run("multi_chunk", func(t *testing.T) {
		db := open(t)
		docs := batch(4097) // Cross two native 2048-row chunk boundaries.
		require.NoError(t, applyPreparedDocsAppender(t.Context(), db, docs))
		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM docs WHERE content = 'replacement'`).Scan(&count))
		require.Equal(t, len(docs), count)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM doc_tombstones`).Scan(&count))
		require.Zero(t, count)
	})

	t.Run("row_error", func(t *testing.T) {
		db := open(t)
		docs := batch(4097)
		docs[len(docs)-1].Embedding = []float32{1, 0, 0}
		require.Error(t, applyPreparedDocsAppender(t.Context(), db, docs))
		var text string
		require.NoError(t, db.QueryRow(`SELECT content FROM docs`).Scan(&text))
		require.Equal(t, "original", text, "a failed batch must not replace existing rows")
		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM docs`).Scan(&count))
		require.Equal(t, 1, count, "a failed batch must not expose partial inserts")
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM doc_tombstones`).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE '_minnow_docs_upsert_%'`).Scan(&count))
		require.Zero(t, count, "partial staging must be removed")

		docs[len(docs)-1].Embedding = []float32{1, 0}
		require.NoError(t, applyPreparedDocsAppender(t.Context(), db, docs))
		require.NoError(t, db.QueryRow(`SELECT count(DISTINCT id) FROM docs`).Scan(&count))
		require.Equal(t, len(docs), count, "retry must reconstruct the entire batch")
	})

	t.Run("close_error", func(t *testing.T) {
		db := open(t)
		_, err := db.Exec(`CREATE TABLE rejected_batch (
			id TEXT CHECK (id != 'doc-1'), content TEXT, embedding FLOAT[2], media_refs TEXT, metadata TEXT)`)
		require.NoError(t, err)
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		err = appendPreparedDocs(t.Context(), conn, "rejected_batch", batch(2))
		require.ErrorContains(t, err, "close docs appender", "final flush errors must reach the caller")
		require.NoError(t, conn.Close())
		var text string
		require.NoError(t, db.QueryRow(`SELECT content FROM docs`).Scan(&text))
		require.Equal(t, "original", text)
		require.NoError(t, applyPreparedDocsAppender(t.Context(), db, batch(2)))
		var count int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM docs`).Scan(&count))
		require.Equal(t, 2, count, "a fresh batch must succeed after a failed flush")
	})
}
