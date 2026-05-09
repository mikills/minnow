package duckdb

import (
	"database/sql"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/mediarefs"
)

func encodeMediaRefs(mediaIDs []string, explicit []kb.ChunkMediaRef) (sql.NullString, error) {
	return mediarefs.Encode(mediaIDs, explicit)
}

func decodeMediaRefs(raw sql.NullString) ([]kb.ChunkMediaRef, error) {
	return mediarefs.Decode(raw)
}
