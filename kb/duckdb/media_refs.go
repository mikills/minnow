package duckdb

import (
	"database/sql"
	"encoding/json"

	kb "github.com/mikills/minnow/kb"
)

// encodeMediaRefs serialises the doc's media refs. Prefers explicit
// MediaRefs (rich: role/label/locator) when provided; otherwise synthesises
// minimal {MediaID} entries from MediaIDs. Returns a null value when there
// are no refs.
func encodeMediaRefs(mediaIDs []string, explicit []kb.ChunkMediaRef) (sql.NullString, error) {
	var refs []kb.ChunkMediaRef
	if len(explicit) > 0 {
		refs = make([]kb.ChunkMediaRef, 0, len(explicit))
		for _, ref := range explicit {
			ref.BlobKey = ""
			refs = append(refs, ref)
		}
	} else {
		refs = make([]kb.ChunkMediaRef, 0, len(mediaIDs))
		for _, id := range mediaIDs {
			if id == "" {
				continue
			}
			refs = append(refs, kb.ChunkMediaRef{MediaID: id})
		}
	}
	if len(refs) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(refs)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

// decodeMediaRefs parses the docs.media_refs column value. A null/empty
// column becomes nil (search hits omit the field).
func decodeMediaRefs(raw sql.NullString) ([]kb.ChunkMediaRef, error) {
	if !raw.Valid || raw.String == "" {
		return nil, nil
	}
	var refs []kb.ChunkMediaRef
	if err := json.Unmarshal([]byte(raw.String), &refs); err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}
	return refs, nil
}
