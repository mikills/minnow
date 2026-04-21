package kb

import (
	"context"
	"errors"
	"strings"
	"sync"
)

// InMemoryMediaStore is the in-memory MediaStore implementation.
type InMemoryMediaStore struct {
	mu      sync.Mutex
	records map[string]MediaObject
	idem    map[string]string
}

func NewInMemoryMediaStore() *InMemoryMediaStore {
	return &InMemoryMediaStore{records: make(map[string]MediaObject), idem: make(map[string]string)}
}

func (s *InMemoryMediaStore) Put(_ context.Context, m MediaObject) error {
	if m.ID == "" {
		return errors.New("media: id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.IdempotencyKey != "" {
		key := m.KBID + "|" + m.IdempotencyKey
		if existing, ok := s.idem[key]; ok && existing != m.ID {
			return ErrMediaDuplicateKey
		}
		s.idem[key] = m.ID
	}
	s.records[m.ID] = m
	return nil
}

func (s *InMemoryMediaStore) Get(_ context.Context, mediaID string) (*MediaObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[mediaID]
	if !ok {
		return nil, ErrMediaNotFound
	}
	return &rec, nil
}

func (s *InMemoryMediaStore) FindByIdempotency(_ context.Context, kbID, idempotencyKey string) (*MediaObject, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.idem[kbID+"|"+idempotencyKey]
	if !ok {
		return nil, nil
	}
	rec, ok := s.records[id]
	if !ok {
		return nil, nil
	}
	return &rec, nil
}

func (s *InMemoryMediaStore) List(_ context.Context, kbID, prefix string, after string, limit int) (MediaPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	matches := make([]MediaObject, 0)
	for _, rec := range s.records {
		if rec.KBID != kbID {
			continue
		}
		if prefix != "" && !strings.HasPrefix(rec.Filename, prefix) {
			continue
		}
		matches = append(matches, rec)
	}
	return mediaSliceToPage(matches, after, limit), nil
}

func (s *InMemoryMediaStore) UpdateState(_ context.Context, mediaID string, state MediaState, tombstonedAtMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[mediaID]
	if !ok {
		return ErrMediaNotFound
	}
	rec.State = state
	if state == MediaStateTombstoned {
		rec.TombstonedAtMs = tombstonedAtMs
	}
	s.records[mediaID] = rec
	return nil
}

func (s *InMemoryMediaStore) ListByState(_ context.Context, kbID string, state MediaState, after string, limit int) (MediaPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	matches := make([]MediaObject, 0)
	for _, rec := range s.records {
		if rec.State != state {
			continue
		}
		if kbID != "" && rec.KBID != kbID {
			continue
		}
		matches = append(matches, rec)
	}
	return mediaSliceToPage(matches, after, limit), nil
}

func (s *InMemoryMediaStore) Delete(_ context.Context, mediaID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.records[mediaID]; ok && rec.IdempotencyKey != "" {
		delete(s.idem, rec.KBID+"|"+rec.IdempotencyKey)
	}
	delete(s.records, mediaID)
	return nil
}

// mediaSliceToPage sorts matches by ID and returns a page after the given
// resume ID, using the shared MediaPage token encoding. after is the raw
// page token produced by a prior call (or "" for the first page).
func mediaSliceToPage(matches []MediaObject, after string, limit int) MediaPage {
	limit = clampMediaLimit(limit)
	sortMediaByID(matches)
	start := 0
	if after != "" {
		lastID, err := decodeMediaPageToken(after)
		if err == nil && lastID != "" {
			for idx, m := range matches {
				if m.ID == lastID {
					start = idx + 1
					break
				}
			}
		}
	}
	end := start + limit
	if end > len(matches) {
		end = len(matches)
	}
	items := append([]MediaObject(nil), matches[start:end]...)
	page := MediaPage{Items: items}
	if end < len(matches) && len(items) > 0 {
		page.NextToken = encodeMediaPageToken(items[len(items)-1].ID)
	}
	return page
}

func sortMediaByID(out []MediaObject) {
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].ID > out[j].ID; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
}
