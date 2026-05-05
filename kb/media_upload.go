package kb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// UploadMedia stores media bytes in BlobStore and persists metadata in
// MediaStore. Returns the resulting metadata.
//
// Server-owned blob key, filename sanitised, sha256 checksum computed during
// streaming. Callers should not provide BlobKey themselves.
func (l *KB) UploadMedia(ctx context.Context, input MediaUploadInput, maxBytes int64) (*MediaUploadResult, error) {
	if err := validateUploadMediaEnv(l, input); err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	if existing, err := lookupExistingUpload(ctx, l, input.KBID, input.IdempotencyKey); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}

	cleanName, ct, err := prepareUploadNames(l, input)
	if err != nil {
		return nil, err
	}

	tmp, err := stageUploadBody(input.Body, maxBytes)
	if err != nil {
		return nil, err
	}
	defer tmp.cleanup()

	mediaID := l.newMediaID()
	blobKey := MediaBlobKey(input.KBID, mediaID, cleanName)
	return persistUploadedMedia(mediaPersistInput{ctx: ctx, kb: l, input: input, cleanName: cleanName, contentType: ct, mediaID: mediaID, blobKey: blobKey, tmp: tmp})
}

// validateUploadMediaEnv enforces that the KB has MediaStore + BlobStore
// configured and that the caller provided the minimum required input.
func validateUploadMediaEnv(l *KB, input MediaUploadInput) error {
	if l.MediaStore == nil {
		return errors.New("media: MediaStore not configured")
	}
	if l.BlobStore == nil {
		return errors.New("media: BlobStore not configured")
	}
	if strings.TrimSpace(input.KBID) == "" {
		return errors.New("media: kb_id required")
	}
	if input.Body == nil {
		return errors.New("media: body required")
	}
	return nil
}

// lookupExistingUpload returns a non-nil result if a prior upload with the
// same idempotency key already completed for this KB.
func lookupExistingUpload(ctx context.Context, l *KB, kbID, idempotencyKey string) (*MediaUploadResult, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	existing, err := l.MediaStore.FindByIdempotency(ctx, kbID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	return mediaResultFromObject(existing), nil
}

// prepareUploadNames sanitises the requested filename and normalises /
// validates the content type against the caller and/or KB allowlist.
func prepareUploadNames(l *KB, input MediaUploadInput) (string, string, error) {
	cleanName, err := SanitizeMediaFilename(input.Filename)
	if err != nil {
		return "", "", err
	}
	ct := normaliseContentType(input.ContentType)
	allowList := input.AllowedContentTypes
	if len(allowList) == 0 {
		allowList = l.MediaContentTypeAllowlist
	}
	if len(allowList) > 0 && !isContentTypeAllowed(ct, allowList) {
		return "", "", fmt.Errorf("media: content type %q not allowed", ct)
	}
	return cleanName, ct, nil
}

// stageUploadBody streams the body to a temp file (computing size + sha256)
// and rejects empty uploads.
func stageUploadBody(body io.Reader, maxBytes int64) (*tempUpload, error) {
	tmp, err := writeTempCapped(body, maxBytes)
	if err != nil {
		return nil, err
	}
	if tmp.size == 0 {
		tmp.cleanup()
		return nil, errors.New("media: empty upload rejected")
	}
	return tmp, nil
}

// persistUploadedMedia uploads the staged bytes to BlobStore and inserts the
// metadata record, cleaning up blob bytes on metadata failures and
// collapsing to the prior record on idempotency-key races.
type mediaPersistInput struct {
	ctx         context.Context
	kb          *KB
	input       MediaUploadInput
	cleanName   string
	contentType string
	mediaID     string
	blobKey     string
	tmp         *tempUpload
}

func persistUploadedMedia(p mediaPersistInput) (*MediaUploadResult, error) {
	if _, err := p.kb.BlobStore.UploadIfMatch(p.ctx, p.blobKey, p.tmp.path, ""); err != nil {
		return nil, fmt.Errorf("media: blob upload: %w", err)
	}

	now := p.kb.Clock.Now()
	rec := MediaObject{
		ID:              p.mediaID,
		KBID:            p.input.KBID,
		Filename:        p.cleanName,
		ContentType:     p.contentType,
		SizeBytes:       p.tmp.size,
		BlobKey:         p.blobKey,
		Checksum:        p.tmp.sha256,
		Source:          p.input.Source,
		Title:           p.input.Title,
		Tags:            p.input.Tags,
		CreatedAtUnixMs: now.UnixMilli(),
		UploadedBy:      p.input.UploadedBy,
		Metadata:        p.input.Metadata,
		State:           MediaStatePending,
		IdempotencyKey:  p.input.IdempotencyKey,
	}
	if err := p.kb.MediaStore.Put(p.ctx, rec); err != nil {
		if errors.Is(err, ErrMediaDuplicateKey) && p.input.IdempotencyKey != "" {
			bestEffortDeleteStagedBlob(p.ctx, p.kb.BlobStore, p.blobKey, "duplicate media idempotency key")
			existing, findErr := p.kb.MediaStore.FindByIdempotency(p.ctx, p.input.KBID, p.input.IdempotencyKey)
			if findErr != nil {
				return nil, findErr
			}
			if existing != nil {
				return mediaResultFromObject(existing), nil
			}
		}
		// Best-effort blob cleanup. Metadata insert failure should not leave
		// orphan bytes.
		bestEffortDeleteStagedBlob(p.ctx, p.kb.BlobStore, p.blobKey, "media metadata insert failed")
		return nil, fmt.Errorf("media: persist metadata: %w", err)
	}

	return mediaResultFromObject(&rec), nil
}

func mediaResultFromObject(m *MediaObject) *MediaUploadResult {
	if m == nil {
		return nil
	}
	return &MediaUploadResult{
		MediaID:     m.ID,
		BlobKey:     m.BlobKey,
		Filename:    m.Filename,
		ContentType: m.ContentType,
		SizeBytes:   m.SizeBytes,
		Checksum:    m.Checksum,
	}
}

// ValidateDocumentReferences rejects client-controlled blob keys and unknown
// or cross-KB media ids before they are persisted.
func (l *KB) ValidateDocumentReferences(ctx context.Context, kbID string, docs []Document) error {
	ids, err := referencedMediaIDs(docs)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if l.MediaStore == nil {
		return errors.New("media: MediaStore not configured")
	}
	for id := range ids {
		if err := l.validateMediaReference(ctx, kbID, id); err != nil {
			return err
		}
	}
	return nil
}

func referencedMediaIDs(docs []Document) (map[string]struct{}, error) {
	ids := make(map[string]struct{})
	for _, doc := range docs {
		if err := addMediaIDs(ids, doc.MediaIDs); err != nil {
			return nil, err
		}
		if err := addMediaRefs(ids, doc.MediaRefs); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func addMediaIDs(ids map[string]struct{}, mediaIDs []string) error {
	for _, id := range mediaIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			return errors.New("media: media_id required")
		}
		ids[id] = struct{}{}
	}
	return nil
}

func addMediaRefs(ids map[string]struct{}, refs []ChunkMediaRef) error {
	for _, ref := range refs {
		if strings.TrimSpace(ref.BlobKey) != "" {
			return errors.New("media: blob_key must not be provided")
		}
		id := strings.TrimSpace(ref.MediaID)
		if id == "" {
			return errors.New("media: media_id required")
		}
		ids[id] = struct{}{}
	}
	return nil
}

func (l *KB) validateMediaReference(ctx context.Context, kbID string, id string) error {
	m, err := l.MediaStore.Get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrMediaNotFound) {
			return fmt.Errorf("media: unknown media_id %q", id)
		}
		return err
	}
	if m.KBID != kbID {
		return fmt.Errorf("media: media_id %q belongs to kb %q", id, m.KBID)
	}
	if m.State == MediaStateTombstoned {
		return fmt.Errorf("media: media_id %q is tombstoned", id)
	}
	return nil
}

func normaliseContentType(ct string) string {
	ct = strings.TrimSpace(strings.ToLower(ct))
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

// isContentTypeAllowed matches ct (case-insensitive) against allowed. The
// comparison is on the media-type portion before any '.' parameter (so
// "image/png. charset=..." matches "image/png"). A trailing "*" on an
// entry acts as a prefix wildcard ("image/*" matches "image/png").
func isContentTypeAllowed(ct string, allowed []string) bool {
	if idx := strings.Index(ct, ";"); idx >= 0 {
		ct = strings.TrimSpace(ct[:idx])
	}
	ct = strings.ToLower(ct)
	for _, a := range allowed {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == "" {
			continue
		}
		if strings.HasSuffix(a, "*") {
			if strings.HasPrefix(ct, strings.TrimSuffix(a, "*")) {
				return true
			}
			continue
		}
		if ct == a {
			return true
		}
	}
	return false
}
