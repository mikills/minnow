package media

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

type BlobStore interface {
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*blobstore.ObjectInfo, error)
	Delete(ctx context.Context, key string) error
}

type UploadConfig struct {
	BlobStore               BlobStore
	MediaStore              MediaStore
	Now                     func() time.Time
	NewMediaID              func() string
	ContentTypeAllowlist    []string
	DuplicateKeyErr         error
	DeleteBlobOnFailureNote func(ctx context.Context, store BlobStore, key string, reason string)
}

type uploadPersistInput struct {
	ctx         context.Context
	config      UploadConfig
	input       MediaUploadInput
	cleanName   string
	contentType string
	mediaID     string
	blobKey     string
	tmp         *TempUpload
}

func Upload(
	ctx context.Context,
	input MediaUploadInput,
	maxBytes int64,
	config UploadConfig,
) (*MediaUploadResult, error) {
	if err := validateUploadEnv(config, input); err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	if existing, err := LookupExistingUpload(ctx, config.MediaStore, input.KBID, input.IdempotencyKey); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	cleanName, ct, err := PrepareUploadNames(input, config.ContentTypeAllowlist)
	if err != nil {
		return nil, err
	}
	tmp, err := StageUploadBody(input.Body, maxBytes)
	if err != nil {
		return nil, err
	}
	defer tmp.Cleanup()
	mediaID := config.NewMediaID()
	blobKey := MediaBlobKey(input.KBID, mediaID, cleanName)
	return persistUploadedMedia(
		uploadPersistInput{
			ctx:         ctx,
			config:      config,
			input:       input,
			cleanName:   cleanName,
			contentType: ct,
			mediaID:     mediaID,
			blobKey:     blobKey,
			tmp:         tmp,
		},
	)
}

func validateUploadEnv(config UploadConfig, input MediaUploadInput) error {
	if config.MediaStore == nil {
		return errors.New("media: MediaStore not configured")
	}
	if config.BlobStore == nil {
		return errors.New("media: BlobStore not configured")
	}
	if config.NewMediaID == nil {
		return errors.New("media: media id generator not configured")
	}
	if strings.TrimSpace(input.KBID) == "" {
		return errors.New("media: kb_id required")
	}
	if input.Body == nil {
		return errors.New("media: body required")
	}
	return nil
}

func LookupExistingUpload(
	ctx context.Context,
	store MediaStore,
	kbID, idempotencyKey string,
) (*MediaUploadResult, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	existing, err := store.FindByIdempotency(ctx, kbID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, nil
	}
	return ResultFromObject(existing), nil
}

func PrepareUploadNames(input MediaUploadInput, defaultAllowlist []string) (string, string, error) {
	cleanName, err := SanitizeMediaFilename(input.Filename)
	if err != nil {
		return "", "", err
	}
	ct := NormaliseContentType(input.ContentType)
	allowList := input.AllowedContentTypes
	if len(allowList) == 0 {
		allowList = defaultAllowlist
	}
	if len(allowList) > 0 && !ContentTypeAllowed(ct, allowList) {
		return "", "", fmt.Errorf("media: content type %q not allowed", ct)
	}
	return cleanName, ct, nil
}

func StageUploadBody(body io.Reader, maxBytes int64) (*TempUpload, error) {
	tmp, err := WriteTempCapped(body, maxBytes)
	if err != nil {
		return nil, err
	}
	if tmp.Size() == 0 {
		tmp.Cleanup()
		return nil, errors.New("media: empty upload rejected")
	}
	return tmp, nil
}

func persistUploadedMedia(p uploadPersistInput) (*MediaUploadResult, error) {
	if _, err := p.config.BlobStore.UploadIfMatch(p.ctx, p.blobKey, p.tmp.Path(), ""); err != nil {
		return nil, fmt.Errorf("media: blob upload: %w", err)
	}
	now := time.Now().UTC()
	if p.config.Now != nil {
		now = p.config.Now()
	}
	rec := MediaObject{
		ID:              p.mediaID,
		KBID:            p.input.KBID,
		Filename:        p.cleanName,
		ContentType:     p.contentType,
		SizeBytes:       p.tmp.Size(),
		BlobKey:         p.blobKey,
		Checksum:        p.tmp.SHA256(),
		Source:          p.input.Source,
		Title:           p.input.Title,
		Tags:            p.input.Tags,
		CreatedAtUnixMs: now.UnixMilli(),
		UploadedBy:      p.input.UploadedBy,
		Metadata:        p.input.Metadata,
		State:           MediaStatePending,
		IdempotencyKey:  p.input.IdempotencyKey,
	}
	if err := p.config.MediaStore.Put(p.ctx, rec); err != nil {
		if errors.Is(err, p.config.DuplicateKeyErr) && p.input.IdempotencyKey != "" {
			deleteUploadBlob(p, "duplicate media idempotency key")
			existing, findErr := p.config.MediaStore.FindByIdempotency(p.ctx, p.input.KBID, p.input.IdempotencyKey)
			if findErr != nil {
				return nil, findErr
			}
			if existing != nil {
				return ResultFromObject(existing), nil
			}
		}
		deleteUploadBlob(p, "media metadata insert failed")
		return nil, fmt.Errorf("media: persist metadata: %w", err)
	}
	return ResultFromObject(&rec), nil
}

func deleteUploadBlob(p uploadPersistInput, reason string) {
	if p.config.DeleteBlobOnFailureNote != nil {
		p.config.DeleteBlobOnFailureNote(p.ctx, p.config.BlobStore, p.blobKey, reason)
		return
	}
	if err := p.config.BlobStore.Delete(p.ctx, p.blobKey); err != nil {
		slog.Default().Warn("media upload blob cleanup failed", "key", p.blobKey, "reason", reason, "error", err)
	}
}

func ResultFromObject(m *MediaObject) *MediaUploadResult {
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

func NormaliseContentType(ct string) string {
	ct = strings.TrimSpace(strings.ToLower(ct))
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

func ContentTypeAllowed(ct string, allowed []string) bool {
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

type DocumentRefs struct {
	MediaIDs  []string
	MediaRefs []ChunkMediaRef
}

func ValidateDocumentReferences(ctx context.Context, store MediaStore, kbID string, docs []DocumentRefs) error {
	ids, err := ReferencedMediaIDs(docs)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if store == nil {
		return errors.New("media: MediaStore not configured")
	}
	for id := range ids {
		if err := ValidateReference(ctx, store, kbID, id); err != nil {
			return err
		}
	}
	return nil
}

func ReferencedMediaIDs(docs []DocumentRefs) (map[string]struct{}, error) {
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

func ValidateReference(ctx context.Context, store MediaStore, kbID string, id string) error {
	m, err := store.Get(ctx, id)
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
