package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"
	"time"
)

// MediaUploadWorkerVisibilityTimeout is the recommended claim window for
// MediaUploadWorker. Exposed as a tuneable constant so operators can raise
// it for slow blob backends.
const MediaUploadWorkerVisibilityTimeout = 5 * time.Minute

type MediaUploadPayload struct {
	KBID           string         `json:"kb_id"`
	MediaID        string         `json:"media_id"`
	StagedBlobKey  string         `json:"staged_blob_key"`
	Filename       string         `json:"filename"`
	ContentType    string         `json:"content_type"`
	SizeBytes      int64          `json:"size_bytes"`
	Checksum       string         `json:"checksum"`
	Source         string         `json:"source,omitempty"`
	Title          string         `json:"title,omitempty"`
	Tags           []string       `json:"tags,omitempty"`
	UploadedBy     string         `json:"uploaded_by,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	SourceEventID  string         `json:"source_event_id,omitempty"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

type MediaUploadedPayload struct {
	KBID          string `json:"kb_id"`
	MediaID       string `json:"media_id"`
	Filename      string `json:"filename"`
	ContentType   string `json:"content_type"`
	SizeBytes     int64  `json:"size_bytes"`
	Checksum      string `json:"checksum"`
	SourceEventID string `json:"source_event_id,omitempty"`
}

func mediaStagingBlobKey(kbID, eventID, filename string) string {
	return path.Join("kbs", kbID, "media-staging", eventID, filename)
}

func (l *KB) AppendMediaUploadDetailed(ctx context.Context, input MediaUploadInput, maxBytes int64, idempotencyKey, correlationID string) (string, string, error) {
	cleanName, contentType, maxBytes, err := l.validateMediaUploadInput(input, maxBytes)
	if err != nil {
		return "", "", err
	}
	idempotencyKey, correlationID = l.ensureMediaUploadKeys(idempotencyKey, correlationID)
	staged, err := l.stageMediaUpload(ctx, input, cleanName, contentType, maxBytes)
	if err != nil {
		return "", "", err
	}
	defer staged.cleanup()
	event, err := l.buildMediaUploadEvent(input, staged, idempotencyKey, correlationID)
	if err != nil {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, staged.blobKey, "marshal payload failed")
		return "", "", err
	}
	return l.appendMediaUploadEvent(ctx, event, input.KBID, idempotencyKey, staged.blobKey)
}

type stagedMediaUpload struct {
	eventID     string
	mediaID     string
	blobKey     string
	filename    string
	contentType string
	tmp         *tempUpload
}

func (l *KB) validateMediaUploadInput(input MediaUploadInput, maxBytes int64) (string, string, int64, error) {
	if l.EventStore == nil {
		return "", "", 0, errors.New("media: EventStore not configured")
	}
	if l.BlobStore == nil {
		return "", "", 0, errors.New("media: BlobStore not configured")
	}
	if strings.TrimSpace(input.KBID) == "" {
		return "", "", 0, errors.New("media: kb_id required")
	}
	if input.Body == nil {
		return "", "", 0, errors.New("media: body required")
	}
	cleanName, err := SanitizeMediaFilename(input.Filename)
	if err != nil {
		return "", "", 0, err
	}
	contentType := normaliseContentType(input.ContentType)
	if err := l.validateMediaContentType(contentType, input.AllowedContentTypes); err != nil {
		return "", "", 0, err
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	return cleanName, contentType, maxBytes, nil
}

func (l *KB) validateMediaContentType(contentType string, allowList []string) error {
	if len(allowList) == 0 {
		allowList = l.MediaContentTypeAllowlist
	}
	if len(allowList) > 0 && !isContentTypeAllowed(contentType, allowList) {
		return fmt.Errorf("media: content type %q not allowed", contentType)
	}
	return nil
}

func (l *KB) ensureMediaUploadKeys(idempotencyKey, correlationID string) (string, string) {
	if idempotencyKey == "" {
		idempotencyKey = l.NewULIDLike("idem")
	}
	if correlationID == "" {
		correlationID = l.NewULIDLike("corr")
	}
	return idempotencyKey, correlationID
}

func (l *KB) stageMediaUpload(ctx context.Context, input MediaUploadInput, cleanName string, contentType string, maxBytes int64) (stagedMediaUpload, error) {
	tmp, err := writeTempCapped(input.Body, maxBytes)
	if err != nil {
		return stagedMediaUpload{}, err
	}
	if tmp.size == 0 {
		tmp.cleanup()
		return stagedMediaUpload{}, errors.New("media: empty upload rejected")
	}
	eventID := l.NewULIDLike("evt")
	staged := stagedMediaUpload{eventID: eventID, mediaID: l.newMediaID(), blobKey: mediaStagingBlobKey(input.KBID, eventID, cleanName), filename: cleanName, contentType: contentType, tmp: tmp}
	if _, err := l.BlobStore.UploadIfMatch(ctx, staged.blobKey, tmp.path, ""); err != nil {
		tmp.cleanup()
		return stagedMediaUpload{}, fmt.Errorf("media: stage upload: %w", err)
	}
	return staged, nil
}

func (s stagedMediaUpload) cleanup() { s.tmp.cleanup() }

func (l *KB) buildMediaUploadEvent(input MediaUploadInput, staged stagedMediaUpload, idempotencyKey, correlationID string) (KBEvent, error) {
	payloadBytes, err := json.Marshal(MediaUploadPayload{KBID: input.KBID, MediaID: staged.mediaID, StagedBlobKey: staged.blobKey, Filename: staged.filename, ContentType: staged.contentType, SizeBytes: staged.tmp.size, Checksum: staged.tmp.sha256, Source: input.Source, Title: input.Title, Tags: input.Tags, UploadedBy: input.UploadedBy, Metadata: input.Metadata, SourceEventID: staged.eventID, CorrelationID: correlationID, IdempotencyKey: idempotencyKey})
	if err != nil {
		return KBEvent{}, fmt.Errorf("media: marshal payload: %w", err)
	}
	return l.newRootPendingEvent(pendingEventInput{kind: EventMediaUpload, kbID: input.KBID, schema: "media.upload/v1", correlationID: correlationID, idempotencyKey: idempotencyKey, payload: payloadBytes}), nil
}

func (l *KB) appendMediaUploadEvent(ctx context.Context, event KBEvent, kbID string, idempotencyKey string, stagedBlobKey string) (string, string, error) {
	if err := l.EventStore.Append(ctx, event); err != nil {
		if errors.Is(err, ErrEventDuplicateKey) {
			return l.handleDuplicateMediaUpload(ctx, kbID, idempotencyKey, stagedBlobKey)
		}
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "event append failed")
		return "", "", err
	}
	return event.EventID, idempotencyKey, nil
}

func (l *KB) handleDuplicateMediaUpload(ctx context.Context, kbID string, idempotencyKey string, stagedBlobKey string) (string, string, error) {
	existingID, ok, err := l.findDuplicateMediaUpload(ctx, kbID, idempotencyKey)
	if err != nil {
		return "", "", err
	}
	if ok {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "duplicate idempotency key")
		return existingID, idempotencyKey, nil
	}
	bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "event append failed")
	return "", "", ErrEventDuplicateKey
}

func (l *KB) findDuplicateMediaUpload(ctx context.Context, kbID string, idempotencyKey string) (string, bool, error) {
	existing, err := l.EventStore.FindByIdempotency(ctx, EventMediaUpload, kbID, idempotencyKey)
	if err != nil {
		return "", false, err
	}
	if existing == nil {
		return "", false, nil
	}
	return existing.EventID, true, nil
}

// bestEffortDeleteStagedBlob removes a staged blob, treating NotFound as
// success and logging any other error. Cascade-cleanup paths use this so a
// failed delete does not mask the primary error, but operators still see the
// potential leak in logs.
func bestEffortDeleteStagedBlob(ctx context.Context, store BlobStore, blobKey, reason string) {
	if store == nil || blobKey == "" {
		return
	}
	err := store.Delete(ctx, blobKey)
	if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
		return
	}
	slog.Default().Warn("media: staged blob cleanup failed",
		"blob_key", blobKey, "reason", reason, "error", err)
}

type MediaUploadWorker struct {
	KB *KB
	ID string
}

func (w *MediaUploadWorker) Kind() EventKind  { return EventMediaUpload }
func (w *MediaUploadWorker) WorkerID() string { return w.ID }

func (w *MediaUploadWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	if w.KB.MediaStore == nil || w.KB.BlobStore == nil {
		return WorkerResult{}, errors.New("media worker: media subsystem not configured")
	}
	var payload MediaUploadPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	if err := w.ensureMediaObject(ctx, payload); err != nil {
		return WorkerResult{}, err
	}
	mediaUploadedPayload, err := json.Marshal(MediaUploadedPayload{KBID: payload.KBID, MediaID: payload.MediaID, Filename: payload.Filename, ContentType: payload.ContentType, SizeBytes: payload.SizeBytes, Checksum: payload.Checksum, SourceEventID: event.EventID})
	if err != nil {
		return WorkerResult{}, fmt.Errorf("marshal media.uploaded: %w", err)
	}
	child := w.KB.newChildPendingEvent(event, EventMediaUploaded, "media.uploaded/v1", event.EventID+"|media.uploaded", mediaUploadedPayload)
	return WorkerResult{FollowUps: []KBEvent{child}, Commit: mediaUploadCommit(w.KB.BlobStore, payload.StagedBlobKey)}, nil
}

func (w *MediaUploadWorker) ensureMediaObject(ctx context.Context, payload MediaUploadPayload) error {
	if _, err := w.KB.MediaStore.Get(ctx, payload.MediaID); err != nil {
		if !errors.Is(err, ErrMediaNotFound) {
			return err
		}
		return w.KB.persistStagedMediaObject(ctx, payload.StagedBlobKey, mediaObjectFromUploadPayload(payload))
	}
	return nil
}

func mediaObjectFromUploadPayload(payload MediaUploadPayload) MediaObject {
	return MediaObject{ID: payload.MediaID, KBID: payload.KBID, Filename: payload.Filename, ContentType: payload.ContentType, SizeBytes: payload.SizeBytes, Checksum: payload.Checksum, Source: payload.Source, Title: payload.Title, Tags: payload.Tags, UploadedBy: payload.UploadedBy, Metadata: payload.Metadata, State: MediaStatePending, IdempotencyKey: payload.IdempotencyKey}
}

func mediaUploadCommit(store BlobStore, stagedBlobKey string) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := store.Delete(ctx, stagedBlobKey); err != nil && !errors.Is(err, ErrBlobNotFound) && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("media: delete staged blob %q: %w", stagedBlobKey, err)
		}
		return nil
	}
}

// mediaSweepPageSize bounds each ListUnfinishedBefore call so the sweep
// does not materialise a large backlog in memory.
const mediaSweepPageSize = 500

func (l *KB) SweepAbortedMediaUploads(ctx context.Context, now time.Time) (int, error) {
	if l.EventStore == nil || l.BlobStore == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = l.Clock.Now()
	}
	cutoff := now.Add(-l.MediaGCConfigOrDefault().UploadCompletion)
	count := 0
	var lastErr error
	token := ""
	for {
		events, next, err := l.EventStore.ListUnfinishedBefore(ctx, EventMediaUpload, cutoff, token, mediaSweepPageSize)
		if err != nil {
			return count, err
		}
		for _, event := range events {
			marked, err := l.sweepAbortedMediaUpload(ctx, event)
			if err != nil {
				lastErr = err
			}
			if marked {
				count++
			}
		}
		if next == "" {
			return count, lastErr
		}
		token = next
	}
}

func (l *KB) sweepAbortedMediaUpload(ctx context.Context, event KBEvent) (bool, error) {
	child, err := l.EventStore.FindByCausation(ctx, EventMediaUploaded, event.EventID)
	if err != nil {
		slog.Default().Warn("media-sweep: FindByCausation failed", "event_id", event.EventID, "error", err)
		return false, err
	}
	if child != nil {
		return false, nil
	}
	l.cleanupAbortedMediaUploadBlob(ctx, event)
	if err := l.EventStore.Fail(ctx, event.EventID, event.Attempt, "media upload completion timeout"); err != nil {
		return false, mediaSweepFailError(event.EventID, err)
	}
	return true, nil
}

func (l *KB) cleanupAbortedMediaUploadBlob(ctx context.Context, event KBEvent) {
	var payload MediaUploadPayload
	if err := json.Unmarshal(event.Payload, &payload); err == nil {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, payload.StagedBlobKey, "sweep aborted upload")
	} else {
		slog.Default().Warn("media-sweep: decode payload failed", "event_id", event.EventID, "error", err)
	}
}

func mediaSweepFailError(eventID string, err error) error {
	if errors.Is(err, ErrEventStateChanged) {
		slog.Default().Info("media-sweep: event state changed since observation; another worker updated it", "event_id", eventID)
		return nil
	}
	slog.Default().Warn("media-sweep: EventStore.Fail failed", "event_id", eventID, "error", err)
	return err
}
