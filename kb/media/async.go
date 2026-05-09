package media

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

const UploadWorkerVisibilityTimeout = 5 * time.Minute

type UploadPayload struct {
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

type UploadedPayload struct {
	KBID          string `json:"kb_id"`
	MediaID       string `json:"media_id"`
	Filename      string `json:"filename"`
	ContentType   string `json:"content_type"`
	SizeBytes     int64  `json:"size_bytes"`
	Checksum      string `json:"checksum"`
	SourceEventID string `json:"source_event_id,omitempty"`
}

type AsyncValidationConfig struct {
	HasEventStore        bool
	HasBlobStore         bool
	ContentTypeAllowlist []string
}

type StagedAsyncUpload struct {
	EventID     string
	MediaID     string
	BlobKey     string
	Filename    string
	ContentType string
	Temp        *TempUpload
}

func StagingBlobKey(kbID, eventID, filename string) string {
	return path.Join("kbs", kbID, "media-staging", eventID, filename)
}

func ValidateAsyncUploadInput(
	input MediaUploadInput,
	maxBytes int64,
	config AsyncValidationConfig,
) (string, string, int64, error) {
	if !config.HasEventStore {
		return "", "", 0, errors.New("media: EventStore not configured")
	}
	if !config.HasBlobStore {
		return "", "", 0, errors.New("media: BlobStore not configured")
	}
	if strings.TrimSpace(input.KBID) == "" {
		return "", "", 0, errors.New("media: kb_id required")
	}
	if input.Body == nil {
		return "", "", 0, errors.New("media: body required")
	}
	cleanName, contentType, err := PrepareUploadNames(input, config.ContentTypeAllowlist)
	if err != nil {
		return "", "", 0, err
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	return cleanName, contentType, maxBytes, nil
}

type StageAsyncConfig struct {
	BlobStore   BlobStore
	NewEventID  func() string
	NewMediaID  func() string
	CleanName   string
	ContentType string
	MaxBytes    int64
}

func StageAsyncUpload(ctx context.Context, input MediaUploadInput, config StageAsyncConfig) (StagedAsyncUpload, error) {
	tmp, err := WriteTempCapped(input.Body, config.MaxBytes)
	if err != nil {
		return StagedAsyncUpload{}, err
	}
	if tmp.Size() == 0 {
		tmp.Cleanup()
		return StagedAsyncUpload{}, errors.New("media: empty upload rejected")
	}
	eventID := config.NewEventID()
	staged := StagedAsyncUpload{
		EventID:     eventID,
		MediaID:     config.NewMediaID(),
		BlobKey:     StagingBlobKey(input.KBID, eventID, config.CleanName),
		Filename:    config.CleanName,
		ContentType: config.ContentType,
		Temp:        tmp,
	}
	if _, err := config.BlobStore.UploadIfMatch(ctx, staged.BlobKey, tmp.Path(), ""); err != nil {
		tmp.Cleanup()
		return StagedAsyncUpload{}, fmt.Errorf("media: stage upload: %w", err)
	}
	return staged, nil
}

func (s StagedAsyncUpload) Cleanup() {
	if s.Temp != nil {
		s.Temp.Cleanup()
	}
}

func UploadPayloadFromInput(
	input MediaUploadInput,
	staged StagedAsyncUpload,
	idempotencyKey, correlationID string,
) UploadPayload {
	return UploadPayload{
		KBID:           input.KBID,
		MediaID:        staged.MediaID,
		StagedBlobKey:  staged.BlobKey,
		Filename:       staged.Filename,
		ContentType:    staged.ContentType,
		SizeBytes:      staged.Temp.Size(),
		Checksum:       staged.Temp.SHA256(),
		Source:         input.Source,
		Title:          input.Title,
		Tags:           input.Tags,
		UploadedBy:     input.UploadedBy,
		Metadata:       input.Metadata,
		SourceEventID:  staged.EventID,
		CorrelationID:  correlationID,
		IdempotencyKey: idempotencyKey,
	}
}

func UploadedPayloadFromUpload(payload UploadPayload, sourceEventID string) UploadedPayload {
	return UploadedPayload{
		KBID:          payload.KBID,
		MediaID:       payload.MediaID,
		Filename:      payload.Filename,
		ContentType:   payload.ContentType,
		SizeBytes:     payload.SizeBytes,
		Checksum:      payload.Checksum,
		SourceEventID: sourceEventID,
	}
}

func ObjectFromUploadPayload(payload UploadPayload) MediaObject {
	return MediaObject{
		ID:             payload.MediaID,
		KBID:           payload.KBID,
		Filename:       payload.Filename,
		ContentType:    payload.ContentType,
		SizeBytes:      payload.SizeBytes,
		Checksum:       payload.Checksum,
		Source:         payload.Source,
		Title:          payload.Title,
		Tags:           payload.Tags,
		UploadedBy:     payload.UploadedBy,
		Metadata:       payload.Metadata,
		State:          MediaStatePending,
		IdempotencyKey: payload.IdempotencyKey,
	}
}
