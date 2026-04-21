package kb

import (
	"context"
	"errors"
	"io"
)

// Media lifecycle states.
type MediaState string

const (
	MediaStatePending    MediaState = "pending"
	MediaStateActive     MediaState = "active"
	MediaStateTombstoned MediaState = "tombstoned"
)

// MediaLocator narrows a chunk's reference to a region within a media object.
// All fields are optional; presence depends on the media type.
type MediaLocator struct {
	Page        int    `json:"page,omitempty"`
	ByteStart   int64  `json:"byte_start,omitempty"`
	ByteEnd     int64  `json:"byte_end,omitempty"`
	TimeStartMs int64  `json:"time_start_ms,omitempty"`
	TimeEndMs   int64  `json:"time_end_ms,omitempty"`
	BoundingBox string `json:"bounding_box,omitempty"`
}

// ChunkMediaRef links a chunk to a media object. Lightweight by design: it
// carries identity + role only, not bytes.
type ChunkMediaRef struct {
	MediaID  string         `json:"media_id"`
	BlobKey  string         `json:"blob_key,omitempty"`
	Role     string         `json:"role,omitempty"`
	Label    string         `json:"label,omitempty"`
	Locator  *MediaLocator  `json:"locator,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// MediaObject is the first-class metadata record for an uploaded file.
type MediaObject struct {
	ID              string         `json:"id"`
	KBID            string         `json:"kb_id"`
	Filename        string         `json:"filename"`
	ContentType     string         `json:"content_type"`
	SizeBytes       int64          `json:"size_bytes"`
	BlobKey         string         `json:"blob_key"`
	Checksum        string         `json:"checksum"`
	Source          string         `json:"source,omitempty"`
	Title           string         `json:"title,omitempty"`
	Tags            []string       `json:"tags,omitempty"`
	CreatedAtUnixMs int64          `json:"created_at_unix_ms"`
	UploadedBy      string         `json:"uploaded_by,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	State           MediaState     `json:"state"`
	TombstonedAtMs  int64          `json:"tombstoned_at_unix_ms,omitempty"`
	IdempotencyKey  string         `json:"-"`
}

// MediaStore is the metadata store for media objects.
type MediaStore interface {
	Put(ctx context.Context, m MediaObject) error
	Get(ctx context.Context, mediaID string) (*MediaObject, error)
	FindByIdempotency(ctx context.Context, kbID, idempotencyKey string) (*MediaObject, error)
	// List returns a single page of media under kbID, optionally filtered by a
	// filename prefix. Pass after="" for the first page and iterate while the
	// returned NextToken is non-empty.
	List(ctx context.Context, kbID, prefix string, after string, limit int) (MediaPage, error)
	UpdateState(ctx context.Context, mediaID string, state MediaState, tombstonedAtMs int64) error
	// ListByState returns a single page of media with the given state. Pass
	// after="" for the first page and iterate while NextToken is non-empty.
	ListByState(ctx context.Context, kbID string, state MediaState, after string, limit int) (MediaPage, error)
	Delete(ctx context.Context, mediaID string) error
}

// ErrMediaNotFound signals that no media object exists for an id.
var ErrMediaNotFound = errors.New("media: not found")

// ErrMediaDuplicateKey signals duplicate media idempotency keys within a KB.
var ErrMediaDuplicateKey = errors.New("media: duplicate idempotency key")

// MediaUploadInput is the validated input to a media upload.
type MediaUploadInput struct {
	KBID           string
	Filename       string
	ContentType    string
	Source         string
	Title          string
	Tags           []string
	UploadedBy     string
	Metadata       map[string]any
	Body           io.Reader
	IdempotencyKey string

	// AllowedContentTypes optionally restricts accepted content types. An
	// empty allowlist permits any normalised content type. A non-empty
	// list rejects anything else (matched case-insensitively on the
	// prefix-before-';').
	AllowedContentTypes []string
}

// MediaUploadResult summarises a completed upload.
type MediaUploadResult struct {
	MediaID     string
	BlobKey     string
	Filename    string
	ContentType string
	SizeBytes   int64
	Checksum    string
}

// DefaultMaxUploadBytes caps upload size when no per-call limit is set.
const DefaultMaxUploadBytes int64 = 50 * 1024 * 1024
