package kb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Embedder generates embeddings for text inputs.
type Embedder interface {
	Embed(ctx context.Context, input string) ([]float32, error)
}

// QueryResult represents a single vector search result.
type QueryResult struct {
	ID        string          `json:"id"`       // Document ID
	Content   string          `json:"content"`  // Document text stored at ingestion time
	Distance  float64         `json:"distance"` // Euclidean distance from query vector
	MediaRefs []ChunkMediaRef `json:"media_refs,omitempty"`
}

// Document is a source document for ingestion pipelines. MediaIDs carries
// simple doc-level media references (synthesised into ChunkMediaRef rows
// when persisted). MediaRefs carries rich chunk-level refs (role/label/
// locator) when the ingest caller has them; when both are present MediaRefs
// wins.
type Document struct {
	ID        string
	Text      string
	MediaIDs  []string
	MediaRefs []ChunkMediaRef
	Metadata  map[string]any
}

// Chunk is a text segment with provenance.
type Chunk struct {
	DocID     string
	ChunkID   string
	Text      string
	Start     int
	End       int
	MediaRefs []ChunkMediaRef // optional; nil for text-only chunks
}

// Chunker produces chunks from raw text.
type Chunker interface {
	Chunk(ctx context.Context, docID string, text string) ([]Chunk, error)
}

// KB is the core knowledge base orchestrator for loading, querying,
// mutating, uploading, and maintaining HNSW indices.
type KB struct {
	BlobStore     BlobStore
	ManifestStore ManifestStore
	CacheDir      string
	Embedder      Embedder
	GraphBuilder  *GraphBuilder

	WriteLeaseManager WriteLeaseManager
	WriteLeaseTTL     time.Duration
	RetryObserver     MutationRetryObserver
	ShardingPolicy    ShardingPolicy
	MaxCacheBytes     int64
	CacheEntryTTL     time.Duration

	// Clock drives all time-based behaviour inside the KB (event timestamps,
	// GC cutoffs, TTL comparisons). Defaults to RealClock; substitute a
	// FakeClock for tests and the simulation harness.
	Clock Clock

	// EventStore and EventInbox back the event-driven ingest pipeline. Both
	// are optional; when nil the scheduler skips the related reaper/cleanup
	// jobs.
	EventStore EventStore
	EventInbox EventInbox

	// MediaStore holds media metadata. Optional; when nil the media upload
	// path and media-gc sweeps are no-ops.
	MediaStore MediaStore

	// MediaGC tunes media GC timings. Zero fields fall back to package
	// defaults.
	MediaGC MediaGCConfig

	// MediaContentTypeAllowlist, if non-empty, restricts accepted upload
	// content types. Entries may end with "*" for prefix matching.
	MediaContentTypeAllowlist []string

	cacheBytesCurrent        int64
	cacheEvictionsTTLTotal   uint64
	cacheEvictionsSizeTotal  uint64
	cacheEvictionErrorsTotal uint64
	cacheBudgetExceededTotal uint64
	shardMetricsByKB         map[string]shardMetrics

	formatMu          sync.RWMutex
	formatRegistry    map[string]ArtifactFormat
	defaultFormatKind string
	initErr           error

	mu      sync.Mutex
	locks   map[string]*sync.Mutex
	shardGC []delayedShardGCEntry
}

// KBOption configures KB instances.
type KBOption func(*KB)

// WithEmbedder sets the embedder for generating document embeddings.
func WithEmbedder(embedder Embedder) KBOption {
	return func(kb *KB) {
		kb.Embedder = embedder
	}
}

// WithManifestStore sets a custom ManifestStore implementation.
func WithManifestStore(store ManifestStore) KBOption {
	return func(kb *KB) {
		kb.ManifestStore = store
	}
}

// WithArtifactFormat registers a single artifact format.
// When multiple formats are registered, the first registered format becomes the
// default for new KBs; call SetDefaultFormatKind after construction to override.
func WithArtifactFormat(format ArtifactFormat) KBOption {
	return func(kb *KB) {
		if err := kb.RegisterFormat(format); err != nil {
			kb.setInitErr(fmt.Errorf("with artifact format: %w", err))
		}
	}
}

// WithFormats registers multiple artifact formats in order.
// When multiple formats are registered, the first registered format becomes the
// default for new KBs; call SetDefaultFormatKind after construction to override.
func WithFormats(formats ...ArtifactFormat) KBOption {
	return func(kb *KB) {
		for _, f := range formats {
			if err := kb.RegisterFormat(f); err != nil {
				kb.setInitErr(fmt.Errorf("with formats: %w", err))
			}
		}
	}
}

// WithGraphBuilder sets the graph builder for RAG functionality.
func WithGraphBuilder(builder *GraphBuilder) KBOption {
	return func(kb *KB) {
		kb.GraphBuilder = builder
	}
}

// WithWriteLeaseManager sets the write lease manager for distributed coordination.
func WithWriteLeaseManager(mgr WriteLeaseManager) KBOption {
	return func(kb *KB) {
		if mgr == nil {
			kb.WriteLeaseManager = NewInMemoryWriteLeaseManager()
			return
		}
		kb.WriteLeaseManager = mgr
	}
}

// WithWriteLeaseTTL sets the TTL for write leases.
func WithWriteLeaseTTL(ttl time.Duration) KBOption {
	return func(kb *KB) {
		if ttl <= 0 {
			kb.WriteLeaseTTL = defaultWriteLeaseTTL
			return
		}
		kb.WriteLeaseTTL = ttl
	}
}

// WithShardingPolicy sets sharding/query/compaction policy thresholds.
func WithShardingPolicy(policy ShardingPolicy) KBOption {
	return func(kb *KB) {
		kb.ShardingPolicy = NormalizeShardingPolicy(policy)
	}
}

// WithCompactionEnabled overrides compaction enablement in sharding policy.
func WithCompactionEnabled(enabled bool) KBOption {
	return func(kb *KB) {
		policy := kb.ShardingPolicy
		policy.CompactionEnabled = enabled
		policy.CompactionEnabledSet = true
		kb.ShardingPolicy = NormalizeShardingPolicy(policy)
	}
}

// WithMaxCacheBytes configures on-disk cache size limit for snapshots.
func WithMaxCacheBytes(max int64) KBOption {
	return func(kb *KB) {
		kb.MaxCacheBytes = max
	}
}

// WithCacheEntryTTL configures time-based eviction for cached KB snapshots.
func WithCacheEntryTTL(ttl time.Duration) KBOption {
	return func(kb *KB) {
		kb.CacheEntryTTL = ttl
	}
}

// WithEventStore wires an EventStore into the KB, enabling the event-driven
// ingest pipeline.
func WithEventStore(store EventStore) KBOption {
	return func(kb *KB) { kb.EventStore = store }
}

// WithEventInbox wires an EventInbox into the KB, enabling worker dedup.
func WithEventInbox(inbox EventInbox) KBOption {
	return func(kb *KB) { kb.EventInbox = inbox }
}

// WithMediaStore wires a MediaStore into the KB, enabling media uploads.
func WithMediaStore(store MediaStore) KBOption {
	return func(kb *KB) { kb.MediaStore = store }
}

// WithMediaGCConfig overrides the pending/tombstone/upload-completion
// timings for media GC.
func WithMediaGCConfig(cfg MediaGCConfig) KBOption {
	return func(kb *KB) { kb.MediaGC = cfg }
}

// WithMediaContentTypeAllowlist sets the upload content-type allowlist.
func WithMediaContentTypeAllowlist(list []string) KBOption {
	return func(kb *KB) { kb.MediaContentTypeAllowlist = list }
}

// NewKB creates a new KB instance with the given blob store and cache directory.
// Callers must provide at least one ArtifactFormat via WithArtifactFormat,
// WithFormats, or RegisterFormat; without one, query/ingest/delete
// operations will return ErrArtifactFormatNotConfigured.
func NewKB(bs BlobStore, cacheDir string, opts ...KBOption) *KB {
	kb := &KB{
		BlobStore:         bs,
		CacheDir:          cacheDir,
		WriteLeaseManager: NewInMemoryWriteLeaseManager(),
		WriteLeaseTTL:     defaultWriteLeaseTTL,
		ShardingPolicy:    NormalizeShardingPolicy(ShardingPolicy{}),
		Clock:             RealClock,
		formatRegistry:    make(map[string]ArtifactFormat),
		locks:             make(map[string]*sync.Mutex),
		shardMetricsByKB:  make(map[string]shardMetrics),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(kb)
		}
	}

	if kb.ManifestStore == nil {
		kb.ManifestStore = &BlobManifestStore{Store: kb.BlobStore}
	}
	if kb.Clock == nil {
		kb.Clock = RealClock
	}

	return kb
}

// WithClock overrides the KB's Clock. Use FakeClock in tests and simulation.
func WithClock(c Clock) KBOption {
	return func(kb *KB) { kb.Clock = c }
}

func (l *KB) SetWriteLeaseManager(mgr WriteLeaseManager) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if mgr == nil {
		l.WriteLeaseManager = NewInMemoryWriteLeaseManager()
		return
	}
	l.WriteLeaseManager = mgr
}

func (l *KB) SetWriteLeaseTTL(ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ttl <= 0 {
		l.WriteLeaseTTL = defaultWriteLeaseTTL
		return
	}
	l.WriteLeaseTTL = ttl
}

// SetMaxCacheBytes updates max cache bytes for local snapshot eviction.
func (l *KB) SetMaxCacheBytes(max int64) {
	l.mu.Lock()
	l.MaxCacheBytes = max
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), defaultCacheEvictionRetryWindow)
	defer cancel()
	_ = l.evictCacheIfNeeded(ctx, "")
}

// SetCacheEntryTTL updates TTL for time-based cache eviction.
func (l *KB) SetCacheEntryTTL(ttl time.Duration) {
	l.mu.Lock()
	l.CacheEntryTTL = ttl
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), defaultCacheEvictionRetryWindow)
	defer cancel()
	_ = l.evictCacheIfNeeded(ctx, "")
}

func (l *KB) SetGraphBuilder(builder *GraphBuilder) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.GraphBuilder = builder
}

func (l *KB) LockFor(kbID string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.locks[kbID]; !ok {
		l.locks[kbID] = &sync.Mutex{}
	}
	return l.locks[kbID]
}

// EvictCacheIfNeeded runs cache eviction, protecting the given KB ID.
func (l *KB) EvictCacheIfNeeded(ctx context.Context, protectKBID string) error {
	return l.evictCacheIfNeeded(ctx, protectKBID)
}

// Close releases resources held by the KB, including pooled shard connections.
func (l *KB) Close() {
	for _, format := range l.registeredFormatsSnapshot() {
		if c, ok := format.(io.Closer); ok {
			c.Close()
		}
	}
}

// RegisterFormat registers an artifact format under its Kind().
// The first registered format becomes the default for new KBs.
// Returns ErrInvalidArtifactFormat when format is nil or Kind() is empty.
func (l *KB) RegisterFormat(format ArtifactFormat) error {
	if format == nil {
		return fmt.Errorf("%w: nil format", ErrInvalidArtifactFormat)
	}
	kind := strings.TrimSpace(format.Kind())
	if kind == "" {
		return fmt.Errorf("%w: empty format kind", ErrInvalidArtifactFormat)
	}

	l.formatMu.Lock()
	defer l.formatMu.Unlock()

	if len(l.formatRegistry) == 0 {
		l.defaultFormatKind = kind
	}

	l.formatRegistry[kind] = format

	return nil
}

// SetDefaultFormatKind changes which registered format is used for new KBs.
func (l *KB) SetDefaultFormatKind(kind string) error {
	l.formatMu.Lock()
	defer l.formatMu.Unlock()

	if _, ok := l.formatRegistry[kind]; !ok {
		return fmt.Errorf("%w: %s", ErrFormatNotRegistered, kind)
	}

	l.defaultFormatKind = kind

	return nil
}

// HasFormat returns true if at least one artifact format is registered.
func (l *KB) HasFormat() bool {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()
	return len(l.formatRegistry) > 0
}

// FormatByKind returns a registered format by kind, or nil if not found.
func (l *KB) FormatByKind(kind string) ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if f, ok := l.formatRegistry[kind]; ok {
		return f
	}

	return nil
}

// DefaultFormat returns the default registered artifact format, or nil if none.
func (l *KB) DefaultFormat() ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if f, ok := l.formatRegistry[l.defaultFormatKind]; ok {
		return f
	}

	return nil
}

// resolveFormat returns the ArtifactFormat for a given kbID by reading
// the manifest's format_kind. Falls back to the default for new KBs.
func (l *KB) resolveFormat(ctx context.Context, kbID string) (ArtifactFormat, error) {
	if err := l.getInitErr(); err != nil {
		return nil, err
	}
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			// New KB: use default format
			if format := l.DefaultFormat(); format != nil {
				return format, nil
			}

			return nil, ErrArtifactFormatNotConfigured
		}

		return nil, err
	}

	return l.resolveFormatByKind(doc.Manifest.FormatKind)
}

func (l *KB) setInitErr(err error) {
	if err == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.initErr == nil {
		l.initErr = err
	}
}

func (l *KB) getInitErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.initErr
}

// resolveFormatByKind looks up a format by kind string without reading the manifest.
func (l *KB) resolveFormatByKind(kind string) (ArtifactFormat, error) {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if f, ok := l.formatRegistry[kind]; ok {
		return f, nil
	}

	return nil, fmt.Errorf("%w: %s", ErrFormatNotRegistered, kind)
}

func (l *KB) registeredFormatsSnapshot() []ArtifactFormat {
	l.formatMu.RLock()
	defer l.formatMu.RUnlock()

	if len(l.formatRegistry) == 0 {
		return nil
	}

	formats := make([]ArtifactFormat, 0, len(l.formatRegistry))
	for _, format := range l.formatRegistry {
		formats = append(formats, format)
	}

	return formats
}

// Embed returns an embedding for input using the configured Embedder.
func (k *KB) Embed(ctx context.Context, input string) ([]float32, error) {
	if k.Embedder == nil {
		return nil, fmt.Errorf("embedder is not configured")
	}

	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}

	return k.Embedder.Embed(ctx, input)
}

// Compactor is an optional interface that ArtifactFormat implementations can
// satisfy to support background compaction of sharded data.
type Compactor interface {
	CompactIfNeeded(ctx context.Context, kbID string) (*CompactionPublishResult, error)
}

// CompactIfNeeded triggers compaction for the given KB if the resolved format
// supports it. Returns a zero result when the format does not implement Compactor.
func (l *KB) CompactIfNeeded(ctx context.Context, kbID string) (*CompactionPublishResult, error) {
	format, err := l.resolveFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}
	if c, ok := format.(Compactor); ok {
		return c.CompactIfNeeded(ctx, kbID)
	}
	return &CompactionPublishResult{}, nil
}
