package sim

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
)

// Harness is a seeded, deterministic test rig for minnow. It wires the KB and
// its stores against a FakeClock and a FaultableBlobStore so scenarios can
// drive time forward and inject storage failures while checking invariants.
type Harness struct {
	t    *testing.T
	ctx  context.Context
	seed int64
	rng  *rand.Rand

	clock     *kb.FakeClock
	blobRoot  string
	cacheDir  string
	blobStore *FaultableBlobStore
	manifest  *kb.BlobManifestStore
	kb        *kb.KB
	format    *duckdb.DuckDBArtifactFormat

	mu               sync.Mutex
	ingestedDocs     map[string]map[string]kb.Document // kbID → docID → doc
	lastManifestVers map[string]string                 // kbID → last observed version

	invariants []Invariant
}

// Option customises a Harness at construction time.
type Option func(*config)

type config struct {
	seed       int64
	startTime  time.Time
	faults     BlobFaults
	invariants []Invariant
}

// WithSeed picks the deterministic RNG seed used for RNG-driven choices and
// for blob fault injection.
func WithSeed(seed int64) Option { return func(c *config) { c.seed = seed } }

// WithBlobFaults configures the faultable blob store.
func WithBlobFaults(f BlobFaults) Option { return func(c *config) { c.faults = f } }

// WithStartTime sets the FakeClock's initial time.
func WithStartTime(t time.Time) Option { return func(c *config) { c.startTime = t } }

// WithInvariants attaches invariants evaluated on AssertInvariants.
func WithInvariants(inv ...Invariant) Option {
	return func(c *config) { c.invariants = append(c.invariants, inv...) }
}

// New constructs a Harness and registers t.Cleanup to release resources.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()

	cfg := config{
		seed:      1,
		startTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, o := range opts {
		o(&cfg)
	}

	clock := kb.NewFakeClock(cfg.startTime)
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	cacheDir := filepath.Join(t.TempDir(), "cache")

	inner := &kb.LocalBlobStore{Root: blobRoot}
	blobs := NewFaultableBlobStore(inner, cfg.faults, rand.New(rand.NewSource(cfg.seed)))

	manifest := &kb.BlobManifestStore{Store: blobs}

	embedder, err := kb.NewLocalEmbedder(32)
	require.NoError(t, err)

	eventStore := kb.NewInMemoryEventStore()
	eventStore.Clock = clock
	eventInbox := kb.NewInMemoryEventInbox()
	eventInbox.Clock = clock
	lease := kb.NewInMemoryWriteLeaseManager()
	lease.Clock = clock

	loader := kb.NewKB(blobs, cacheDir,
		kb.WithEmbedder(embedder),
		kb.WithManifestStore(manifest),
		kb.WithClock(clock),
	)
	loader.WriteLeaseManager = lease
	loader.EventStore = eventStore
	loader.EventInbox = eventInbox

	format, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
		BlobStore:      blobs,
		ManifestStore:  manifest,
		CacheDir:       cacheDir,
		MemoryLimit:    "256MB",
		ShardingPolicy: loader.ShardingPolicy,
		Embed:          loader.Embed,
		GraphBuilder:   func() *kb.GraphBuilder { return loader.GraphBuilder },
		EvictCacheIfNeeded: func(ctx context.Context, protectKBID string) error {
			return loader.EvictCacheIfNeeded(ctx, protectKBID)
		},
		LockFor: loader.LockFor,
		AcquireWriteLease: func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
			return loader.AcquireWriteLease(ctx, kbID)
		},
		EnqueueReplacedShardsForGC: loader.EnqueueReplacedShardsForGC,
		Metrics:                    loader,
	})
	require.NoError(t, err)
	require.NoError(t, loader.RegisterFormat(format))

	h := &Harness{
		t:                t,
		ctx:              context.Background(),
		seed:             cfg.seed,
		rng:              rand.New(rand.NewSource(cfg.seed)),
		clock:            clock,
		blobRoot:         blobRoot,
		cacheDir:         cacheDir,
		blobStore:        blobs,
		manifest:         manifest,
		kb:               loader,
		format:           format,
		ingestedDocs:     make(map[string]map[string]kb.Document),
		lastManifestVers: make(map[string]string),
		invariants:       cfg.invariants,
	}
	t.Cleanup(h.close)
	return h
}

func (h *Harness) close() {
	h.format.Close()
}

// Seed returns the RNG seed this harness was constructed with. Useful in
// failure messages so a reviewer can reproduce the run.
func (h *Harness) Seed() int64 { return h.seed }

// Clock exposes the FakeClock so scenarios can advance time directly.
func (h *Harness) Clock() *kb.FakeClock { return h.clock }

// SetBlobFaults replaces the active fault config mid-scenario.
func (h *Harness) SetBlobFaults(f BlobFaults) { h.blobStore.SetFaults(f) }

// GenerateDocs returns n deterministic documents seeded from the harness RNG.
// Each doc's text is "<kbID>-<seed>-<i>" so re-runs with the same seed produce
// identical documents and identical embeddings.
func (h *Harness) GenerateDocs(kbID string, n int) []kb.Document {
	out := make([]kb.Document, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d-%05d", kbID, h.seed, i)
		out[i] = kb.Document{ID: id, Text: id + " synthetic content for sim"}
	}
	return out
}

// RandomVec returns a pseudo-random unit-length vector of the requested dim.
func (h *Harness) RandomVec(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(h.rng.NormFloat64())
	}
	return normalize(vec)
}

func normalize(vec []float32) []float32 {
	var sum float64
	for _, v := range vec {
		sum += float64(v) * float64(v)
	}
	if sum == 0 {
		return vec
	}
	n := float32(1.0 / (sum * 0.5))
	for i := range vec {
		vec[i] *= n
	}
	return vec
}

// Ingest upserts docs into kbID and tracks them for no-doc-loss invariants.
func (h *Harness) Ingest(kbID string, docs []kb.Document) error {
	if err := h.kb.UpsertDocsAndUpload(h.ctx, kbID, docs); err != nil {
		return err
	}
	h.mu.Lock()
	m, ok := h.ingestedDocs[kbID]
	if !ok {
		m = make(map[string]kb.Document)
		h.ingestedDocs[kbID] = m
	}
	for _, d := range docs {
		m[d.ID] = d
	}
	h.mu.Unlock()
	return nil
}

// Query runs a top-k vector query against kbID.
func (h *Harness) Query(kbID string, vec []float32, k int) ([]kb.ExpandedResult, error) {
	return h.format.QueryRag(h.ctx, kb.RagQueryRequest{
		KBID:     kbID,
		QueryVec: vec,
		Options:  kb.RagQueryOptions{TopK: k},
	})
}

// ManifestVersion returns the current manifest version for kbID, or "" if the
// KB has no manifest yet.
func (h *Harness) ManifestVersion(kbID string) string {
	v, err := h.manifest.HeadVersion(h.ctx, kbID)
	if err != nil {
		return ""
	}
	return v
}

// IngestedDocIDs returns a snapshot of the doc IDs ingested into kbID so far.
func (h *Harness) IngestedDocIDs(kbID string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]string, 0, len(h.ingestedDocs[kbID]))
	for id := range h.ingestedDocs[kbID] {
		ids = append(ids, id)
	}
	return ids
}

// KB returns the underlying KB for scenarios that need deeper access.
func (h *Harness) KB() *kb.KB { return h.kb }

// Format returns the DuckDB artifact format.
func (h *Harness) Format() *duckdb.DuckDBArtifactFormat { return h.format }

// Ctx returns the base context. Scenarios wanting a timeout should derive
// their own via context.WithTimeout.
func (h *Harness) Ctx() context.Context { return h.ctx }

// AssertInvariants runs all registered invariants and reports failures via
// t.Errorf (non-fatal so the full batch runs). Returns the number of
// violations.
func (h *Harness) AssertInvariants() int {
	h.t.Helper()
	violations := 0
	for _, inv := range h.invariants {
		if err := inv.Check(h); err != nil {
			h.t.Errorf("invariant %q violated (seed=%d): %v", inv.Name(), h.seed, err)
			violations++
		}
	}
	return violations
}
