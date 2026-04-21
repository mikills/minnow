package sim

import (
	"errors"
	"fmt"
)

// Scenario is a function that exercises a particular workload/fault combo.
// Scenarios are responsible for driving operations; invariants are checked
// after the scenario returns via Harness.AssertInvariants.
type Scenario func(h *Harness)

// WarmVsCold ingests a small corpus, queries it twice with the same vector,
// and checks that results are deterministic. The first query pays any
// one-time setup (shard file creation, HNSW index build); the second should
// hit the warm state.
func WarmVsCold(h *Harness) {
	const kbID = "warm-vs-cold"
	docs := h.GenerateDocs(kbID, 50)
	if err := h.Ingest(kbID, docs); err != nil {
		h.t.Fatalf("ingest failed (seed=%d): %v", h.Seed(), err)
	}
	h.recordManifestVersion(kbID)

	probe, err := h.kb.Embed(h.Ctx(), docs[0].Text)
	if err != nil {
		h.t.Fatalf("embed probe (seed=%d): %v", h.Seed(), err)
	}

	cold, err := h.Query(kbID, probe, 5)
	if err != nil {
		h.t.Fatalf("cold query (seed=%d): %v", h.Seed(), err)
	}
	warm, err := h.Query(kbID, probe, 5)
	if err != nil {
		h.t.Fatalf("warm query (seed=%d): %v", h.Seed(), err)
	}
	if len(cold) != len(warm) {
		h.t.Errorf("cold/warm result lengths differ (seed=%d): %d vs %d", h.Seed(), len(cold), len(warm))
	}
	for i := 0; i < len(cold) && i < len(warm); i++ {
		if cold[i].ID != warm[i].ID {
			h.t.Errorf("cold/warm result ordering differs at %d (seed=%d): %q vs %q", i, h.Seed(), cold[i].ID, warm[i].ID)
		}
	}
}

// FlakyBlobIngest injects a non-zero upload failure rate and asserts that
// ingest either succeeds after retry or surfaces ErrInjected (never silently
// loses data). The retry here is caller-driven since the sync Ingest path
// does not wrap CAS failures in automatic backoff beyond the manifest store.
func FlakyBlobIngest(h *Harness) {
	const kbID = "flaky-blob"
	h.SetBlobFaults(BlobFaults{UploadFailRate: 0.15})
	defer h.SetBlobFaults(BlobFaults{})

	for batch := 0; batch < 5; batch++ {
		docs := h.GenerateDocs(fmt.Sprintf("%s-b%d", kbID, batch), 10)
		// Move into kbID so the invariant sees one logical KB.
		for i := range docs {
			docs[i].ID = fmt.Sprintf("%s-%d-%d", kbID, batch, i)
		}

		var lastErr error
		for attempt := 0; attempt < 10; attempt++ {
			lastErr = h.Ingest(kbID, docs)
			if lastErr == nil {
				break
			}
			if !errors.Is(lastErr, ErrInjected) {
				h.t.Fatalf("unexpected non-injected error on batch %d attempt %d (seed=%d): %v",
					batch, attempt, h.Seed(), lastErr)
			}
		}
		if lastErr != nil {
			h.t.Fatalf("ingest exhausted retries on batch %d (seed=%d): %v", batch, h.Seed(), lastErr)
		}
		h.recordManifestVersion(kbID)
	}
}

// recordManifestVersion reads the current manifest version for kbID and
// stores it on the harness so the monotonic invariant can compare against
// future writes on the same KB.
func (h *Harness) recordManifestVersion(kbID string) {
	v, err := h.manifest.HeadVersion(h.Ctx(), kbID)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.lastManifestVers[kbID] = v
	h.mu.Unlock()
}
