package sim

import (
	"fmt"
	"sort"
)

// Invariant is a property that must hold after a scenario runs. Implementations
// are called from Harness.AssertInvariants.
type Invariant interface {
	Name() string
	Check(h *Harness) error
}

// ManifestMonotonic asserts that every KB's manifest version string, read from
// the store, is equal to or greater (by lexicographic compare) than the last
// version observed during the harness run. The harness's existing ingest
// helpers set lastManifestVers after every Ingest; this invariant just checks
// that the observed version never went backwards on disk.
type manifestMonotonic struct{}

// ManifestMonotonic returns the monotonic-manifest-version invariant.
func ManifestMonotonic() Invariant { return manifestMonotonic{} }

func (manifestMonotonic) Name() string { return "manifest_monotonic" }

func (manifestMonotonic) Check(h *Harness) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for kbID, last := range h.lastManifestVers {
		current, err := h.manifest.HeadVersion(h.ctx, kbID)
		if err != nil {
			return fmt.Errorf("head version for %s: %w", kbID, err)
		}
		if current < last {
			return fmt.Errorf("kb %s manifest regressed from %q to %q", kbID, last, current)
		}
	}
	return nil
}

// NoDocLoss re-queries every ingested doc via ID lookup and asserts each is
// retrievable. Uses a pass-through top-k query with the doc's own embedding
// (via the same embedder) so near-neighbour matches include itself.
type noDocLoss struct {
	topK int
}

// NoDocLoss returns the no-doc-loss invariant. topK controls how many
// neighbours are inspected per query (set generously to tolerate HNSW recall).
func NoDocLoss(topK int) Invariant {
	if topK <= 0 {
		topK = 50
	}
	return noDocLoss{topK: topK}
}

func (n noDocLoss) Name() string { return "no_doc_loss" }

func (n noDocLoss) Check(h *Harness) error {
	h.mu.Lock()
	snapshot := make(map[string]map[string]struct{}, len(h.ingestedDocs))
	for kbID, docs := range h.ingestedDocs {
		ids := make(map[string]struct{}, len(docs))
		for id := range docs {
			ids[id] = struct{}{}
		}
		snapshot[kbID] = ids
	}
	h.mu.Unlock()

	for kbID, expected := range snapshot {
		if len(expected) == 0 {
			continue
		}
		any := pickOne(expected)
		vec, err := h.kb.Embed(h.ctx, any)
		if err != nil {
			return fmt.Errorf("embed probe %q: %w", any, err)
		}
		k := n.topK
		if k < len(expected) {
			k = len(expected)
		}
		results, err := h.Query(kbID, vec, k)
		if err != nil {
			return fmt.Errorf("probe query %s: %w", kbID, err)
		}
		seen := make(map[string]struct{}, len(results))
		for _, r := range results {
			seen[r.ID] = struct{}{}
		}
		missing := make([]string, 0)
		for id := range expected {
			if _, ok := seen[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return fmt.Errorf("kb %s missing %d/%d docs after ingest; sample=%v",
				kbID, len(missing), len(expected), missing[:min(3, len(missing))])
		}
	}
	return nil
}

func pickOne(set map[string]struct{}) string {
	for k := range set {
		return k
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
