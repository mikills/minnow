package sim

import (
	"fmt"
	"sort"

	"github.com/mikills/minnow/kb"
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
// helpers set lastManifestVers after every Ingest. this invariant just checks
// that the observed version never went backwards on disk.
type manifestMonotonic struct{}

// ManifestMonotonic returns the monotonic-manifest-version invariant.
func ManifestMonotonic() Invariant { return manifestMonotonic{} }

func (manifestMonotonic) Name() string { return "manifest_monotonic" }

func (manifestMonotonic) Check(h *Harness) error {
	h.mu.Lock()
	snapshot := make(map[string]string, len(h.lastManifestVers))
	for kbID, v := range h.lastManifestVers {
		snapshot[kbID] = v
	}
	h.mu.Unlock()

	for kbID, last := range snapshot {
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
	snapshot := ingestedDocsSnapshot(h)

	for kbID, expected := range snapshot {
		if len(expected) == 0 {
			continue
		}
		missing, err := n.missingDocs(h, kbID, expected)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			sort.Strings(missing)
			return fmt.Errorf("kb %s missing %d/%d docs after ingest; sample=%v",
				kbID, len(missing), len(expected), missing[:min(3, len(missing))])
		}
	}
	return nil
}

func ingestedDocsSnapshot(h *Harness) map[string]map[string]struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := make(map[string]map[string]struct{}, len(h.ingestedDocs))
	for kbID, docs := range h.ingestedDocs {
		snapshot[kbID] = docIDSet(docs)
	}
	return snapshot
}

func docIDSet(docs map[string]kb.Document) map[string]struct{} {
	ids := make(map[string]struct{}, len(docs))
	for id := range docs {
		ids[id] = struct{}{}
	}
	return ids
}

func (n noDocLoss) missingDocs(h *Harness, kbID string, expected map[string]struct{}) ([]string, error) {
	probe := pickOne(expected)
	vec, err := h.kb.Embed(h.ctx, probe)
	if err != nil {
		return nil, fmt.Errorf("embed probe %q: %w", probe, err)
	}
	matches, err := h.Search(kbID, vec, max(n.topK, len(expected)))
	if err != nil {
		return nil, fmt.Errorf("probe query %s: %w", kbID, err)
	}
	return missingDocIDs(expected, matches), nil
}

func missingDocIDs(expected map[string]struct{}, matches []kb.ExpandedResult) []string {
	seen := make(map[string]struct{}, len(matches))
	for _, result := range matches {
		seen[result.ID] = struct{}{}
	}
	missing := make([]string, 0)
	for id := range expected {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

// ShardsInManifestExist asserts that every shard listed in every KB's
// manifest has a corresponding blob object. Catches bugs where a manifest is
// swapped before its shards finish uploading.
type shardsInManifestExist struct{}

// ShardsInManifestExist returns the shard-liveness invariant.
func ShardsInManifestExist() Invariant { return shardsInManifestExist{} }

func (shardsInManifestExist) Name() string { return "shards_in_manifest_exist" }

func (shardsInManifestExist) Check(h *Harness) error {
	for _, kbID := range h.KBIDs() {
		shards, err := h.ManifestShards(h.ctx, kbID)
		if err != nil {
			return fmt.Errorf("read manifest for %s: %w", kbID, err)
		}
		for _, shard := range shards {
			info, err := h.blobStore.Head(h.ctx, shard.Key)
			if err != nil {
				return fmt.Errorf("kb %s shard %s (%s) missing from blob store: %w",
					kbID, shard.ShardID, shard.Key, err)
			}
			if info == nil {
				return fmt.Errorf("kb %s shard %s (%s): Head returned nil info",
					kbID, shard.ShardID, shard.Key)
			}
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
