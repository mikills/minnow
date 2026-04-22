package scenarios

import (
	"fmt"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

// RepeatedSmallIngests performs many consecutive single-doc upserts against
// one KB. Regression targets: manifest version growth, shard rollover under
// accumulation, compaction behaviour, and whether the full set stays
// queryable after dozens of publishes.
func RepeatedSmallIngests(h *sim.Harness) {
	const (
		kbID   = "repeated-ingest"
		rounds = 30
	)
	for i := 0; i < rounds; i++ {
		doc := kb.Document{
			ID:   fmt.Sprintf("%s-%05d", kbID, i),
			Text: fmt.Sprintf("round %d content for %s", i, kbID),
		}
		if err := h.Ingest(kbID, []kb.Document{doc}); err != nil {
			h.Fatalf("round %d ingest (seed=%d): %v", i, h.Seed(), err)
		}
		h.RecordManifestVersion(kbID)
	}

	// Spot-check: every ingested doc must be reachable via its own embedding.
	sampleIDs := []int{0, rounds / 2, rounds - 1}
	for _, idx := range sampleIDs {
		id := fmt.Sprintf("%s-%05d", kbID, idx)
		probe, err := h.Embed(h.Ctx(), fmt.Sprintf("round %d content for %s", idx, kbID))
		if err != nil {
			h.Fatalf("embed sample %d (seed=%d): %v", idx, h.Seed(), err)
		}
		results, err := h.Query(kbID, probe, 50)
		if err != nil {
			h.Fatalf("query sample %d (seed=%d): %v", idx, h.Seed(), err)
		}
		found := false
		for _, r := range results {
			if r.ID == id {
				found = true
				break
			}
		}
		if !found {
			h.Errorf("doc %s missing from top-50 after %d rounds (seed=%d)", id, rounds, h.Seed())
		}
	}
}
