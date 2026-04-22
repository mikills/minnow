package scenarios

import (
	"time"

	"github.com/mikills/minnow/sim"
)

// ClockJumpsBackward exercises temporal robustness. After a successful
// ingest, the FakeClock is pulled behind its previous value (simulating an
// NTP step backward, a VM snapshot restore, or a misconfigured node). A
// second ingest and query must still work. Regression target: event
// visibility windows, GC grace checks, or lease expiries that assume
// monotonic time and silently misbehave when now < lastSeen.
func ClockJumpsBackward(h *sim.Harness) {
	const kbID = "clock-jump"
	first := h.GenerateDocs(kbID, 10)
	if err := h.Ingest(kbID, first); err != nil {
		h.Fatalf("first ingest (seed=%d): %v", h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)

	// Net backward jump: advance +10m, then retreat -1h. The clock now sits
	// 50m before any observations recorded during the first ingest.
	h.Clock().Advance(10 * time.Minute)
	h.Clock().Advance(-1 * time.Hour)

	second := h.GenerateDocs(kbID+"-second", 10)
	for i := range second {
		second[i].ID = kbID + "-post-jump-" + second[i].ID[len(kbID)+len("-second-"):]
	}
	if err := h.Ingest(kbID, second); err != nil {
		h.Fatalf("ingest after clock jump (seed=%d): %v", h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)

	probe, err := h.Embed(h.Ctx(), first[0].Text)
	if err != nil {
		h.Fatalf("embed (seed=%d): %v", h.Seed(), err)
	}
	results, err := h.Query(kbID, probe, 50)
	if err != nil {
		h.Fatalf("query after clock jump (seed=%d): %v", h.Seed(), err)
	}

	// Every doc from both ingests should still be reachable; a monotonic-
	// time assumption elsewhere could silently drop rows on the rewind.
	seen := make(map[string]struct{}, len(results))
	for _, r := range results {
		seen[r.ID] = struct{}{}
	}
	for _, doc := range first {
		if _, ok := seen[doc.ID]; !ok {
			h.Errorf("doc %q from pre-jump ingest missing after clock jump (seed=%d)",
				doc.ID, h.Seed())
			return
		}
	}
}
