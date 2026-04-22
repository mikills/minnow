package scenarios

import (
	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

// WarmVsCold ingests a small corpus, queries it twice with the same vector,
// and checks that both calls return the same SET of top-K ids. Exact
// ordering is not asserted because HNSW is approximate: two runs can pick
// the same neighbours in different order depending on graph traversal. The
// real regression target is a result set that drifts after warm-up (e.g. a
// cache bug that returned a stale snapshot).
func WarmVsCold(h *sim.Harness) {
	const kbID = "warm-vs-cold"
	docs := h.GenerateDocs(kbID, 50)
	if err := h.Ingest(kbID, docs); err != nil {
		h.Fatalf("ingest failed (seed=%d): %v", h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)

	probe, err := h.Embed(h.Ctx(), docs[0].Text)
	if err != nil {
		h.Fatalf("embed probe (seed=%d): %v", h.Seed(), err)
	}

	cold, err := h.Query(kbID, probe, 5)
	if err != nil {
		h.Fatalf("cold query (seed=%d): %v", h.Seed(), err)
	}
	warm, err := h.Query(kbID, probe, 5)
	if err != nil {
		h.Fatalf("warm query (seed=%d): %v", h.Seed(), err)
	}
	if len(cold) != len(warm) {
		h.Errorf("cold/warm result lengths differ (seed=%d): %d vs %d",
			h.Seed(), len(cold), len(warm))
		return
	}
	coldIDs := make(map[string]struct{}, len(cold))
	for _, r := range cold {
		coldIDs[r.ID] = struct{}{}
	}
	for _, r := range warm {
		if _, ok := coldIDs[r.ID]; !ok {
			h.Errorf("warm result %q not present in cold result set (seed=%d): cold=%v warm=%v",
				r.ID, h.Seed(), idsOf(cold), idsOf(warm))
			return
		}
	}
}

func idsOf(results []kb.ExpandedResult) []string {
	out := make([]string, len(results))
	for i, r := range results {
		out[i] = r.ID
	}
	return out
}
