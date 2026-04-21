package scenarios

import "github.com/mikills/minnow/sim"

// WarmVsCold ingests a small corpus, queries it twice with the same vector,
// and checks that results are deterministic. The first query pays any one-
// time setup (shard file creation, HNSW index build); the second should hit
// the warm state. Any divergence in result ordering is a bug.
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
	}
	for i := 0; i < len(cold) && i < len(warm); i++ {
		if cold[i].ID != warm[i].ID {
			h.Errorf("cold/warm result ordering differs at %d (seed=%d): %q vs %q",
				i, h.Seed(), cold[i].ID, warm[i].ID)
		}
	}
}
