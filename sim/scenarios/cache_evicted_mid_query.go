package scenarios

import "github.com/mikills/minnow/sim"

// CacheEvictedMidQuery ingests a corpus, queries it once (populating the
// local shard cache), wipes the cache dir, then queries again. The second
// query must succeed by re-downloading shards from the blob store — exercises
// the cold-start path that a pod restart or aggressive eviction would hit.
func CacheEvictedMidQuery(h *sim.Harness) {
	const kbID = "cache-wipe"
	docs := h.GenerateDocs(kbID, 30)
	if err := h.Ingest(kbID, docs); err != nil {
		h.Fatalf("ingest (seed=%d): %v", h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)

	probe, err := h.Embed(h.Ctx(), docs[0].Text)
	if err != nil {
		h.Fatalf("embed (seed=%d): %v", h.Seed(), err)
	}
	if _, err := h.Query(kbID, probe, 5); err != nil {
		h.Fatalf("warm query (seed=%d): %v", h.Seed(), err)
	}
	if err := h.WipeCache(); err != nil {
		h.Fatalf("wipe cache (seed=%d): %v", h.Seed(), err)
	}
	if _, err := h.Query(kbID, probe, 5); err != nil {
		h.Fatalf("post-wipe query (seed=%d): %v", h.Seed(), err)
	}
}
