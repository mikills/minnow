package scenarios

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/mikills/minnow/sim"
)

// RandomOpsFuzz runs a seeded random sequence of operations (ingest, query,
// wipe cache, toggle fault rates) against a small pool of KBs and relies on
// the registered invariants to catch drift. Regression target: any arbitrary
// combination of operations that produces a state the individual scenarios
// don't exercise.
func RandomOpsFuzz(h *sim.Harness) {
	const (
		numKBs   = 3
		numOps   = 40
		batchLen = 8
	)

	// Use a dedicated RNG so we don't trample the harness's shared one.
	rng := rand.New(rand.NewSource(h.Seed() + 7919))

	kbIDs := make([]string, numKBs)
	for i := range kbIDs {
		kbIDs[i] = fmt.Sprintf("fuzz-kb-%d", i)
	}

	for step := 0; step < numOps; step++ {
		op := rng.Intn(100)
		kbID := kbIDs[rng.Intn(numKBs)]

		switch {
		case op < 50: // 50% ingest
			docs := h.GenerateDocs(fmt.Sprintf("%s-s%d", kbID, step), batchLen)
			for i := range docs {
				docs[i].ID = fmt.Sprintf("%s-s%d-%d", kbID, step, i)
			}
			err := h.Ingest(kbID, docs)
			if err != nil && !errors.Is(err, sim.ErrInjected) {
				h.Errorf("fuzz ingest step %d (seed=%d, kb=%s): %v",
					step, h.Seed(), kbID, err)
				continue
			}
			if err == nil {
				h.RecordManifestVersion(kbID)
			}

		case op < 85: // 35% query
			vec := h.RandomVec(32)
			_, err := h.Query(kbID, vec, 5)
			if err != nil && !errors.Is(err, sim.ErrInjected) &&
				!isTolerableConcurrentReadError(err) {
				h.Errorf("fuzz query step %d (seed=%d, kb=%s): %v",
					step, h.Seed(), kbID, err)
			}

		case op < 92: // 7% wipe cache
			if err := h.WipeCache(); err != nil {
				h.Errorf("fuzz wipe cache step %d (seed=%d): %v",
					step, h.Seed(), err)
			}

		case op < 96: // 4% inject faults
			h.SetBlobFaults(sim.BlobFaults{
				UploadFailRate:   0.1,
				DownloadFailRate: 0.1,
			})

		default: // 4% clear faults
			h.SetBlobFaults(sim.BlobFaults{})
		}
	}

	// Leave the store in a clean state for the invariant pass.
	h.SetBlobFaults(sim.BlobFaults{})
}
