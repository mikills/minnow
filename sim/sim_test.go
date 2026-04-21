package sim_test

import (
	"fmt"
	"testing"

	"github.com/mikills/minnow/sim"
	"github.com/mikills/minnow/sim/scenarios"
)

// TestSim exercises each scenario under a fixed set of seeds. Failures print
// the seed so a reviewer can rerun with -run to reproduce.
//
// To add a new scenario, drop a file under sim/scenarios/ that exports a
// func(h *sim.Harness) and add it to the map below.
func TestSim(t *testing.T) {
	seeds := []int64{1, 2, 3, 7, 42, 100}

	cases := map[string]func(*sim.Harness){
		"warm_vs_cold":            scenarios.WarmVsCold,
		"flaky_blob_ingest":       scenarios.FlakyBlobIngest,
		"concurrent_writers":      scenarios.ConcurrentWriters,
		"cache_evicted_mid_query": scenarios.CacheEvictedMidQuery,
		"high_download_failure":   scenarios.HighDownloadFailure,
	}

	invariants := []sim.Invariant{
		sim.ManifestMonotonic(),
		sim.NoDocLoss(50),
		sim.ShardsInManifestExist(),
	}

	for name, scenario := range cases {
		name, scenario := name, scenario
		for _, seed := range seeds {
			seed := seed
			t.Run(fmt.Sprintf("%s/seed_%d", name, seed), func(t *testing.T) {
				h := sim.New(t, sim.WithSeed(seed), sim.WithInvariants(invariants...))
				scenario(h)
				h.AssertInvariants()
			})
		}
	}
}
