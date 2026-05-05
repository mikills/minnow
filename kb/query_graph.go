package kb

import (
	"context"
	"fmt"
)

// ExpansionOptions configures graph-based search expansion.
// Zero values are replaced with defaults.
type ExpansionOptions struct {
	SeedK               int
	Hops                int
	MaxNeighborsPerNode int
	Alpha               float64
	Decay               float64
	EdgeTypes           []string
	UseDuckPGQ          bool
	MaxEntityResults    int
	OfflineExt          bool
}

// ExpandedResult represents a blended result combining vector and graph scores.
type ExpandedResult struct {
	ID         string
	Content    string
	Distance   float64
	GraphScore float64
	Score      float64
	MediaRefs  []ChunkMediaRef
}

// SearchMode controls which retrieval strategy is used.
//
//   - SearchModeVector: pure vector similarity search.
//   - SearchModeGraph: vector seed + BFS/PGQ graph expansion (true hybrid).
//   - SearchModeAdaptive: vector-first with graph fallback when top-1
//     similarity is below AdaptiveMinSim.
type SearchMode int

const (
	SearchModeVector SearchMode = iota
	SearchModeGraph
	SearchModeAdaptive
)

// SearchOptions configures search strategy selection.
type SearchOptions struct {
	Mode           SearchMode
	TopK           int
	MaxDistance    *float64
	Expansion      *ExpansionOptions
	AdaptiveMinSim float64
}

type EdgeRow struct {
	Src    string
	Dst    string
	Weight float64
}

// Search performs vector, graph, or adaptive retrieval based on options.
func (k *KB) Search(ctx context.Context, kbID string, queryVec []float32, opts *SearchOptions) ([]ExpandedResult, error) {
	format, err := k.resolveFormat(ctx, kbID)
	if err != nil {
		return nil, err
	}

	options := normalizeSearchOptions(opts)
	if options.TopK <= 0 {
		return nil, fmt.Errorf("%w: top_k must be > 0", ErrInvalidQueryRequest)
	}

	switch options.Mode {
	case SearchModeGraph:
		return queryGraphSearch(ctx, format, kbID, queryVec, options)
	case SearchModeAdaptive:
		return queryAdaptiveSearch(ctx, format, kbID, queryVec, options)
	default:
		return queryVectorSearch(ctx, format, kbID, queryVec, options)
	}
}

func queryGraphSearch(ctx context.Context, format ArtifactFormat, kbID string, queryVec []float32, options SearchOptions) ([]ExpandedResult, error) {
	graphReq := graphQueryRequest(kbID, queryVec, options)
	if err := ValidateGraphQueryRequest(graphReq); err != nil {
		return nil, err
	}
	return format.QueryGraph(ctx, graphReq)
}

func queryAdaptiveSearch(ctx context.Context, format ArtifactFormat, kbID string, queryVec []float32, options SearchOptions) ([]ExpandedResult, error) {
	vectorResults, err := queryVectorSearch(ctx, format, kbID, queryVec, options)
	if err != nil || len(vectorResults) == 0 {
		return vectorResults, err
	}
	if vectorResultSimilarity(vectorResults[0]) >= options.AdaptiveMinSim {
		return vectorResults, nil
	}
	return queryGraphSearch(ctx, format, kbID, queryVec, options)
}

func queryVectorSearch(ctx context.Context, format ArtifactFormat, kbID string, queryVec []float32, options SearchOptions) ([]ExpandedResult, error) {
	ragReq := RagQueryRequest{KBID: kbID, QueryVec: queryVec, Options: RagQueryOptions{TopK: options.TopK, MaxDistance: options.MaxDistance}}
	if err := ValidateRagQueryRequest(ragReq); err != nil {
		return nil, err
	}
	return format.QueryRag(ctx, ragReq)
}

func graphQueryRequest(kbID string, queryVec []float32, options SearchOptions) GraphQueryRequest {
	return GraphQueryRequest{KBID: kbID, QueryVec: queryVec, Options: GraphQueryOptions{TopK: options.TopK, MaxDistance: options.MaxDistance, Expansion: options.Expansion}}
}

func vectorResultSimilarity(result ExpandedResult) float64 {
	return 1.0 / (1.0 + result.Distance)
}

func NormalizeExpansionOptions(topK int, opts *ExpansionOptions) ExpansionOptions {
	defaults := ExpansionOptions{
		SeedK:               max(topK, 10),
		Hops:                2,
		MaxNeighborsPerNode: 25,
		Alpha:               0.7,
		Decay:               0.7,
		MaxEntityResults:    1000,
	}
	if opts == nil {
		return defaults
	}

	normalized := *opts
	if normalized.SeedK <= 0 {
		normalized.SeedK = defaults.SeedK
	}

	if normalized.Hops < 0 {
		normalized.Hops = defaults.Hops
	}

	if normalized.MaxNeighborsPerNode <= 0 {
		normalized.MaxNeighborsPerNode = defaults.MaxNeighborsPerNode
	}

	if normalized.Alpha < 0 || normalized.Alpha > 1 {
		normalized.Alpha = defaults.Alpha
	}

	if normalized.Decay <= 0 || normalized.Decay > 1 {
		normalized.Decay = defaults.Decay
	}

	if normalized.MaxEntityResults <= 0 {
		normalized.MaxEntityResults = defaults.MaxEntityResults
	}

	return normalized
}

func normalizeSearchOptions(opts *SearchOptions) SearchOptions {
	defaults := SearchOptions{
		Mode:           SearchModeVector,
		AdaptiveMinSim: 0.35,
	}
	if opts == nil {
		return defaults
	}

	normalized := *opts
	if normalized.Mode != SearchModeGraph && normalized.Mode != SearchModeAdaptive {
		normalized.Mode = defaults.Mode
	}

	if normalized.AdaptiveMinSim <= 0 || normalized.AdaptiveMinSim > 1 {
		normalized.AdaptiveMinSim = defaults.AdaptiveMinSim
	}

	return normalized
}
