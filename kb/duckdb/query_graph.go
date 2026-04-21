package duckdb

import (
	"context"
	"database/sql"
	"sort"

	kb "github.com/mikills/minnow/kb"
)

func searchExpandedWithDB(ctx context.Context, db *sql.DB, queryVec []float32, topK int, options kb.ExpansionOptions) ([]kb.ExpandedResult, error) {
	if err := ensureGraphQueryReady(ctx, db); err != nil {
		return nil, err
	}

	seeds, err := QueryTopKWithDB(ctx, db, queryVec, options.SeedK)
	if err != nil {
		return nil, err
	}
	if len(seeds) == 0 {
		return []kb.ExpandedResult{}, nil
	}

	entityScores, err := seedEntityScores(ctx, db, seeds)
	if err != nil {
		return nil, err
	}

	entityScores, err = expandEntityScores(ctx, db, entityScores, options)
	if err != nil {
		return nil, err
	}

	if options.MaxEntityResults > 0 && len(entityScores) > options.MaxEntityResults {
		entityScores = kb.TopNEntityScores(entityScores, options.MaxEntityResults)
	}

	return buildExpandedResults(ctx, db, queryVec, topK, seeds, entityScores, options.Alpha)
}

func mergeExpandedShardResults(shardResults [][]kb.ExpandedResult, topK int) []kb.ExpandedResult {
	if topK <= 0 {
		return []kb.ExpandedResult{}
	}
	flattened := make([]kb.ExpandedResult, 0)
	for _, shard := range shardResults {
		flattened = append(flattened, shard...)
	}
	if len(flattened) == 0 {
		return []kb.ExpandedResult{}
	}

	sort.SliceStable(flattened, func(i, j int) bool {
		if flattened[i].Score == flattened[j].Score {
			if flattened[i].Distance == flattened[j].Distance {
				return flattened[i].ID < flattened[j].ID
			}
			return flattened[i].Distance < flattened[j].Distance
		}
		return flattened[i].Score > flattened[j].Score
	})

	if topK > len(flattened) {
		topK = len(flattened)
	}
	return flattened[:topK]
}

func seedEntityScores(ctx context.Context, db *sql.DB, seeds []kb.QueryResult) (map[string]float64, error) {
	seedDocIDs := extractSeedDocIDs(seeds)
	seedEntities, err := queryEntitiesForDocs(ctx, db, seedDocIDs)
	if err != nil {
		return nil, err
	}
	entityScores := make(map[string]float64, len(seedEntities))
	for id, weight := range seedEntities {
		entityScores[id] = weight
	}
	return entityScores, nil
}

func expandEntityScores(ctx context.Context, db *sql.DB, scores map[string]float64, options kb.ExpansionOptions) (map[string]float64, error) {
	if options.Hops <= 0 || len(scores) == 0 {
		return scores, nil
	}
	if options.UseDuckPGQ {
		return expandEntityScoresDuckPGQ(ctx, db, scores, options)
	}
	return expandEntityScoresBFS(ctx, db, scores, options)
}

func expandEntityScoresBFS(ctx context.Context, db *sql.DB, scores map[string]float64, options kb.ExpansionOptions) (map[string]float64, error) {
	frontier := copyFloatMap(scores)
	for hop := 0; hop < options.Hops; hop++ {
		sources := kb.MapKeys(frontier)
		if len(sources) == 0 {
			break
		}

		edges, err := queryEdgesBySources(ctx, db, sources, options.EdgeTypes, options.MaxNeighborsPerNode)
		if err != nil {
			return nil, err
		}
		if len(edges) == 0 {
			break
		}

		nextFrontier := make(map[string]float64)
		for _, edge := range edges {
			if edge.Weight <= 0 {
				continue
			}
			base := frontier[edge.Src]
			if base <= 0 {
				continue
			}
			nextWeight := base * edge.Weight * options.Decay
			if nextWeight <= 0 {
				continue
			}
			scores[edge.Dst] += nextWeight
			nextFrontier[edge.Dst] += nextWeight
		}

		if len(nextFrontier) == 0 {
			break
		}
		frontier = nextFrontier
	}

	return scores, nil
}

func buildExpandedResults(ctx context.Context, db *sql.DB, queryVec []float32, topK int, seeds []kb.QueryResult, entityScores map[string]float64, alpha float64) ([]kb.ExpandedResult, error) {
	docGraphScore, err := queryDocsForEntities(ctx, db, entityScores)
	if err != nil {
		return nil, err
	}

	candidateIDs := candidateIDsFromSeeds(seeds, docGraphScore)
	docMatches, err := queryDocMatchesForIDs(ctx, db, queryVec, candidateIDs)
	if err != nil {
		return nil, err
	}
	if len(docMatches) == 0 {
		return []kb.ExpandedResult{}, nil
	}

	var maxGraphScore float64
	for _, score := range docGraphScore {
		if score > maxGraphScore {
			maxGraphScore = score
		}
	}

	results := make([]kb.ExpandedResult, 0, len(docMatches))
	for id, match := range docMatches {
		graph := docGraphScore[id]
		graphNorm := 0.0
		if maxGraphScore > 0 {
			graphNorm = graph / maxGraphScore
		}
		sim := 1.0 / (1.0 + match.Distance)
		score := alpha*sim + (1.0-alpha)*graphNorm
		results = append(results, kb.ExpandedResult{
			ID:         id,
			Content:    match.Content,
			Distance:   match.Distance,
			GraphScore: graph,
			Score:      score,
			MediaRefs:  match.MediaRefs,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			if results[i].Distance == results[j].Distance {
				return results[i].ID < results[j].ID
			}
			return results[i].Distance < results[j].Distance
		}
		return results[i].Score > results[j].Score
	})

	if len(results) > topK {
		results = results[:topK]
	}

	return results, nil
}

func candidateIDsFromSeeds(seeds []kb.QueryResult, docGraphScore map[string]float64) []string {
	candidateSet := make(map[string]struct{}, len(seeds)+len(docGraphScore))
	for _, seed := range seeds {
		candidateSet[seed.ID] = struct{}{}
	}
	for id := range docGraphScore {
		candidateSet[id] = struct{}{}
	}

	candidateIDs := make([]string, 0, len(candidateSet))
	for id := range candidateSet {
		candidateIDs = append(candidateIDs, id)
	}
	return candidateIDs
}

func extractSeedDocIDs(seeds []kb.QueryResult) []string {
	seedDocIDs := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		seedDocIDs = append(seedDocIDs, seed.ID)
	}
	return seedDocIDs
}

func copyFloatMap(src map[string]float64) map[string]float64 {
	dst := make(map[string]float64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
