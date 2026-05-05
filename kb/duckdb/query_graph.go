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

	return buildExpandedResults(ctx, db, expandedResultInput{queryVec: queryVec, topK: topK, seeds: seeds, entityScores: entityScores, alpha: options.Alpha})
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

type expandedResultInput struct {
	queryVec     []float32
	topK         int
	seeds        []kb.QueryResult
	entityScores map[string]float64
	alpha        float64
}

func buildExpandedResults(ctx context.Context, db *sql.DB, input expandedResultInput) ([]kb.ExpandedResult, error) {
	docGraphScore, err := queryDocsForEntities(ctx, db, input.entityScores)
	if err != nil {
		return nil, err
	}

	candidateIDs := candidateIDsFromSeeds(input.seeds, docGraphScore)
	docMatches, err := queryDocMatchesForIDs(ctx, db, input.queryVec, candidateIDs)
	if err != nil {
		return nil, err
	}
	if len(docMatches) == 0 {
		return []kb.ExpandedResult{}, nil
	}

	results := expandedResultsFromMatches(docMatches, docGraphScore, maxFloat64Value(docGraphScore), input.alpha)
	sortExpandedResults(results)
	return limitExpandedResults(results, input.topK), nil
}

func expandedResultsFromMatches(docMatches map[string]docMatch, docGraphScore map[string]float64, maxGraphScore float64, alpha float64) []kb.ExpandedResult {
	results := make([]kb.ExpandedResult, 0, len(docMatches))
	for id, match := range docMatches {
		results = append(results, expandedResultFromMatch(id, match, docGraphScore[id], maxGraphScore, alpha))
	}
	return results
}

func expandedResultFromMatch(id string, match docMatch, graph float64, maxGraphScore float64, alpha float64) kb.ExpandedResult {
	graphNorm := 0.0
	if maxGraphScore > 0 {
		graphNorm = graph / maxGraphScore
	}
	sim := 1.0 / (1.0 + match.Distance)
	return kb.ExpandedResult{ID: id, Content: match.Content, Distance: match.Distance, GraphScore: graph, Score: alpha*sim + (1.0-alpha)*graphNorm, MediaRefs: match.MediaRefs}
}

func maxFloat64Value(values map[string]float64) float64 {
	var maxValue float64
	for _, value := range values {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
}

func sortExpandedResults(results []kb.ExpandedResult) {
	sort.Slice(results, func(i, j int) bool { return expandedResultLess(results[i], results[j]) })
}

func expandedResultLess(left kb.ExpandedResult, right kb.ExpandedResult) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	if left.Distance != right.Distance {
		return left.Distance < right.Distance
	}
	return left.ID < right.ID
}

func limitExpandedResults(results []kb.ExpandedResult, topK int) []kb.ExpandedResult {
	if len(results) > topK {
		return results[:topK]
	}
	return results
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
