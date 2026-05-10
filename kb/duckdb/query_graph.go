package duckdb

import (
	"context"
	"database/sql"
	"maps"

	kb "github.com/mikills/minnow/kb"
	graph "github.com/mikills/minnow/kb/duckdb/internal/graph"
)

func searchExpandedWithDB(
	ctx context.Context,
	db *sql.DB,
	queryVec []float32,
	topK int,
	options kb.ExpansionOptions,
) ([]kb.ExpandedResult, error) {
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

	return buildExpandedResults(
		ctx,
		db,
		expandedResultInput{
			queryVec:     queryVec,
			topK:         topK,
			seeds:        seeds,
			entityScores: entityScores,
			alpha:        options.Alpha,
		},
	)
}

func mergeExpandedShardResults(shardResults [][]kb.ExpandedResult, topK int) []kb.ExpandedResult {
	if topK <= 0 {
		return []kb.ExpandedResult{}
	}
	return graph.MergeShardResults(shardResults, topK)
}

func seedEntityScores(ctx context.Context, db *sql.DB, seeds []kb.QueryResult) (map[string]float64, error) {
	seedDocIDs := graph.SeedDocIDs(seeds)
	seedEntities, err := graph.QueryEntitiesForDocs(ctx, db, seedDocIDs)
	if err != nil {
		return nil, err
	}
	entityScores := make(map[string]float64, len(seedEntities))
	maps.Copy(entityScores, seedEntities)
	return entityScores, nil
}

func expandEntityScores(
	ctx context.Context,
	db *sql.DB,
	scores map[string]float64,
	options kb.ExpansionOptions,
) (map[string]float64, error) {
	if options.Hops <= 0 || len(scores) == 0 {
		return scores, nil
	}
	if options.UseDuckPGQ {
		return expandEntityScoresDuckPGQ(ctx, db, scores, options)
	}
	return expandEntityScoresBFS(ctx, db, scores, options)
}

func expandEntityScoresBFS(
	ctx context.Context,
	db *sql.DB,
	scores map[string]float64,
	options kb.ExpansionOptions,
) (map[string]float64, error) {
	frontier := graph.CopyFloatMap(scores)
	for hop := 0; hop < options.Hops; hop++ {
		sources := kb.MapKeys(frontier)
		if len(sources) == 0 {
			break
		}

		edges, err := graph.QueryEdgesBySources(ctx, db, sources, options.EdgeTypes, options.MaxNeighborsPerNode)
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
	docGraphScore, err := graph.QueryDocsForEntities(ctx, db, input.entityScores)
	if err != nil {
		return nil, err
	}

	candidateIDs := graph.CandidateIDs(input.seeds, docGraphScore)
	docMatches, err := queryDocMatchesForIDs(ctx, db, input.queryVec, candidateIDs)
	if err != nil {
		return nil, err
	}
	if len(docMatches) == 0 {
		return []kb.ExpandedResult{}, nil
	}

	return graph.BuildExpandedResults(
		graph.ExpandInput{
			TopK:          input.topK,
			Seeds:         input.seeds,
			DocMatches:    convertDocMatches(docMatches),
			DocGraphScore: docGraphScore,
			Alpha:         input.alpha,
		},
	), nil
}

func convertDocMatches(matches map[string]docMatch) map[string]graph.DocMatch {
	out := make(map[string]graph.DocMatch, len(matches))
	for id, match := range matches {
		out[id] = graph.DocMatch{Content: match.Content, Distance: match.Distance, MediaRefs: match.MediaRefs}
	}
	return out
}
