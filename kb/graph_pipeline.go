package kb

import (
	"context"
	"fmt"
)

// GraphSink persists graph build results incrementally. Implementations
// must create any required tables in EnsureGraphTables and write batch
// results in InsertGraphBuildResult.
type GraphSink interface {
	EnsureGraphTables(ctx context.Context) error
	InsertGraphBuildResult(ctx context.Context, result *GraphBuildResult) error
}

// EntityCandidate is a raw entity name extracted by a Grapher.
type EntityCandidate struct {
	Name    string
	ChunkID string
}

// EdgeCandidate is a raw edge between entity names.
type EdgeCandidate struct {
	Src     string
	Dst     string
	RelType string
	Weight  float64
	ChunkID string
}

// GraphExtraction is the output of a Grapher before canonicalization.
type GraphExtraction struct {
	Entities []EntityCandidate
	Edges    []EdgeCandidate
}

// Grapher extracts entities and edges from chunks.
type Grapher interface {
	Extract(ctx context.Context, chunks []Chunk) (*GraphExtraction, error)
}

// Canonicalizer maps entity names to canonical IDs.
type Canonicalizer interface {
	Canonicalize(ctx context.Context, name string) (string, error)
}

// GraphEntity is a canonicalized entity.
type GraphEntity struct {
	ID   string
	Name string
}

// GraphEdge is a canonicalized edge between entities.
type GraphEdge struct {
	SrcID   string
	DstID   string
	RelType string
	Weight  float64
	ChunkID string
}

// EntityChunkMapping links a standalone entity to its source chunk,
// enabling doc_entities rows for entities that have no edges.
type EntityChunkMapping struct {
	EntityID string
	ChunkID  string
}

// GraphBuildResult contains the intermediate chunks and canonicalized graph output.
type GraphBuildResult struct {
	Chunks              []Chunk
	Entities            []GraphEntity
	Edges               []GraphEdge
	EntityChunkMappings []EntityChunkMapping
}

// GraphBuilder coordinates chunking, graph extraction, and canonicalization.
// BatchSize controls how many chunks are sent to Grapher.Extract per call.
// it defaults to 500 when <= 0. Canonicalizer is optional. when nil, raw
// entity names are used as IDs directly.
type GraphBuilder struct {
	Chunker       Chunker
	Grapher       Grapher
	Canonicalizer Canonicalizer
	BatchSize     int
}

// buildGraph runs the chunk → extract → canonicalize pipeline over docs.
//
// For each batch of chunks it calls Grapher.Extract, canonicalizes entity names
// and edge endpoints, deduplicates entities by ID across batches via entityMap,
// and records an EntityChunkMapping for every entity so doc_entities rows can
// be built even for entities that have no edges.
//
// If sink is non-nil it is called after each completed batch with the
// incremental result so the caller can persist without accumulating the full
// result in memory. pendingEntities is reset after each sink call to avoid
// re-inserting entities that were already flushed.
type graphBuildAccumulator struct {
	builder                    *GraphBuilder
	ctx                        context.Context
	sink                       func(context.Context, *GraphBuildResult) error
	entityMap                  map[string]GraphEntity
	allEdges                   []GraphEdge
	allChunks                  []Chunk
	entityChunkMappings        []EntityChunkMapping
	pendingEntities            []GraphEntity
	pendingEntityChunkMappings []EntityChunkMapping
}

func (b *GraphBuilder) buildGraph(ctx context.Context, docs []Document, sink func(context.Context, *GraphBuildResult) error) (*GraphBuildResult, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	acc := newGraphBuildAccumulator(ctx, b, sink)
	if err := acc.processDocuments(docs); err != nil {
		return nil, err
	}
	return acc.result(), nil
}

func (b *GraphBuilder) validate() error {
	if b.Chunker == nil {
		return fmt.Errorf("chunker is required")
	}
	if b.Grapher == nil {
		return fmt.Errorf("grapher is required")
	}
	return nil
}

func newGraphBuildAccumulator(ctx context.Context, builder *GraphBuilder, sink func(context.Context, *GraphBuildResult) error) *graphBuildAccumulator {
	return &graphBuildAccumulator{builder: builder, ctx: ctx, sink: sink, entityMap: map[string]GraphEntity{}}
}

func (a *graphBuildAccumulator) processDocuments(docs []Document) error {
	batch := make([]Chunk, 0, a.batchSize())
	for _, doc := range docs {
		chunks, err := a.builder.Chunker.Chunk(a.ctx, doc.ID, doc.Text)
		if err != nil {
			return err
		}
		for _, chunk := range chunks {
			batch = append(batch, chunk)
			if len(batch) >= a.batchSize() {
				if err := a.extractAndFlush(batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}
	if len(batch) > 0 {
		return a.extractAndFlush(batch)
	}
	return nil
}

func (a *graphBuildAccumulator) batchSize() int {
	if a.builder.BatchSize > 0 {
		return a.builder.BatchSize
	}
	return 500
}

func (a *graphBuildAccumulator) extractAndFlush(chunks []Chunk) error {
	extraction, err := a.builder.Grapher.Extract(a.ctx, chunks)
	if err != nil {
		return err
	}
	return a.flush(chunks, extraction)
}

func (a *graphBuildAccumulator) flush(chunks []Chunk, extraction *GraphExtraction) error {
	if extraction == nil {
		return nil
	}
	batchEdges, err := a.graphEdges(extraction.Edges)
	if err != nil {
		return err
	}
	if err := a.recordEntities(extraction.Entities); err != nil {
		return err
	}
	a.allChunks = append(a.allChunks, chunks...)
	a.allEdges = append(a.allEdges, batchEdges...)
	if err := a.flushSink(chunks, batchEdges); err != nil {
		return err
	}
	a.pendingEntities = a.pendingEntities[:0]
	a.pendingEntityChunkMappings = a.pendingEntityChunkMappings[:0]
	return nil
}

func (a *graphBuildAccumulator) recordEntities(entities []EntityCandidate) error {
	for _, ent := range entities {
		if err := a.recordEntity(ent); err != nil {
			return err
		}
	}
	return nil
}

func (a *graphBuildAccumulator) recordEntity(ent EntityCandidate) error {
	if ent.Name == "" {
		return nil
	}
	id, err := a.canonicalize(ent.Name)
	if err != nil || id == "" {
		return err
	}
	if _, ok := a.entityMap[id]; !ok {
		entity := GraphEntity{ID: id, Name: ent.Name}
		a.entityMap[id] = entity
		a.pendingEntities = append(a.pendingEntities, entity)
	}
	if ent.ChunkID != "" {
		mapping := EntityChunkMapping{EntityID: id, ChunkID: ent.ChunkID}
		a.entityChunkMappings = append(a.entityChunkMappings, mapping)
		a.pendingEntityChunkMappings = append(a.pendingEntityChunkMappings, mapping)
	}
	return nil
}

func (a *graphBuildAccumulator) graphEdges(candidates []EdgeCandidate) ([]GraphEdge, error) {
	edges := make([]GraphEdge, 0, len(candidates))
	for _, edge := range candidates {
		graphEdge, ok, err := a.graphEdge(edge)
		if err != nil {
			return nil, err
		}
		if ok {
			edges = append(edges, graphEdge)
		}
	}
	return edges, nil
}

func (a *graphBuildAccumulator) graphEdge(edge EdgeCandidate) (GraphEdge, bool, error) {
	if edge.Src == "" || edge.Dst == "" {
		return GraphEdge{}, false, nil
	}
	srcID, err := a.canonicalize(edge.Src)
	if err != nil {
		return GraphEdge{}, false, err
	}
	dstID, err := a.canonicalize(edge.Dst)
	if err != nil || srcID == "" || dstID == "" {
		return GraphEdge{}, false, err
	}
	weight := edge.Weight
	if weight <= 0 {
		weight = 1.0
	}
	return GraphEdge{SrcID: srcID, DstID: dstID, RelType: edge.RelType, Weight: weight, ChunkID: edge.ChunkID}, true, nil
}

func (a *graphBuildAccumulator) canonicalize(name string) (string, error) {
	if a.builder.Canonicalizer == nil {
		return name, nil
	}
	return a.builder.Canonicalizer.Canonicalize(a.ctx, name)
}

func (a *graphBuildAccumulator) flushSink(chunks []Chunk, edges []GraphEdge) error {
	if a.sink == nil {
		return nil
	}
	return a.sink(a.ctx, &GraphBuildResult{Chunks: chunks, Entities: a.pendingEntities, Edges: edges, EntityChunkMappings: a.pendingEntityChunkMappings})
}

func (a *graphBuildAccumulator) result() *GraphBuildResult {
	entities := make([]GraphEntity, 0, len(a.entityMap))
	for _, ent := range a.entityMap {
		entities = append(entities, ent)
	}
	return &GraphBuildResult{Chunks: a.allChunks, Entities: entities, Edges: a.allEdges, EntityChunkMappings: a.entityChunkMappings}
}

// Build runs the full pipeline and returns the accumulated GraphBuildResult.
// The entire result is held in memory. use BuildAndInsert for large document
// sets where incremental persistence is preferred.
func (b *GraphBuilder) Build(ctx context.Context, docs []Document) (*GraphBuildResult, error) {
	return b.buildGraph(ctx, docs, nil)
}

// BuildAndInsert runs the pipeline and persists each batch via the given
// GraphSink as it completes, keeping peak memory proportional to BatchSize.
// Graph tables are created if they do not already exist before the first
// batch is written.
func (b *GraphBuilder) BuildAndInsert(ctx context.Context, sink GraphSink, docs []Document) (*GraphBuildResult, error) {
	if sink == nil {
		return nil, fmt.Errorf("graph sink is required")
	}

	if err := sink.EnsureGraphTables(ctx); err != nil {
		return nil, err
	}

	return b.buildGraph(ctx, docs, func(ctx context.Context, batch *GraphBuildResult) error {
		return sink.InsertGraphBuildResult(ctx, batch)
	})
}
