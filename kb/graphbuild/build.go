package graphbuild

import (
	"context"
	"fmt"
)

type Document struct {
	ID   string
	Text string
}

type Chunker interface {
	Chunk(ctx context.Context, docID string, text string) ([]Chunk, error)
}

type Sink interface {
	EnsureGraphTables(ctx context.Context) error
	InsertGraphBuildResult(ctx context.Context, result *BuildResult) error
}

type Builder struct {
	Chunker       Chunker
	Grapher       Grapher
	Canonicalizer Canonicalizer
	BatchSize     int
}

type accumulator struct {
	builder                    *Builder
	ctx                        context.Context
	sink                       func(context.Context, *BuildResult) error
	entityMap                  map[string]Entity
	allEdges                   []Edge
	allChunks                  []Chunk
	entityChunkMappings        []EntityChunkMapping
	pendingEntities            []Entity
	pendingEntityChunkMappings []EntityChunkMapping
}

func (b *Builder) Build(ctx context.Context, docs []Document) (*BuildResult, error) {
	return b.build(ctx, docs, nil)
}

func (b *Builder) BuildAndInsert(ctx context.Context, sink Sink, docs []Document) (*BuildResult, error) {
	if sink == nil {
		return nil, fmt.Errorf("graph sink is nil")
	}
	if err := sink.EnsureGraphTables(ctx); err != nil {
		return nil, err
	}
	return b.build(ctx, docs, sink.InsertGraphBuildResult)
}

func (b *Builder) build(
	ctx context.Context,
	docs []Document,
	sink func(context.Context, *BuildResult) error,
) (*BuildResult, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	acc := &accumulator{builder: b, ctx: ctx, sink: sink, entityMap: map[string]Entity{}}
	if err := acc.processDocuments(docs); err != nil {
		return nil, err
	}
	return acc.result(), nil
}

func (b *Builder) validate() error {
	if b == nil {
		return fmt.Errorf("graph builder is nil")
	}
	if b.Chunker == nil {
		return fmt.Errorf("graph builder requires Chunker")
	}
	if b.Grapher == nil {
		return fmt.Errorf("graph builder requires Grapher")
	}
	return nil
}

func (a *accumulator) processDocuments(docs []Document) error {
	var batch []Chunk
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
				batch = nil
			}
		}
	}
	if len(batch) > 0 {
		return a.extractAndFlush(batch)
	}
	return nil
}

func (a *accumulator) batchSize() int {
	if a.builder.BatchSize <= 0 {
		return 500
	}
	return a.builder.BatchSize
}

func (a *accumulator) extractAndFlush(chunks []Chunk) error {
	extraction, err := a.builder.Grapher.Extract(a.ctx, chunks)
	if err != nil {
		return err
	}
	return a.flush(chunks, extraction)
}

func (a *accumulator) flush(chunks []Chunk, extraction *Extraction) error {
	if extraction == nil {
		extraction = &Extraction{}
	}
	a.allChunks = append(a.allChunks, chunks...)
	if err := a.recordEntities(extraction.Entities); err != nil {
		return err
	}
	edges, err := a.graphEdges(extraction.Edges)
	if err != nil {
		return err
	}
	a.allEdges = append(a.allEdges, edges...)
	return a.flushSink(chunks, edges)
}

func (a *accumulator) recordEntities(entities []EntityCandidate) error {
	for _, ent := range entities {
		if err := a.recordEntity(ent); err != nil {
			return err
		}
	}
	return nil
}

func (a *accumulator) recordEntity(ent EntityCandidate) error {
	id, err := a.canonicalize(ent.Name)
	if err != nil {
		return err
	}
	if _, ok := a.entityMap[id]; !ok {
		entity := Entity{ID: id, Name: ent.Name}
		a.entityMap[id] = entity
		a.pendingEntities = append(a.pendingEntities, entity)
	}
	mapping := EntityChunkMapping{EntityID: id, ChunkID: ent.ChunkID}
	a.entityChunkMappings = append(a.entityChunkMappings, mapping)
	a.pendingEntityChunkMappings = append(a.pendingEntityChunkMappings, mapping)
	return nil
}

func (a *accumulator) graphEdges(candidates []EdgeCandidate) ([]Edge, error) {
	edges := make([]Edge, 0, len(candidates))
	for _, candidate := range candidates {
		edge, ok, err := a.graphEdge(candidate)
		if err != nil {
			return nil, err
		}
		if ok {
			edges = append(edges, edge)
		}
	}
	return edges, nil
}

func (a *accumulator) graphEdge(edge EdgeCandidate) (Edge, bool, error) {
	srcID, err := a.canonicalize(edge.Src)
	if err != nil {
		return Edge{}, false, err
	}
	dstID, err := a.canonicalize(edge.Dst)
	if err != nil {
		return Edge{}, false, err
	}
	if srcID == "" || dstID == "" || srcID == dstID {
		return Edge{}, false, nil
	}
	return Edge{
		SrcID:   srcID,
		DstID:   dstID,
		RelType: edge.RelType,
		Weight:  edge.Weight,
		ChunkID: edge.ChunkID,
	}, true, nil
}

func (a *accumulator) canonicalize(name string) (string, error) {
	if a.builder.Canonicalizer == nil {
		return name, nil
	}
	return a.builder.Canonicalizer.Canonicalize(a.ctx, name)
}

func (a *accumulator) flushSink(chunks []Chunk, edges []Edge) error {
	if a.sink == nil {
		return nil
	}
	batch := &BuildResult{
		Chunks:              chunks,
		Entities:            a.pendingEntities,
		Edges:               edges,
		EntityChunkMappings: a.pendingEntityChunkMappings,
	}
	if err := a.sink(a.ctx, batch); err != nil {
		return err
	}
	a.pendingEntities = nil
	a.pendingEntityChunkMappings = nil
	return nil
}

func (a *accumulator) result() *BuildResult {
	entities := make([]Entity, 0, len(a.entityMap))
	for _, ent := range a.entityMap {
		entities = append(entities, ent)
	}
	return &BuildResult{
		Chunks:              a.allChunks,
		Entities:            entities,
		Edges:               a.allEdges,
		EntityChunkMappings: a.entityChunkMappings,
	}
}
