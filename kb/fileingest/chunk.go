package fileingest

import (
	"context"
	"fmt"

	"github.com/mikills/minnow/kb/graphbuild"
	"github.com/mikills/minnow/kb/media"
)

type Chunker interface {
	Chunk(ctx context.Context, docID string, text string) ([]graphbuild.Chunk, error)
}

type ChunkedDocument struct {
	ID        string
	Text      string
	MediaRefs []media.ChunkMediaRef
	Metadata  map[string]any
}

func ChunkPages(ctx context.Context, src Source, pages []Page, chunker Chunker) ([]ChunkedDocument, int, error) {
	out := make([]ChunkedDocument, 0)
	for _, page := range pages {
		chunks, err := chunker.Chunk(ctx, fmt.Sprintf("%s-page-%d", src.DocumentID, page.Number), page.Text)
		if err != nil {
			return nil, 0, err
		}
		for _, chunk := range chunks {
			out = append(out, ChunkedDocument{
				ID:   chunk.ChunkID,
				Text: chunk.Text,
				MediaRefs: []media.ChunkMediaRef{{
					MediaID:  src.MediaID,
					Label:    src.Filename,
					Locator:  &media.MediaLocator{Page: page.Number},
					Metadata: CloneMap(src.Metadata),
				}},
				Metadata: CloneMap(src.Metadata),
			})
		}
	}
	return out, len(pages), nil
}
