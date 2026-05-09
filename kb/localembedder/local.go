package localembedder

import (
	"context"
	"fmt"

	"github.com/mikills/minnow/kb/internal/localembed"
	"github.com/mikills/minnow/kb/openaiembedder"
)

const (
	defaultMinNgram = 3
	defaultMaxNgram = 6
)

type Embedder struct {
	dim      int
	minNgram int
	maxNgram int
	dimErr   error
}

func New(dim int, dimErr error) (*Embedder, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("%w: got %d", dimErr, dim)
	}
	return &Embedder{dim: dim, minNgram: defaultMinNgram, maxNgram: defaultMaxNgram, dimErr: dimErr}, nil
}

func (e *Embedder) Embed(_ context.Context, input string) ([]float32, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	return localembed.Vector(input, e.dim, e.minNgram, e.maxNgram)
}

func (e *Embedder) validate() error {
	if e == nil {
		return fmt.Errorf("embedder is nil")
	}
	if e.dim <= 0 {
		dimErr := e.dimErr
		if dimErr == nil {
			dimErr = openaiembedder.ErrInvalidEmbeddingDimension
		}
		return fmt.Errorf("%w: got %d", dimErr, e.dim)
	}
	if e.minNgram <= 0 || e.maxNgram <= 0 || e.minNgram > e.maxNgram {
		return fmt.Errorf("invalid n-gram range: min=%d max=%d", e.minNgram, e.maxNgram)
	}
	return nil
}
