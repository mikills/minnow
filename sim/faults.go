package sim

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	kb "github.com/mikills/minnow/kb"
)

// BlobFaults configures injected failures for FaultableBlobStore. Rates are
// 0.0 (never) to 1.0 (always). Zero values mean "no fault".
type BlobFaults struct {
	UploadFailRate   float64
	DownloadFailRate float64
	HeadFailRate     float64
	DeleteFailRate   float64
	ListFailRate     float64
}

// ErrInjected is returned by the faultable store when a random roll triggers
// a failure. Scenarios can test for this to assert retry behaviour.
var ErrInjected = errors.New("sim: injected fault")

// FaultableBlobStore wraps a kb.BlobStore with a seeded RNG that decides
// whether each operation should fail. All underlying I/O still hits the inner
// store when the roll passes.
type FaultableBlobStore struct {
	inner  kb.BlobStore
	mu     sync.Mutex
	faults BlobFaults
	rng    *rand.Rand
}

// NewFaultableBlobStore wraps inner with seeded fault injection.
func NewFaultableBlobStore(inner kb.BlobStore, faults BlobFaults, rng *rand.Rand) *FaultableBlobStore {
	if rng == nil {
		rng = rand.New(rand.NewSource(1))
	}
	return &FaultableBlobStore{inner: inner, faults: faults, rng: rng}
}

// SetFaults replaces the active fault configuration mid-run.
func (s *FaultableBlobStore) SetFaults(f BlobFaults) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = f
}

func (s *FaultableBlobStore) shouldFail(rate float64) bool {
	if rate <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Float64() < rate
}

func (s *FaultableBlobStore) Head(ctx context.Context, key string) (*kb.BlobObjectInfo, error) {
	if s.shouldFail(s.faults.HeadFailRate) {
		return nil, ErrInjected
	}
	return s.inner.Head(ctx, key)
}

func (s *FaultableBlobStore) Download(ctx context.Context, key string, dest string) error {
	if s.shouldFail(s.faults.DownloadFailRate) {
		return ErrInjected
	}
	return s.inner.Download(ctx, key, dest)
}

func (s *FaultableBlobStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*kb.BlobObjectInfo, error) {
	if s.shouldFail(s.faults.UploadFailRate) {
		return nil, ErrInjected
	}
	return s.inner.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (s *FaultableBlobStore) Delete(ctx context.Context, key string) error {
	if s.shouldFail(s.faults.DeleteFailRate) {
		return ErrInjected
	}
	return s.inner.Delete(ctx, key)
}

func (s *FaultableBlobStore) List(ctx context.Context, prefix string) ([]kb.BlobObjectInfo, error) {
	if s.shouldFail(s.faults.ListFailRate) {
		return nil, ErrInjected
	}
	return s.inner.List(ctx, prefix)
}
