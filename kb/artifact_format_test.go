package kb

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestResolveFormat(t *testing.T) {
	formatA := &stubArtifactFormat{kind: "format-a"}
	formatB := &stubArtifactFormat{kind: "format-b"}

	tests := []struct {
		name       string
		opts       []KBOption
		store      ManifestStore
		kbID       string
		wantFormat ArtifactFormat
	}{
		{
			name: "default_when_missing",
			opts: []KBOption{
				WithFormats(formatB),
				WithArtifactFormat(formatA),
			},
			store: &stubManifestStore{
				docs: map[string]*ManifestDocument{
					"existing": {
						Manifest: SnapshotShardManifest{
							KBID:       "existing",
							FormatKind: formatB.Kind(),
						},
						Version: "v1",
					},
				},
			},
			kbID:       "missing",
			wantFormat: formatB,
		},
		{
			name: "manifest_kind_match",
			opts: []KBOption{
				WithFormats(formatB),
				WithArtifactFormat(formatA),
			},
			store: &stubManifestStore{
				docs: map[string]*ManifestDocument{
					"existing": {
						Manifest: SnapshotShardManifest{
							KBID:       "existing",
							FormatKind: formatB.Kind(),
						},
						Version: "v1",
					},
				},
			},
			kbID:       "existing",
			wantFormat: formatB,
		},
		{
			name: "artifact_then_formats",
			opts: []KBOption{
				WithArtifactFormat(formatA),
				WithFormats(formatB),
			},
			kbID:       "missing",
			wantFormat: formatA,
		},
		{
			name: "formats_then_artifact",
			opts: []KBOption{
				WithFormats(formatB),
				WithArtifactFormat(formatA),
			},
			kbID:       "missing",
			wantFormat: formatB,
		},
		{
			name: "wrapped_not_found",
			opts: []KBOption{
				WithArtifactFormat(formatA),
			},
			store: &stubManifestStore{
				getErr: fmt.Errorf("wrapped: %w", ErrManifestNotFound),
			},
			kbID:       "missing",
			wantFormat: formatA,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts
			if tc.store != nil {
				opts = append(opts, WithManifestStore(tc.store))
			}

			kb := NewKB(
				&LocalBlobStore{Root: t.TempDir()},
				t.TempDir(),
				opts...,
			)

			resolved, err := kb.resolveFormat(context.Background(), tc.kbID)
			if err != nil {
				t.Fatalf("resolveFormat(%q): %v", tc.kbID, err)
			}
			if resolved != tc.wantFormat {
				t.Fatalf("format mismatch: got %T %v, want %T %v", resolved, resolved, tc.wantFormat, tc.wantFormat)
			}
		})
	}
}

func TestResolveFormatByKindMismatch(t *testing.T) {
	formatA := &stubArtifactFormat{kind: "format-a"}
	kb := NewKB(
		&LocalBlobStore{Root: t.TempDir()},
		t.TempDir(),
		WithArtifactFormat(formatA),
	)

	_, err := kb.resolveFormatByKind("format-b")
	if !errors.Is(err, ErrFormatNotRegistered) {
		t.Fatalf("expected ErrFormatNotRegistered, got %v", err)
	}
}

func TestRegisterFormatValidation(t *testing.T) {
	tests := []struct {
		name   string
		format ArtifactFormat
	}{
		{
			name:   "nil_format",
			format: nil,
		},
		{
			name:   "empty_kind",
			format: &stubArtifactFormat{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir())

			err := kb.RegisterFormat(tc.format)
			if !errors.Is(err, ErrInvalidArtifactFormat) {
				t.Fatalf("expected ErrInvalidArtifactFormat, got %v", err)
			}
			if kb.HasFormat() {
				t.Fatal("expected registry to remain empty")
			}
		})
	}
}

func TestOptionRegistrationErrorsSurfaceAtResolve(t *testing.T) {
	kb := NewKB(
		&LocalBlobStore{Root: t.TempDir()},
		t.TempDir(),
		WithArtifactFormat(nil),
	)

	_, err := kb.resolveFormat(context.Background(), "missing")
	if !errors.Is(err, ErrInvalidArtifactFormat) {
		t.Fatalf("expected ErrInvalidArtifactFormat, got %v", err)
	}
}

type stubArtifactFormat struct {
	kind string
}

func (f *stubArtifactFormat) Kind() string { return f.kind }

func (f *stubArtifactFormat) Version() int { return 1 }

func (f *stubArtifactFormat) FileExt() string { return ".stub" }

func (f *stubArtifactFormat) BuildArtifacts(context.Context, string, string, int64) ([]SnapshotShardMetadata, error) {
	return nil, nil
}

func (f *stubArtifactFormat) QueryRag(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}

func (f *stubArtifactFormat) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}

func (f *stubArtifactFormat) Ingest(context.Context, IngestUpsertRequest) (IngestResult, error) {
	return IngestResult{}, nil
}

func (f *stubArtifactFormat) Delete(context.Context, IngestDeleteRequest) (IngestResult, error) {
	return IngestResult{}, nil
}

type stubManifestStore struct {
	docs   map[string]*ManifestDocument
	getErr error
}

func (s *stubManifestStore) Get(_ context.Context, kbID string) (*ManifestDocument, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	if doc, ok := s.docs[kbID]; ok {
		return doc, nil
	}
	return nil, ErrManifestNotFound
}

func (s *stubManifestStore) HeadVersion(context.Context, string) (string, error) {
	return "", nil
}

func (s *stubManifestStore) UpsertIfMatch(_ context.Context, kbID string, manifest SnapshotShardManifest, expectedVersion string) (string, error) {
	if s.docs == nil {
		s.docs = make(map[string]*ManifestDocument)
	}
	version := expectedVersion
	if version == "" {
		version = "v1"
	}
	s.docs[kbID] = &ManifestDocument{Manifest: manifest, Version: version}
	return version, nil
}

func (s *stubManifestStore) Delete(_ context.Context, kbID string) error {
	delete(s.docs, kbID)
	return nil
}
