package kb

import (
	"context"
	"fmt"
	"strings"
)

func (k *KB) CodeIndexStatus(ctx context.Context, kbID string) (CodeIndexStatus, error) {
	if strings.TrimSpace(kbID) == "" {
		kbID = defaultKBIDForIndexKey("default")
	}
	manifest, version, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return CodeIndexStatus{}, err
	}
	if version == "" {
		return CodeIndexStatus{KBID: kbID, Indexed: false, ManifestKey: codeIndexManifestKey(kbID)}, nil
	}
	chunkCount := 0
	for _, file := range manifest.Files {
		chunkCount += len(file.ChunkIDs)
	}
	updatedAt := manifest.UpdatedAt
	return CodeIndexStatus{KBID: kbID, Indexed: true, Root: manifest.Root, RepoID: manifest.RepoID, UpdatedAt: &updatedAt, FileCount: len(manifest.Files), ChunkCount: chunkCount, ManifestKey: codeIndexManifestKey(kbID)}, nil
}

func (k *KB) SearchCode(ctx context.Context, kbID, query string, opts CodeSearchOptions) ([]CodeSearchResult, error) {
	if strings.TrimSpace(kbID) == "" {
		kbID = defaultKBIDForIndexKey("default")
	}
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query is required")
	}
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	manifest, _, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return nil, err
	}
	vec, err := k.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	searchK := opts.TopK * 20
	if searchK < 200 {
		searchK = 200
	}
	results, err := k.Search(ctx, kbID, vec, &SearchOptions{TopK: searchK})
	if err != nil {
		return nil, err
	}
	out := make([]CodeSearchResult, 0, opts.TopK)
	pathFilter := strings.TrimSpace(opts.Path)
	langFilter := strings.ToLower(strings.TrimSpace(opts.Language))
	for _, r := range results {
		meta, ok := manifest.Chunks[r.ID]
		if !ok {
			continue
		}
		if pathFilter != "" && !strings.Contains(meta.Path, pathFilter) {
			continue
		}
		if langFilter != "" && strings.ToLower(meta.Language) != langFilter {
			continue
		}
		out = append(out, CodeSearchResult{ID: r.ID, Content: r.Content, Distance: r.Distance, Path: meta.Path, Language: meta.Language, Symbol: meta.Symbol, Kind: meta.Kind, StartLine: meta.StartLine, EndLine: meta.EndLine})
		if len(out) >= opts.TopK {
			break
		}
	}
	return out, nil
}
