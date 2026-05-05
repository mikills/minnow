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
	opts = normalizeCodeSearchOptions(opts)
	manifest, _, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return nil, err
	}
	vec, err := k.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	results, err := k.Search(ctx, kbID, vec, &SearchOptions{TopK: codeSearchFanout(opts.TopK)})
	if err != nil {
		return nil, err
	}
	return filterCodeSearchResults(results, manifest, opts), nil
}

func normalizeCodeSearchOptions(opts CodeSearchOptions) CodeSearchOptions {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	return opts
}

func codeSearchFanout(topK int) int {
	searchK := topK * 20
	if searchK < 200 {
		return 200
	}
	return searchK
}

func filterCodeSearchResults(results []ExpandedResult, manifest codeIndexManifest, opts CodeSearchOptions) []CodeSearchResult {
	out := make([]CodeSearchResult, 0, opts.TopK)
	pathFilter := strings.TrimSpace(opts.Path)
	langFilter := strings.ToLower(strings.TrimSpace(opts.Language))
	for _, result := range results {
		if appendCodeSearchResult(&out, result, codeSearchFilter{manifest: manifest, path: pathFilter, language: langFilter, topK: opts.TopK}) {
			break
		}
	}
	return out
}

type codeSearchFilter struct {
	manifest codeIndexManifest
	path     string
	language string
	topK     int
}

func appendCodeSearchResult(out *[]CodeSearchResult, result ExpandedResult, filter codeSearchFilter) bool {
	meta, ok := filter.manifest.Chunks[result.ID]
	if !ok || !codeSearchMetaMatches(meta, filter.path, filter.language) {
		return false
	}
	*out = append(*out, CodeSearchResult{ID: result.ID, Content: result.Content, Distance: result.Distance, Path: meta.Path, Language: meta.Language, Symbol: meta.Symbol, Kind: meta.Kind, StartLine: meta.StartLine, EndLine: meta.EndLine})
	return len(*out) >= filter.topK
}

func codeSearchMetaMatches(meta CodeChunkMetadata, pathFilter string, langFilter string) bool {
	if pathFilter != "" && !strings.Contains(meta.Path, pathFilter) {
		return false
	}
	if langFilter != "" && strings.ToLower(meta.Language) != langFilter {
		return false
	}
	return true
}
