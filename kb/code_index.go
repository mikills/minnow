package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	DefaultCodeChunkSize    = 1200
	DefaultCodeChunkOverlap = 120
	DefaultCodeMaxFileBytes = 1024 * 1024
	CodeIndexManifestSchema = "minnow.code_index/v1"
)

var defaultCodeExcludePatterns = []string{
	".git/**",
	".minnow/**",
	"node_modules/**",
	"vendor/**",
	"**/vendor/**",
	"dist/**",
	"build/**",
	"coverage/**",
	"fixtures/**",
	"**/fixtures/**",
	"data/**",
	"**/data/**",
	".next/**",
	".turbo/**",
	"target/**",
	"extensions/**",
	"examples/extensions/**",
	"**/*.duckdb",
	"**/*.duckdb_extension",
	"**/*.parquet",
	"**/*.min.js",
	"**/*.min.css",
	"**/*.map",
	"*.lock",
	".gitignore",
}

var defaultCodeIncludePatterns = []string{
	"**/*.go",
	"**/*.js",
	"**/*.jsx",
	"**/*.ts",
	"**/*.tsx",
	"**/*.mjs",
	"**/*.cjs",
	"**/*.py",
	"**/*.rs",
	"**/*.java",
	"**/*.rb",
	"**/*.php",
	"**/*.c",
	"**/*.cc",
	"**/*.cpp",
	"**/*.h",
	"**/*.hpp",
	"**/*.cs",
	"**/*.swift",
	"**/*.kt",
	"**/*.kts",
	"**/*.sh",
	"**/*.bash",
	"**/*.zsh",
	"**/*.md",
	"**/*.mdx",
	"**/*.yaml",
	"**/*.yml",
	"**/*.json",
	"**/*.toml",
	"**/*.xml",
	"**/Dockerfile",
}

type CodeIndexOptions struct {
	KBID             string
	IndexKey         string
	Description      string
	Root             string
	Include          []string
	Exclude          []string
	MaxFileBytes     int64
	ChunkSize        int
	ChunkOverlap     int
	IncludeUntracked bool
	EmbedBatchSize   int
	MaxBatchBytes    int
	Throttle         time.Duration
	MaxHeapBytes     uint64
	MaxRSSBytes      uint64
	LargeRepoFiles   int
	RequireConfirm   bool
	ConfirmedLarge   bool
}

type CodeIndexResult struct {
	KBID           string `json:"kb_id"`
	IndexKey       string `json:"index_key"`
	Description    string `json:"description,omitempty"`
	Root           string `json:"root"`
	ScannedFiles   int    `json:"scanned_files"`
	SkippedFiles   int    `json:"skipped_files"`
	IndexedFiles   int    `json:"indexed_files"`
	DeletedFiles   int    `json:"deleted_files"`
	UnchangedFiles int    `json:"unchanged_files"`
	ChunksIndexed  int    `json:"chunks_indexed"`
	ChunksDeleted  int    `json:"chunks_deleted"`
	ManifestKey    string `json:"manifest_key"`
}

type CodeIndexStatus struct {
	KBID        string     `json:"kb_id"`
	IndexKey    string     `json:"index_key,omitempty"`
	Description string     `json:"description,omitempty"`
	Indexed     bool       `json:"indexed"`
	Root        string     `json:"root,omitempty"`
	RepoID      string     `json:"repo_id,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	FileCount   int        `json:"file_count"`
	ChunkCount  int        `json:"chunk_count"`
	ManifestKey string     `json:"manifest_key"`
}

type CodeSearchOptions struct {
	TopK     int
	Path     string
	Language string
}

type CodeSearchResult struct {
	ID        string  `json:"id"`
	Content   string  `json:"content"`
	Distance  float64 `json:"distance"`
	Path      string  `json:"path,omitempty"`
	Language  string  `json:"language,omitempty"`
	Symbol    string  `json:"symbol,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	StartLine int     `json:"start_line,omitempty"`
	EndLine   int     `json:"end_line,omitempty"`
}

var ErrCodeIndexRequiresConfirmation = errors.New("code index requires confirmation for large repository")

type codeIndexManifest struct {
	SchemaVersion string                       `json:"schema_version"`
	KBID          string                       `json:"kb_id"`
	Root          string                       `json:"root"`
	RepoID        string                       `json:"repo_id"`
	UpdatedAt     time.Time                    `json:"updated_at"`
	Files         map[string]codeIndexedFile   `json:"files"`
	Chunks        map[string]CodeChunkMetadata `json:"chunks"`
}

type CodebaseIndexRegistry struct {
	SchemaVersion string                                `json:"schema_version"`
	Indexes       map[string]CodebaseIndexRegistryEntry `json:"codebase_indexes"`
}

type CodebaseIndexRegistryEntry struct {
	KBID             string `json:"kb_id"`
	Root             string `json:"root"`
	Description      string `json:"description,omitempty"`
	IncludeUntracked bool   `json:"include_untracked"`
}

type codeIndexedFile struct {
	Path      string   `json:"path"`
	Hash      string   `json:"hash"`
	SizeBytes int64    `json:"size_bytes"`
	Language  string   `json:"language,omitempty"`
	ChunkIDs  []string `json:"chunk_ids"`
}

type CodeChunkMetadata struct {
	ID        string `json:"id"`
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	Language  string `json:"language,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
	Kind      string `json:"kind,omitempty"`
	StartLine int    `json:"start_line"`
	EndLine   int    `json:"end_line"`
}

type codeScannedFile struct {
	AbsPath   string
	RelPath   string
	Hash      string
	SizeBytes int64
	Language  string
}

type codeChunk struct {
	Text      string
	Symbol    string
	Kind      string
	StartLine int
	EndLine   int
}

type resolvedCodeIndexTarget struct {
	Root         string
	RepoRoot     string
	RegistryRoot string
	RepoID       string
	KBID         string
	IndexKey     string
	Description  string
	Registry     CodebaseIndexRegistry
	Options      CodeIndexOptions
}

func (k *KB) IndexCodebase(ctx context.Context, opts CodeIndexOptions) (CodeIndexResult, error) {
	started := time.Now()
	if err := k.validateCodeIndexReady(); err != nil {
		return CodeIndexResult{}, err
	}
	target, err := resolveCodeIndexTarget(opts)
	if err != nil {
		return CodeIndexResult{}, err
	}
	slog.Default().InfoContext(ctx, "code index start", "kb_id", target.KBID, "index_key", target.IndexKey, "root", target.Root, "repo_root", target.RepoRoot, "include_untracked", target.Options.IncludeUntracked)

	manifest, version, err := k.loadPreparedCodeIndexManifest(ctx, target.KBID)
	if err != nil {
		return CodeIndexResult{}, err
	}
	scanned, skipped, err := scanCodebase(ctx, target.Root, target.Options)
	if err != nil {
		return CodeIndexResult{}, err
	}
	if err := validateCodeIndexConfirmation(target.Options, len(scanned)); err != nil {
		return CodeIndexResult{}, err
	}

	result, deleteIDs, nextFiles, nextChunks := diffCodeIndexManifest(target, manifest, scanned, skipped)
	if err := k.publishCodeIndexChanges(ctx, target, codeIndexPublishState{scanned: scanned, nextFiles: nextFiles, nextChunks: nextChunks, result: &result}); err != nil {
		return CodeIndexResult{}, err
	}
	if err := k.deleteRemovedCodeChunks(ctx, target.KBID, deleteIDs); err != nil {
		return CodeIndexResult{}, err
	}
	if err := k.saveCodeIndexState(ctx, target, nextFiles, nextChunks, version); err != nil {
		return CodeIndexResult{}, err
	}
	slog.Default().InfoContext(ctx, "code index complete", "kb_id", target.KBID, "indexed_files", result.IndexedFiles, "unchanged_files", result.UnchangedFiles, "chunks_indexed", result.ChunksIndexed, "total_duration_ms", time.Since(started).Milliseconds())
	return result, nil
}

func (k *KB) validateCodeIndexReady() error {
	if k == nil || k.BlobStore == nil {
		return errors.New("code index: BlobStore not configured")
	}
	return nil
}

func validateCodeIndexConfirmation(opts CodeIndexOptions, scanned int) error {
	if opts.RequireConfirm && !opts.ConfirmedLarge && scanned > opts.LargeRepoFiles {
		return fmt.Errorf("%w: scanned %d files exceeds threshold %d; rerun with confirmation or lower the threshold", ErrCodeIndexRequiresConfirmation, scanned, opts.LargeRepoFiles)
	}
	return nil
}

func resolveCodeIndexTarget(opts CodeIndexOptions) (resolvedCodeIndexTarget, error) {
	root, err := resolveRequestedCodeRoot(opts.Root)
	if err != nil {
		return resolvedCodeIndexTarget{}, err
	}
	repoRoot, err := resolveCodeRoot(root)
	if err != nil {
		return resolvedCodeIndexTarget{}, err
	}
	opts = normalizeCodeIndexOptions(opts)
	registry, err := loadCodebaseIndexRegistry(repoRoot)
	if err != nil {
		return resolvedCodeIndexTarget{}, err
	}
	entry, hasEntry := registry.Indexes[opts.IndexKey]
	root = resolvedCodeIndexRoot(root, repoRoot, entry, hasEntry)
	kbID := resolvedCodeIndexKBID(opts, entry, hasEntry)
	description := resolvedCodeIndexDescription(opts, entry, hasEntry, root)
	opts.IncludeUntracked = resolvedIncludeUntracked(opts, entry, hasEntry)
	return resolvedCodeIndexTarget{Root: root, RepoRoot: repoRoot, RegistryRoot: repoRoot, RepoID: codeRepoID(repoRoot), KBID: kbID, IndexKey: opts.IndexKey, Description: description, Registry: registry, Options: opts}, nil
}

func resolvedCodeIndexRoot(root string, repoRoot string, entry CodebaseIndexRegistryEntry, hasEntry bool) string {
	if hasEntry {
		return rootFromRegistryEntry(repoRoot, entry)
	}
	return root
}

func resolvedCodeIndexKBID(opts CodeIndexOptions, entry CodebaseIndexRegistryEntry, hasEntry bool) string {
	kbID := strings.TrimSpace(opts.KBID)
	if kbID == "" && hasEntry {
		kbID = entry.KBID
	}
	if kbID == "" {
		return defaultKBIDForIndexKey(opts.IndexKey)
	}
	return kbID
}

func resolvedCodeIndexDescription(opts CodeIndexOptions, entry CodebaseIndexRegistryEntry, hasEntry bool, root string) string {
	description := strings.TrimSpace(opts.Description)
	if description == "" && hasEntry {
		description = entry.Description
	}
	if description == "" {
		return defaultCodeIndexDescription(root, opts.IndexKey)
	}
	return description
}

func resolvedIncludeUntracked(opts CodeIndexOptions, entry CodebaseIndexRegistryEntry, hasEntry bool) bool {
	return opts.IncludeUntracked || (hasEntry && entry.IncludeUntracked)
}

func (k *KB) loadPreparedCodeIndexManifest(ctx context.Context, kbID string) (codeIndexManifest, string, error) {
	loadStarted := time.Now()
	manifest, version, err := k.loadCodeIndexManifest(ctx, kbID)
	if err != nil {
		return codeIndexManifest{}, "", err
	}
	if manifest.Files == nil {
		manifest.Files = map[string]codeIndexedFile{}
	}
	if manifest.Chunks == nil {
		manifest.Chunks = map[string]CodeChunkMetadata{}
	}
	slog.Default().InfoContext(ctx, "code index manifest loaded", "kb_id", kbID, "files", len(manifest.Files), "chunks", len(manifest.Chunks), "duration_ms", time.Since(loadStarted).Milliseconds())
	return manifest, version, nil
}

func diffCodeIndexManifest(target resolvedCodeIndexTarget, manifest codeIndexManifest, scanned []codeScannedFile, skipped int) (CodeIndexResult, []string, map[string]codeIndexedFile, map[string]CodeChunkMetadata) {
	current := make(map[string]codeScannedFile, len(scanned))
	for _, file := range scanned {
		current[file.RelPath] = file
	}
	result := CodeIndexResult{KBID: target.KBID, IndexKey: target.IndexKey, Description: target.Description, Root: target.Root, ScannedFiles: len(scanned), SkippedFiles: skipped, ManifestKey: codeIndexManifestKey(target.KBID)}
	var deleteIDs []string
	nextFiles := make(map[string]codeIndexedFile, len(current))
	nextChunks := make(map[string]CodeChunkMetadata)
	for relPath, old := range manifest.Files {
		file, ok := current[relPath]
		if !ok {
			deleteIDs = append(deleteIDs, old.ChunkIDs...)
			result.DeletedFiles++
			result.ChunksDeleted += len(old.ChunkIDs)
			continue
		}
		if old.Hash == file.Hash && old.Language == file.Language {
			nextFiles[relPath] = old
			copyExistingCodeChunks(nextChunks, manifest.Chunks, old.ChunkIDs)
			result.UnchangedFiles++
			continue
		}
		deleteIDs = append(deleteIDs, old.ChunkIDs...)
		result.ChunksDeleted += len(old.ChunkIDs)
	}
	return result, deleteIDs, nextFiles, nextChunks
}

func copyExistingCodeChunks(dst map[string]CodeChunkMetadata, src map[string]CodeChunkMetadata, ids []string) {
	for _, id := range ids {
		if chunk, ok := src[id]; ok {
			dst[id] = chunk
		}
	}
}

type codeIndexPublishState struct {
	scanned    []codeScannedFile
	nextFiles  map[string]codeIndexedFile
	nextChunks map[string]CodeChunkMetadata
	result     *CodeIndexResult
}

func (k *KB) publishCodeIndexChanges(ctx context.Context, target resolvedCodeIndexTarget, state codeIndexPublishState) error {
	streamStarted := time.Now()
	streamer := newCodeDocumentStreamer(k, target, state)
	graphEnabled := false
	if err := k.publishPreparedStream(ctx, PreparedStreamRequest{KBID: target.KBID, Options: UpsertDocsOptions{GraphEnabled: &graphEnabled}, Next: streamer.Next}); err != nil {
		return err
	}
	slog.Default().InfoContext(ctx, "code index stream publish complete", "kb_id", target.KBID, "indexed_files", state.result.IndexedFiles, "chunks_indexed", state.result.ChunksIndexed, "duration_ms", time.Since(streamStarted).Milliseconds())
	return nil
}

func (k *KB) deleteRemovedCodeChunks(ctx context.Context, kbID string, deleteIDs []string) error {
	if len(deleteIDs) == 0 {
		return nil
	}
	deleteStarted := time.Now()
	if err := k.DeleteDocsAndUpload(ctx, kbID, deleteIDs, DeleteDocsOptions{HardDelete: true, CleanupGraph: true}); err != nil {
		return err
	}
	slog.Default().InfoContext(ctx, "code index delete complete", "kb_id", kbID, "chunks_deleted", len(deleteIDs), "duration_ms", time.Since(deleteStarted).Milliseconds())
	return nil
}

func (k *KB) saveCodeIndexState(ctx context.Context, target resolvedCodeIndexTarget, nextFiles map[string]codeIndexedFile, nextChunks map[string]CodeChunkMetadata, version string) error {
	nextManifest := codeIndexManifest{SchemaVersion: CodeIndexManifestSchema, KBID: target.KBID, Root: target.Root, RepoID: target.RepoID, UpdatedAt: k.Clock.Now(), Files: nextFiles, Chunks: nextChunks}
	manifestSaveStarted := time.Now()
	if err := k.saveCodeIndexManifest(ctx, nextManifest, version); err != nil {
		return err
	}
	slog.Default().InfoContext(ctx, "code index manifest saved", "kb_id", target.KBID, "duration_ms", time.Since(manifestSaveStarted).Milliseconds())
	registry := target.Registry
	registry.Indexes[target.IndexKey] = CodebaseIndexRegistryEntry{KBID: target.KBID, Root: registryRelativeRoot(target.RegistryRoot, target.Root), Description: target.Description, IncludeUntracked: target.Options.IncludeUntracked}
	return saveCodebaseIndexRegistry(target.RegistryRoot, registry)
}

func (k *KB) embedCodeDocuments(ctx context.Context, docs []Document, batchSize int) ([]EmbeddedDocument, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	batcher, ok := k.Embedder.(BatchEmbedder)
	if !ok {
		return k.embedCodeDocumentsOneByOne(ctx, docs)
	}
	out := make([]EmbeddedDocument, 0, len(docs))
	if batchSize <= 0 {
		batchSize = DefaultCodeEmbedBatchSize
	}
	for start := 0; start < len(docs); start += batchSize {
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		inputs := codeDocumentTexts(docs[start:end])
		vectors, err := batcher.EmbedBatch(ctx, inputs)
		if err != nil {
			return nil, err
		}
		if len(vectors) != len(inputs) {
			return nil, fmt.Errorf("batch embed returned %d vectors for %d code chunks", len(vectors), len(inputs))
		}
		out = append(out, embeddedCodeDocuments(docs[start:end], vectors)...)
	}
	return out, nil
}

func (k *KB) embedCodeDocumentsOneByOne(ctx context.Context, docs []Document) ([]EmbeddedDocument, error) {
	out := make([]EmbeddedDocument, 0, len(docs))
	for _, doc := range docs {
		vec, err := k.Embed(ctx, doc.Text)
		if err != nil {
			return nil, err
		}
		out = append(out, EmbeddedDocument{ID: doc.ID, Text: doc.Text, Metadata: doc.Metadata, Embedding: vec})
	}
	return out, nil
}

func codeDocumentTexts(docs []Document) []string {
	inputs := make([]string, len(docs))
	for i, doc := range docs {
		inputs[i] = doc.Text
	}
	return inputs
}

func embeddedCodeDocuments(docs []Document, vectors [][]float32) []EmbeddedDocument {
	out := make([]EmbeddedDocument, 0, len(docs))
	for i, doc := range docs {
		out = append(out, EmbeddedDocument{ID: doc.ID, Text: doc.Text, Metadata: doc.Metadata, Embedding: vectors[i]})
	}
	return out
}

type codeDocumentStreamer struct {
	k          *KB
	root       string
	repoID     string
	files      []codeScannedFile
	opts       CodeIndexOptions
	policy     CodeIndexResourcePolicy
	nextFiles  map[string]codeIndexedFile
	nextChunks map[string]CodeChunkMetadata
	result     *CodeIndexResult
	filePos    int
	pending    []Document
}

func newCodeDocumentStreamer(k *KB, target resolvedCodeIndexTarget, state codeIndexPublishState) *codeDocumentStreamer {
	return &codeDocumentStreamer{k: k, root: target.Root, repoID: target.RepoID, files: state.scanned, opts: target.Options, policy: codeIndexResourcePolicyFromOptions(target.Options), nextFiles: state.nextFiles, nextChunks: state.nextChunks, result: state.result}
}

func (s *codeDocumentStreamer) Next(ctx context.Context) ([]EmbeddedDocument, error) {
	if err := s.policy.Check(ctx); err != nil {
		return nil, err
	}
	if err := s.fillPending(ctx); err != nil {
		return nil, err
	}
	if len(s.pending) == 0 {
		return nil, nil
	}
	end := s.policy.BatchEnd(s.pending)
	if end <= 0 {
		end = 1
	}
	batch := s.pending[:end]
	s.pending = s.pending[end:]
	embedStarted := time.Now()
	embedded, err := s.k.embedCodeDocuments(ctx, batch, s.policy.EmbedBatchSize)
	if err != nil {
		return nil, err
	}
	slog.Default().InfoContext(ctx, "code index embed batch complete", "chunks", len(batch), "files_seen", s.filePos, "pending_chunks", len(s.pending), "duration_ms", time.Since(embedStarted).Milliseconds())
	if err := s.policy.Check(ctx); err != nil {
		return nil, err
	}
	if err := s.policy.ThrottleBatch(ctx); err != nil {
		return nil, err
	}
	return embedded, nil
}

func (s *codeDocumentStreamer) fillPending(ctx context.Context) error {
	for s.shouldReadMoreFiles() {
		file := s.files[s.filePos]
		s.filePos++
		if _, ok := s.nextFiles[file.RelPath]; ok {
			continue
		}
		if err := s.addCodeFile(ctx, file); err != nil {
			return err
		}
	}
	return nil
}

func (s *codeDocumentStreamer) shouldReadMoreFiles() bool {
	return len(s.pending) < s.policy.EmbedBatchSize && documentsTextBytes(s.pending) < s.policy.MaxBatchBytes && s.filePos < len(s.files)
}

func (s *codeDocumentStreamer) addCodeFile(ctx context.Context, file codeScannedFile) error {
	docs, metas, err := buildCodeDocuments(ctx, s.root, s.repoID, file, s.opts)
	if err != nil {
		return err
	}
	chunkIDs := make([]string, 0, len(docs))
	for _, doc := range docs {
		chunkIDs = append(chunkIDs, doc.ID)
		s.pending = append(s.pending, doc)
	}
	for _, meta := range metas {
		s.nextChunks[meta.ID] = meta
	}
	s.nextFiles[file.RelPath] = codeIndexedFile{Path: file.RelPath, Hash: file.Hash, SizeBytes: file.SizeBytes, Language: file.Language, ChunkIDs: chunkIDs}
	s.result.IndexedFiles++
	s.result.ChunksIndexed += len(docs)
	return nil
}

func (k *KB) publishCodeDocumentStream(ctx context.Context, kbID string, docs []Document, opts UpsertDocsOptions) error {
	pos := 0
	return k.publishPreparedStream(ctx, PreparedStreamRequest{
		KBID:    kbID,
		Options: opts,
		Next: func(ctx context.Context) ([]EmbeddedDocument, error) {
			if pos >= len(docs) {
				return nil, nil
			}
			end := pos + DefaultCodeEmbedBatchSize
			if end > len(docs) {
				end = len(docs)
			}
			batch := docs[pos:end]
			pos = end
			embedStarted := time.Now()
			embedded, err := k.embedCodeDocuments(ctx, batch, DefaultCodeEmbedBatchSize)
			if err != nil {
				return nil, err
			}
			slog.Default().InfoContext(ctx, "code index embed batch complete", "kb_id", kbID, "chunks", len(batch), "duration_ms", time.Since(embedStarted).Milliseconds())
			return embedded, nil
		},
	})
}

func normalizeCodeIndexOptions(opts CodeIndexOptions) CodeIndexOptions {
	if strings.TrimSpace(opts.IndexKey) == "" {
		opts.IndexKey = "default"
	} else {
		opts.IndexKey = sanitizeCodeIndexKey(opts.IndexKey)
	}
	if opts.MaxFileBytes <= 0 {
		opts.MaxFileBytes = DefaultCodeMaxFileBytes
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = DefaultCodeChunkSize
	}
	if opts.ChunkOverlap < 0 {
		opts.ChunkOverlap = 0
	}
	if opts.ChunkOverlap == 0 {
		opts.ChunkOverlap = DefaultCodeChunkOverlap
	}
	if opts.ChunkOverlap >= opts.ChunkSize {
		opts.ChunkOverlap = opts.ChunkSize / 10
	}
	if len(opts.Include) == 0 {
		opts.Include = append([]string(nil), defaultCodeIncludePatterns...)
	}
	if len(opts.Exclude) == 0 {
		opts.Exclude = append([]string(nil), defaultCodeExcludePatterns...)
	}
	opts = codeIndexResourcePolicyFromOptions(opts).ApplyToOptions(opts)
	return opts
}
