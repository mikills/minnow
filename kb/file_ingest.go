package kb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	pdf "rsc.io/pdf"
)

// fileIDPattern constrains caller-supplied FileID values to characters that
// are safe inside a blob key (the staged blob path is derived from FileID).
var fileIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

type fileResultCarrier interface {
	FileIngestResults() []FileIngestResult
}

type fileIngestStageError struct {
	err         error
	fileResults []FileIngestResult
}

func (e *fileIngestStageError) Error() string { return e.err.Error() }
func (e *fileIngestStageError) Unwrap() error { return e.err }
func (e *fileIngestStageError) FileIngestResults() []FileIngestResult {
	return append([]FileIngestResult(nil), e.fileResults...)
}

type stagedFileUpload struct {
	FileIngestSource
	cleanup func(context.Context)
}

type extractedFileDocument struct {
	doc        Document
	chunked    []Document
	mediaID    string
	result     FileIngestResult
	stagedBlob string
}

// MaxFilesPerIngest caps the combined file + document count accepted by
// AppendFileIngestDetailed in a single request.
const MaxFilesPerIngest = 1000

func (l *KB) AppendFileIngestDetailed(ctx context.Context, input FileIngestInput, maxBytes int64, idempotencyKey, correlationID string) (string, string, error) {
	if l.BlobStore == nil {
		return "", "", errors.New("ingest: BlobStore not configured")
	}
	if len(input.Files) == 0 && len(input.Documents) == 0 {
		return "", "", errors.New("ingest: at least one file or document is required")
	}
	if total := len(input.Files) + len(input.Documents); total > MaxFilesPerIngest {
		return "", "", fmt.Errorf("ingest: too many items (%d): max %d per request", total, MaxFilesPerIngest)
	}
	staged := make([]stagedFileUpload, 0, len(input.Files))
	cleanupAll := func() {
		for _, s := range staged {
			if s.cleanup != nil {
				s.cleanup(ctx)
			}
		}
	}
	for i, file := range input.Files {
		s, err := l.stageFileIngestUpload(ctx, input.KBID, file, maxBytes, i)
		if err != nil {
			cleanupAll()
			return "", "", err
		}
		staged = append(staged, s)
	}
	payload := DocumentUpsertPayload{KBID: input.KBID, Documents: input.Documents, ChunkSize: input.ChunkSize, Options: input.Options}
	for _, s := range staged {
		payload.FileSources = append(payload.FileSources, s.FileIngestSource)
	}
	eventID, effectiveIdem, err := l.AppendDocumentUpsertDetailed(ctx, payload, idempotencyKey, correlationID)
	if err != nil {
		cleanupAll()
		return "", "", err
	}
	return eventID, effectiveIdem, nil
}

func (l *KB) stageFileIngestUpload(ctx context.Context, kbID string, file FileIngestUpload, maxBytes int64, index int) (stagedFileUpload, error) {
	if strings.TrimSpace(kbID) == "" {
		return stagedFileUpload{}, errors.New("ingest: kb_id required")
	}
	if file.Body == nil {
		return stagedFileUpload{}, errors.New("ingest: file body required")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	cleanName, err := SanitizeMediaFilename(file.Filename)
	if err != nil {
		return stagedFileUpload{}, err
	}
	ct := normaliseContentType(file.ContentType)
	if ct == "application/octet-stream" {
		if guessed := mime.TypeByExtension(strings.ToLower(filepath.Ext(cleanName))); guessed != "" {
			ct = normaliseContentType(guessed)
		}
	}
	if !isSupportedIngestFileContentType(ct, cleanName) {
		return stagedFileUpload{}, fmt.Errorf("ingest: content type %q for %q is not supported", ct, cleanName)
	}
	tmp, err := writeTempCapped(file.Body, maxBytes)
	if err != nil {
		return stagedFileUpload{}, err
	}
	if tmp.size == 0 {
		tmp.cleanup()
		return stagedFileUpload{}, errors.New("ingest: empty file rejected")
	}
	fileID := strings.TrimSpace(file.FileID)
	if fileID == "" {
		fileID = fmt.Sprintf("file-%03d-%s", index, randomHexString(3))
	} else if !fileIDPattern.MatchString(fileID) {
		return stagedFileUpload{}, fmt.Errorf("ingest: file_id %q must match [A-Za-z0-9_.-]+", fileID)
	}
	mediaID := l.newMediaID()
	documentID := strings.TrimSpace(file.DocumentID)
	if documentID == "" {
		documentID = generateSourceDocumentID(cleanName)
	}
	stagedBlobKey := mediaStagingBlobKey(kbID, fileID, cleanName)
	if _, err := l.BlobStore.UploadIfMatch(ctx, stagedBlobKey, tmp.path, ""); err != nil {
		tmp.cleanup()
		return stagedFileUpload{}, fmt.Errorf("ingest: stage file upload: %w", err)
	}
	tmp.cleanup()
	return stagedFileUpload{FileIngestSource: FileIngestSource{FileID: fileID, DocumentID: documentID, MediaID: mediaID, Filename: cleanName, ContentType: ct, StagedBlobKey: stagedBlobKey, SizeBytes: tmp.size, Checksum: tmp.sha256, Metadata: cloneMap(file.Metadata)}, cleanup: func(ctx context.Context) { _ = l.BlobStore.Delete(ctx, stagedBlobKey) }}, nil
}

func generateSourceDocumentID(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	base = strings.TrimSpace(base)
	if base == "" {
		base = "file"
	}
	return fmt.Sprintf("%s-%s", base, randomHexString(3))
}

func randomHexString(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func isSupportedIngestFileContentType(ct, filename string) bool {
	ct = normaliseContentType(ct)
	if idx := strings.Index(ct, ";"); idx >= 0 {
		ct = strings.TrimSpace(ct[:idx])
	}
	switch ct {
	case "application/pdf", "text/plain":
		return true
	case "":
		fallthrough
	case "application/octet-stream":
		ext := strings.ToLower(filepath.Ext(filename))
		return ext == ".pdf" || ext == ".txt"
	default:
		ext := strings.ToLower(filepath.Ext(filename))
		return ct == "text/markdown" && ext == ".txt"
	}
}

func (w *DocumentUpsertWorker) normalizeDocuments(ctx context.Context, kbID string, sourceDocs []Document, fileSources []FileIngestSource, chunkSize int) ([]Document, []FileIngestResult, []string, []string, error) {
	chunkedDocs, err := chunkDocuments(ctx, sourceDocs, chunkSize)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	results := make([]FileIngestResult, 0, len(fileSources))
	mediaIDs := collectMediaIDs(sourceDocs)
	stagedBlobKeys := make([]string, 0, len(fileSources))
	successes := 0
	for _, src := range fileSources {
		extracted, err := w.extractFileSource(ctx, kbID, src, chunkSize)
		if err != nil {
			results = append(results, FileIngestResult{FileID: src.FileID, DocumentID: src.DocumentID, MediaID: src.MediaID, Filename: src.Filename, ContentType: src.ContentType, Status: "failed", Error: err.Error(), Metadata: cloneMap(src.Metadata)})
			continue
		}
		successes++
		results = append(results, extracted.result)
		mediaIDs = append(mediaIDs, extracted.mediaID)
		stagedBlobKeys = append(stagedBlobKeys, extracted.stagedBlob)
		chunkedDocs = append(chunkedDocs, extracted.chunked...)
	}
	if len(chunkedDocs) == 0 && len(fileSources) > 0 {
		return nil, results, mediaIDs, stagedBlobKeys, &fileIngestStageError{err: errors.New("ingest: no ingestible files in request"), fileResults: results}
	}
	_ = successes
	return chunkedDocs, results, mediaIDs, stagedBlobKeys, nil
}

func (w *DocumentUpsertWorker) extractFileSource(ctx context.Context, kbID string, src FileIngestSource, chunkSize int) (*extractedFileDocument, error) {
	if w.KB.BlobStore == nil || w.KB.MediaStore == nil {
		return nil, errors.New("ingest worker: media subsystem not configured")
	}
	mediaRec := MediaObject{ID: src.MediaID, KBID: kbID, Filename: src.Filename, ContentType: src.ContentType, SizeBytes: src.SizeBytes, Checksum: src.Checksum, CreatedAtUnixMs: w.KB.Clock.Now().UnixMilli(), Metadata: cloneMap(src.Metadata), State: MediaStatePending}
	if err := w.KB.persistStagedMediaObject(ctx, src.StagedBlobKey, mediaRec); err != nil {
		return nil, err
	}
	tmpPath, cleanup, err := w.KB.downloadStagedBlobTemp(ctx, src.StagedBlobKey, "minnow-file-ingest-*")
	if err != nil {
		return nil, err
	}
	defer cleanup()
	pages, err := extractSourceText(tmpPath, src.ContentType)
	if err != nil {
		return nil, err
	}
	chunked, pageCount, err := chunkExtractedPages(ctx, src, pages, chunkSize)
	if err != nil {
		return nil, err
	}
	return &extractedFileDocument{mediaID: src.MediaID, stagedBlob: src.StagedBlobKey, chunked: chunked, result: FileIngestResult{FileID: src.FileID, DocumentID: src.DocumentID, MediaID: src.MediaID, Filename: src.Filename, ContentType: src.ContentType, Status: "succeeded", PageCount: pageCount, Metadata: cloneMap(src.Metadata)}}, nil
}

func (l *KB) persistStagedMediaObject(ctx context.Context, stagedBlobKey string, rec MediaObject) error {
	if l.BlobStore == nil || l.MediaStore == nil {
		return errors.New("media subsystem not configured")
	}
	tmpPath, cleanup, err := l.downloadStagedBlobTemp(ctx, stagedBlobKey, "minnow-staged-media-*")
	if err != nil {
		return err
	}
	defer cleanup()
	finalBlobKey := MediaBlobKey(rec.KBID, rec.ID, rec.Filename)
	// Create-if-absent via UploadIfMatch(""); a version-mismatch response
	// means another actor uploaded concurrently; accept as success. Avoids
	// a TOCTOU between Head() and UploadIfMatch().
	if _, err := l.BlobStore.UploadIfMatch(ctx, finalBlobKey, tmpPath, ""); err != nil && !errors.Is(err, ErrBlobVersionMismatch) {
		return fmt.Errorf("upload final media: %w", err)
	}
	rec.BlobKey = finalBlobKey
	if rec.CreatedAtUnixMs == 0 {
		rec.CreatedAtUnixMs = l.Clock.Now().UnixMilli()
	}
	if err := l.MediaStore.Put(ctx, rec); err != nil && !errors.Is(err, ErrMediaDuplicateKey) {
		return err
	}
	return nil
}

func (l *KB) downloadStagedBlobTemp(ctx context.Context, stagedBlobKey, pattern string) (string, func(), error) {
	tmp, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", nil, err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if err := l.BlobStore.Download(ctx, stagedBlobKey, tmpPath); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("download staged media: %w", err)
	}
	return tmpPath, cleanup, nil
}

type extractedPage struct {
	Number int
	Text   string
}

func extractSourceText(path, contentType string) ([]extractedPage, error) {
	ct := normaliseContentType(contentType)
	if idx := strings.Index(ct, ";"); idx >= 0 {
		ct = strings.TrimSpace(ct[:idx])
	}
	if ct == "application/octet-stream" || ct == "" {
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".pdf" {
			ct = "application/pdf"
		} else {
			ct = "text/plain"
		}
	}
	switch ct {
	case "text/plain":
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		text := strings.TrimSpace(string(b))
		if text == "" {
			return nil, errors.New("ingest: extracted text is empty")
		}
		return []extractedPage{{Number: 1, Text: text}}, nil
	case "application/pdf":
		return extractPDFPages(path)
	default:
		return nil, fmt.Errorf("ingest: unsupported extracted content type %q", ct)
	}
}

func extractPDFPages(path string) ([]extractedPage, error) {
	r, err := pdf.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ingest: parse pdf: %w", err)
	}
	pages := make([]extractedPage, 0, r.NumPage())
	for i := 1; i <= r.NumPage(); i++ {
		p := r.Page(i)
		content := p.Content()
		var parts []string
		for _, txt := range content.Text {
			if s := strings.TrimSpace(txt.S); s != "" {
				parts = append(parts, s)
			}
		}
		pageText := strings.TrimSpace(strings.Join(parts, " "))
		if pageText == "" {
			continue
		}
		pages = append(pages, extractedPage{Number: i, Text: pageText})
	}
	if len(pages) == 0 {
		return nil, errors.New("ingest: extracted text is empty")
	}
	return pages, nil
}

func chunkExtractedPages(ctx context.Context, src FileIngestSource, pages []extractedPage, chunkSize int) ([]Document, int, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultTextChunkSize
	}
	chunker := TextChunker{ChunkSize: chunkSize}
	out := make([]Document, 0)
	for _, page := range pages {
		chunks, err := chunker.Chunk(ctx, fmt.Sprintf("%s-page-%d", src.DocumentID, page.Number), page.Text)
		if err != nil {
			return nil, 0, err
		}
		for _, chunk := range chunks {
			ref := ChunkMediaRef{MediaID: src.MediaID, Label: src.Filename, Locator: &MediaLocator{Page: page.Number}, Metadata: cloneMap(src.Metadata)}
			out = append(out, Document{ID: chunk.ChunkID, Text: chunk.Text, MediaRefs: []ChunkMediaRef{ref}, Metadata: cloneMap(src.Metadata)})
		}
	}
	return out, len(pages), nil
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
