package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/mikills/minnow/kb"

	"github.com/labstack/echo/v4"
)

type ragIngestRequest struct {
	KBID         string           `json:"kb_id"`
	ChunkSize    int              `json:"chunk_size"`
	GraphEnabled *bool            `json:"graph_enabled"`
	Documents    []ragIngestDocIn `json:"documents"`
}

type ragIngestDocIn struct {
	ID        string             `json:"id"`
	Text      string             `json:"text"`
	MediaIDs  []string           `json:"media_ids,omitempty"`
	MediaRefs []kb.ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata  map[string]any     `json:"metadata,omitempty"`
}

type ragQueryRequest struct {
	KBID       string `json:"kb_id"`
	Query      string `json:"query"`
	K          int    `json:"k"`
	SearchMode string `json:"search_mode,omitempty"`
}

type ragQueryResultOut struct {
	ID         string             `json:"id"`
	Content    string             `json:"content"`
	Distance   float64            `json:"distance"`
	Score      *float64           `json:"score,omitempty"`
	GraphScore *float64           `json:"graph_score,omitempty"`
	MediaRefs  []kb.ChunkMediaRef `json:"media_refs,omitempty"`
}

type multipartFileMetadata struct {
	Metadata map[string]any `json:"metadata,omitempty"`
}

type multipartIngestRequest struct {
	KBID         string
	ChunkSize    int
	GraphEnabled *bool
	Documents    []kb.Document
	FileIDs      []string
	FileMetadata map[string]multipartFileMetadata
}

func registerRagRoutes(e *echo.Echo, deps Dependencies) {
	logger := deps.Logger
	metrics := deps.AppMetrics

	e.POST("/rag/ingest", func(c echo.Context) error {
		start := time.Now()
		if deps.AppendDocumentUpsert == nil && deps.AppendFileIngest == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "ingest event pipeline not configured"})
		}
		if strings.HasPrefix(strings.ToLower(c.Request().Header.Get("Content-Type")), "multipart/form-data") {
			return handleMultipartIngest(c, deps, logger, metrics, start)
		}

		var req ragIngestRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid request body"})
		}
		req.KBID = strings.TrimSpace(req.KBID)
		if req.KBID == "" {
			req.KBID = "default"
		}
		if req.GraphEnabled == nil {
			err := fmt.Errorf("graph_enabled is required")
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
			return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		if len(req.Documents) == 0 {
			err := fmt.Errorf("documents are required")
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
			return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		docs, docIDs, opts, err := buildIngestDocuments(req)
		if err != nil {
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
			return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		}

		idemKey, corr := requestIDs(c)
		evtID, effectiveIdem, err := deps.AppendDocumentUpsert(c.Request().Context(),
			kb.DocumentUpsertPayload{KBID: req.KBID, Documents: docs, ChunkSize: req.ChunkSize, Options: opts},
			idemKey, corr)
		if err != nil {
			metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, err)
			if errors.Is(err, kb.ErrGraphUnavailable) {
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return WriteError(c, err, deps.IsBudgetExceeded)
		}
		logger.InfoContext(c.Request().Context(), "rag ingest accepted",
			"kb_id", req.KBID,
			"document_count", len(req.Documents),
			"event_id", evtID,
		)
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents), 0, nil)

		return writeAcceptedOperation(c, evtID, effectiveIdem, map[string]any{
			"event_id":       evtID,
			"status_url":     "/rag/operations/" + evtID,
			"kb_id":          req.KBID,
			"document_count": len(req.Documents),
			"doc_ids":        docIDs,
		})
	})

	e.POST("/rag/query", func(c echo.Context) error {
		start := time.Now()
		if deps.Embed == nil || deps.Search == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "kb unavailable"})
		}

		var req ragQueryRequest
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid request body"})
		}
		req.KBID = strings.TrimSpace(req.KBID)
		if req.KBID == "" {
			req.KBID = "default"
		}
		if strings.TrimSpace(req.Query) == "" {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "query is required"})
		}
		if req.K <= 0 {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": "k must be > 0"})
		}
		mode, modeName, modeErr := parseSearchMode(req.SearchMode)
		if modeErr != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": modeErr.Error()})
		}

		embedStart := time.Now()
		vec, err := deps.Embed(c.Request().Context(), req.Query)
		embedLatencyMS := time.Since(embedStart).Milliseconds()
		metrics.RecordEmbed(req.KBID, embedLatencyMS, err)
		if err != nil {
			logger.ErrorContext(c.Request().Context(), "rag query failed",
				"kb_id", req.KBID,
				"error", err,
			)
			return WriteError(c, err, deps.IsBudgetExceeded)
		}

		searchOpts := &kb.SearchOptions{Mode: mode, TopK: req.K}
		results, err := deps.Search(c.Request().Context(), req.KBID, vec, searchOpts)
		if err != nil {
			metrics.RecordQuery(req.KBID, time.Since(start).Milliseconds(), 0, 0, err)
			logger.ErrorContext(c.Request().Context(), "rag query failed",
				"kb_id", req.KBID,
				"error", err,
			)
			if errors.Is(err, kb.ErrGraphQueryUnavailable) {
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			if errors.Is(err, kb.ErrKBUninitialized) {
				return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
			}
			return WriteError(c, err, deps.IsBudgetExceeded)
		}

		includeScoring := mode != kb.SearchModeVector
		queryResults := make([]ragQueryResultOut, 0, len(results))
		for _, result := range results {
			out := ragQueryResultOut{
				ID:       result.ID,
				Content:  result.Content,
				Distance: result.Distance,
			}
			if includeScoring {
				score := result.Score
				graphScore := result.GraphScore
				out.Score = &score
				out.GraphScore = &graphScore
			}
			if len(result.MediaRefs) > 0 {
				out.MediaRefs = result.MediaRefs
			}
			queryResults = append(queryResults, out)
		}

		queryForLog := strings.TrimSpace(req.Query)
		if runes := []rune(queryForLog); len(runes) > 100 {
			queryForLog = string(runes[:100])
		}
		logger.InfoContext(c.Request().Context(), "rag query completed",
			"kb_id", req.KBID,
			"search_mode", modeName,
			"query", queryForLog,
			"result_count", len(results),
			"latency_ms", time.Since(start).Milliseconds(),
		)
		topDistance := float32(0)
		if len(queryResults) > 0 {
			topDistance = float32(queryResults[0].Distance)
		}
		metrics.RecordQuery(req.KBID, time.Since(start).Milliseconds(), len(results), topDistance, nil)

		return c.JSON(http.StatusOK, map[string]any{"results": queryResults})
	})
}

func parseSearchMode(raw string) (kb.SearchMode, string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", "vector":
		return kb.SearchModeVector, "vector", nil
	case "graph":
		return kb.SearchModeGraph, "graph", nil
	case "adaptive":
		return kb.SearchModeAdaptive, "adaptive", nil
	default:
		return kb.SearchModeVector, "", fmt.Errorf("invalid search_mode: %q (allowed: vector, graph, adaptive)", raw)
	}
}

func generateDocID() string {
	return fmt.Sprintf("doc-%d-%s", time.Now().UnixMilli(), randomHex(3))
}

func randomHex(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func buildIngestDocuments(req ragIngestRequest) ([]kb.Document, []string, kb.UpsertDocsOptions, error) {
	if req.GraphEnabled == nil {
		return nil, nil, kb.UpsertDocsOptions{}, fmt.Errorf("graph_enabled is required")
	}
	docs := make([]kb.Document, 0, len(req.Documents))
	docIDs := make([]string, 0, len(req.Documents))
	for _, doc := range req.Documents {
		id := strings.TrimSpace(doc.ID)
		text := strings.TrimSpace(doc.Text)
		if id == "" {
			id = generateDocID()
		}
		docIDs = append(docIDs, id)
		if text == "" {
			return nil, nil, kb.UpsertDocsOptions{}, fmt.Errorf("each document requires non-empty text")
		}
		docs = append(docs, kb.Document{ID: id, Text: text, MediaIDs: doc.MediaIDs, MediaRefs: doc.MediaRefs, Metadata: doc.Metadata})
	}
	return docs, docIDs, kb.UpsertDocsOptions{GraphEnabled: req.GraphEnabled}, nil
}

func handleMultipartIngest(c echo.Context, deps Dependencies, logger *slog.Logger, metrics kb.AppMetrics, start time.Time) error {
	if deps.AppendFileIngest == nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]any{"error": "multipart file ingest not configured"})
	}
	if deps.MaxMediaBytes > 0 && c.Request().ContentLength > deps.MaxMediaBytes {
		return c.JSON(http.StatusRequestEntityTooLarge, map[string]any{"error": "upload exceeds maximum allowed size"})
	}
	if deps.MaxMediaBytes > 0 {
		c.Request().Body = http.MaxBytesReader(c.Response().Writer, c.Request().Body, deps.MaxMediaBytes)
	}
	if err := c.Request().ParseMultipartForm(deps.MaxMediaBytes); err != nil {
		if isRequestBodyTooLarge(err) {
			return c.JSON(http.StatusRequestEntityTooLarge, map[string]any{"error": "upload exceeds maximum allowed size"})
		}
		return c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid multipart form"})
	}
	form := c.Request().MultipartForm
	req, files, docIDs, err := buildMultipartIngestInput(form)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), 0, 0, err)
		return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
	}
	defer func() {
		for _, f := range files {
			if f != nil {
				_ = f.Close()
			}
		}
	}()
	fileUploads := make([]kb.FileIngestUpload, 0, len(files))
	for i, fh := range multipartFiles(form) {
		fileID := req.FileIDs[i]
		meta := req.FileMetadata[fileID]
		fileUploads = append(fileUploads, kb.FileIngestUpload{FileID: fileID, DocumentID: docIDs[len(req.Documents)+i], Filename: fh.Filename, ContentType: fh.Header.Get("Content-Type"), Metadata: meta.Metadata, Body: files[i]})
	}
	input := kb.FileIngestInput{KBID: req.KBID, Documents: req.Documents, Files: fileUploads, ChunkSize: req.ChunkSize, Options: kb.UpsertDocsOptions{GraphEnabled: req.GraphEnabled}}
	idemKey := strings.TrimSpace(c.Request().Header.Get("Idempotency-Key"))
	corr := strings.TrimSpace(c.Request().Header.Get("X-Correlation-Id"))
	evtID, effectiveIdem, err := deps.AppendFileIngest(c.Request().Context(), input, deps.MaxMediaBytes, idemKey, corr)
	if err != nil {
		metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents)+len(fileUploads), 0, err)
		if errors.Is(err, kb.ErrGraphUnavailable) {
			return c.JSON(http.StatusBadRequest, map[string]any{"error": err.Error()})
		}
		return WriteError(c, err, deps.IsBudgetExceeded)
	}
	c.Response().Header().Set("X-Source-Event-Id", evtID)
	c.Response().Header().Set("Idempotency-Key", effectiveIdem)
	logger.InfoContext(c.Request().Context(), "multipart rag ingest accepted", "kb_id", req.KBID, "document_count", len(req.Documents)+len(fileUploads), "event_id", evtID)
	metrics.RecordIngest(req.KBID, time.Since(start).Milliseconds(), len(req.Documents)+len(fileUploads), 0, nil)
	return c.JSON(http.StatusAccepted, map[string]any{"event_id": evtID, "status_url": "/rag/operations/" + evtID, "kb_id": req.KBID, "document_count": len(req.Documents) + len(fileUploads), "doc_ids": docIDs})
}

func buildMultipartIngestInput(form *multipart.Form) (multipartIngestRequest, []multipart.File, []string, error) {
	var req multipartIngestRequest
	req.KBID = strings.TrimSpace(firstFormValue(form, "kb_id"))
	if req.KBID == "" {
		req.KBID = "default"
	}
	gb, err := parseRequiredBool(firstFormValue(form, "graph_enabled"), "graph_enabled")
	if err != nil {
		return req, nil, nil, err
	}
	req.GraphEnabled = &gb
	req.ChunkSize = parsePositiveInt(firstFormValue(form, "chunk_size"), 0)
	if raw := strings.TrimSpace(firstFormValue(form, "documents")); raw != "" {
		var docs []ragIngestDocIn
		if err := json.Unmarshal([]byte(raw), &docs); err != nil {
			return req, nil, nil, fmt.Errorf("documents must be valid JSON")
		}
		built, _, _, err := buildIngestDocuments(ragIngestRequest{KBID: req.KBID, ChunkSize: req.ChunkSize, GraphEnabled: req.GraphEnabled, Documents: docs})
		if err != nil {
			return req, nil, nil, err
		}
		req.Documents = built
	}
	fhs := multipartFiles(form)
	if len(req.Documents) == 0 && len(fhs) == 0 {
		return req, nil, nil, fmt.Errorf("documents or files are required")
	}
	for _, fileID := range form.Value["file_ids"] {
		if trimmed := strings.TrimSpace(fileID); trimmed != "" {
			for _, existing := range req.FileIDs {
				if existing == trimmed {
					return req, nil, nil, fmt.Errorf("file_ids must be unique")
				}
			}
			req.FileIDs = append(req.FileIDs, trimmed)
		}
	}
	if len(fhs) > 0 && len(req.FileIDs) != len(fhs) {
		return req, nil, nil, fmt.Errorf("file_ids count must match file count")
	}
	req.FileMetadata = make(map[string]multipartFileMetadata, len(req.FileIDs))
	if raw := strings.TrimSpace(firstFormValue(form, "file_metadata")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &req.FileMetadata); err != nil {
			return req, nil, nil, fmt.Errorf("file_metadata must be valid JSON")
		}
		for fileID := range req.FileMetadata {
			if !containsString(req.FileIDs, fileID) {
				return req, nil, nil, fmt.Errorf("file_metadata contains unknown file_id %q", fileID)
			}
		}
	}
	for _, fileID := range req.FileIDs {
		if _, ok := req.FileMetadata[fileID]; !ok {
			req.FileMetadata[fileID] = multipartFileMetadata{}
		}
	}
	opened := make([]multipart.File, 0, len(fhs))
	docIDs := make([]string, 0, len(req.Documents)+len(fhs))
	for _, doc := range req.Documents {
		docIDs = append(docIDs, doc.ID)
	}
	for i, fh := range fhs {
		f, err := fh.Open()
		if err != nil {
			for _, openedFile := range opened {
				_ = openedFile.Close()
			}
			return req, nil, nil, fmt.Errorf("cannot open uploaded file %q", fh.Filename)
		}
		opened = append(opened, f)
		docIDs = append(docIDs, req.FileIDs[i])
	}
	return req, opened, docIDs, nil
}

func multipartFiles(form *multipart.Form) []*multipart.FileHeader {
	if form == nil {
		return nil
	}
	out := append([]*multipart.FileHeader(nil), form.File["files"]...)
	if len(out) == 0 {
		out = append(out, form.File["file"]...)
	}
	return out
}

func firstFormValue(form *multipart.Form, key string) string {
	if form == nil || len(form.Value[key]) == 0 {
		return ""
	}
	return form.Value[key][0]
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func parseRequiredBool(raw, field string) (bool, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return false, fmt.Errorf("%s is required", field)
	}
	switch raw {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be true or false", field)
	}
}

func isRequestBodyTooLarge(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "request body too large") || strings.Contains(msg, "http: request body too large")
}
