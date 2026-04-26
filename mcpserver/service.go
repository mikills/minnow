package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/mikills/minnow/kb"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Service struct {
	Config Config
	Logger *slog.Logger

	Embed                 func(context.Context, string) ([]float32, error)
	Search                func(context.Context, string, []float32, *kb.SearchOptions) ([]kb.ExpandedResult, error)
	AppendDocumentUpsert  func(context.Context, kb.DocumentUpsertPayload, string, string) (string, string, error)
	GetEvent              func(context.Context, string) (*kb.KBEvent, error)
	FindOperationTerminal func(context.Context, string) (*kb.KBEvent, error)
	OperationStages       func(context.Context, string) ([]kb.OperationStageSnapshot, error)
	ListMedia             func(context.Context, string, string, string, int) (kb.MediaPage, error)
	GetMedia              func(context.Context, string) (*kb.MediaObject, error)
	DeleteMedia           func(context.Context, string) error
	SweepCache            func(context.Context) error
	ClearCache            func(context.Context) error
	ForceCompaction       func(context.Context, string) (*kb.CompactionPublishResult, error)
	DeleteKnowledgeBase   func(context.Context, string) error
}

// mustQueryInputSchema returns the queryInput JSONSchema with explicit bounds
// on K. Agents reading `tools/list` see the constraint up front instead of
// only learning of it via a runtime error. Handler still validates K > 0 as
// defense-in-depth.
func mustQueryInputSchema() *jsonschema.Schema {
	schema, err := jsonschema.For[queryInput](nil)
	if err != nil {
		// jsonschema.For only fails on programmer error (unsupported types);
		// queryInput is plain string/int. Panic so misuse is caught at startup.
		panic(fmt.Sprintf("build queryInput schema: %v", err))
	}
	if k, ok := schema.Properties["k"]; ok {
		min := float64(1)
		max := float64(queryToolMaxK)
		k.Minimum = &min
		k.Maximum = &max
	}
	return schema
}

func New(s Service) *mcp.Server {
	cfg := s.Config.normalized()
	s.Config = cfg
	server := mcp.NewServer(&mcp.Implementation{Name: "minnow", Version: "v0.1.0"}, &mcp.ServerOptions{
		Instructions: "Minnow MCP exposes retrieval, indexing, operation status, and explicitly gated maintenance tools for coding agents.",
		Logger:       s.Logger,
	})
	registerTools(server, &s)
	registerResources(server, &s)
	return server
}

// queryToolMaxK caps the K value the schema advertises so agents do not
// request unboundedly large result sets. Handler still enforces > 0.
const queryToolMaxK = 200

// registerTools registers only the tools permitted by s.Config so that
// `tools/list` reflects what the agent can actually call. Per-handler gates
// remain as defense-in-depth, so a misconfigured Service still rejects
// disallowed calls instead of silently executing them.
func registerTools(server *mcp.Server, s *Service) {
	cfg := s.Config

	queryTool := &mcp.Tool{
		Name:        "minnow_query",
		Description: "Query a Minnow knowledge base using vector, graph, or adaptive search.",
		InputSchema: mustQueryInputSchema(),
	}
	mcp.AddTool(server, queryTool, s.query)
	mcp.AddTool(server, &mcp.Tool{Name: "minnow_operation_status", Description: "Read the status, stages, and terminal event for an async Minnow operation."}, s.operationStatus)
	mcp.AddTool(server, &mcp.Tool{Name: "minnow_list_media", Description: "List media metadata for a knowledge base when media is configured."}, s.listMedia)
	mcp.AddTool(server, &mcp.Tool{Name: "minnow_get_media", Description: "Get media metadata by media ID when media is configured."}, s.getMedia)

	if !cfg.ReadOnly && cfg.AllowIndexing {
		mcp.AddTool(server, &mcp.Tool{Name: "minnow_ingest_documents_async", Description: "Submit text documents for asynchronous indexing. Returns an operation ID."}, s.ingestAsync)
		if cfg.AllowSyncIndexing {
			mcp.AddTool(server, &mcp.Tool{Name: "minnow_ingest_documents_sync", Description: "Submit text documents for indexing and wait for publish up to a bounded timeout."}, s.ingestSync)
		}
	}

	if !cfg.ReadOnly && cfg.AllowDestructive {
		mcp.AddTool(server, &mcp.Tool{Name: "minnow_delete_media", Description: "Destructive tool: tombstone media by media ID."}, s.deleteMedia)
		mcp.AddTool(server, &mcp.Tool{Name: "minnow_delete_knowledge_base", Description: "Destructive tool: delete a knowledge base manifest, shards, cache, and media metadata."}, s.deleteKnowledgeBase)
	}

	if cfg.AllowAdmin {
		mcp.AddTool(server, &mcp.Tool{Name: "minnow_sweep_cache", Description: "Admin tool: run policy-based cache eviction."}, s.sweepCache)
		mcp.AddTool(server, &mcp.Tool{Name: "minnow_force_compaction", Description: "Admin tool: trigger compaction for a knowledge base when compaction debt exists."}, s.forceCompaction)
		if !cfg.ReadOnly && cfg.AllowDestructive {
			mcp.AddTool(server, &mcp.Tool{Name: "minnow_clear_cache", Description: "Admin destructive tool: clear all local cache entries."}, s.clearCache)
		}
	}
}

type queryInput struct {
	KBID       string `json:"kb_id,omitempty" jsonschema:"Knowledge base ID. Defaults to default."`
	Query      string `json:"query" jsonschema:"Natural language query."`
	K          int    `json:"k" jsonschema:"Number of results to return."`
	SearchMode string `json:"search_mode,omitempty" jsonschema:"vector, graph, or adaptive."`
}

type queryOutput struct {
	KBID       string      `json:"kb_id"`
	SearchMode string      `json:"search_mode"`
	Results    []resultOut `json:"results"`
}

type resultOut struct {
	ID         string             `json:"id"`
	Content    string             `json:"content"`
	Distance   float64            `json:"distance"`
	Score      *float64           `json:"score,omitempty"`
	GraphScore *float64           `json:"graph_score,omitempty"`
	MediaRefs  []kb.ChunkMediaRef `json:"media_refs,omitempty"`
}

func (s *Service) query(ctx context.Context, _ *mcp.CallToolRequest, in queryInput) (*mcp.CallToolResult, queryOutput, error) {
	kbID := defaultKBID(in.KBID)
	if strings.TrimSpace(in.Query) == "" {
		return nil, queryOutput{}, fmt.Errorf("query is required")
	}
	if in.K <= 0 {
		return nil, queryOutput{}, fmt.Errorf("k must be > 0")
	}
	if in.K > queryToolMaxK {
		return nil, queryOutput{}, fmt.Errorf("k must be <= %d", queryToolMaxK)
	}
	mode, modeName, err := parseSearchMode(in.SearchMode)
	if err != nil {
		return nil, queryOutput{}, err
	}
	if s.Embed == nil || s.Search == nil {
		return nil, queryOutput{}, fmt.Errorf("kb unavailable")
	}
	vec, err := s.Embed(ctx, in.Query)
	if err != nil {
		return nil, queryOutput{}, err
	}
	results, err := s.Search(ctx, kbID, vec, &kb.SearchOptions{Mode: mode, TopK: in.K})
	if err != nil {
		return nil, queryOutput{}, err
	}
	out := queryOutput{KBID: kbID, SearchMode: modeName, Results: make([]resultOut, 0, len(results))}
	for _, r := range results {
		item := resultOut{ID: r.ID, Content: r.Content, Distance: r.Distance}
		if mode != kb.SearchModeVector {
			score := r.Score
			graphScore := r.GraphScore
			item.Score = &score
			item.GraphScore = &graphScore
		}
		if len(r.MediaRefs) > 0 {
			item.MediaRefs = r.MediaRefs
		}
		out.Results = append(out.Results, item)
	}
	return nil, out, nil
}

type ingestDocInput struct {
	ID        string             `json:"id" jsonschema:"Document ID."`
	Text      string             `json:"text" jsonschema:"Document text."`
	MediaIDs  []string           `json:"media_ids,omitempty"`
	MediaRefs []kb.ChunkMediaRef `json:"media_refs,omitempty"`
	Metadata  map[string]any     `json:"metadata,omitempty"`
}

type ingestInput struct {
	KBID           string           `json:"kb_id,omitempty"`
	Documents      []ingestDocInput `json:"documents"`
	ChunkSize      int              `json:"chunk_size,omitempty"`
	GraphEnabled   bool             `json:"graph_enabled"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
	CorrelationID  string           `json:"correlation_id,omitempty"`
}

type ingestSyncInput struct {
	KBID           string           `json:"kb_id,omitempty"`
	Documents      []ingestDocInput `json:"documents"`
	ChunkSize      int              `json:"chunk_size,omitempty"`
	GraphEnabled   bool             `json:"graph_enabled"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
	CorrelationID  string           `json:"correlation_id,omitempty"`
	TimeoutMS      int64            `json:"timeout_ms,omitempty"`
}

type ingestOutput struct {
	EventID        string           `json:"event_id"`
	Status         string           `json:"status"`
	KBID           string           `json:"kb_id"`
	DocumentCount  int              `json:"document_count"`
	DocIDs         []string         `json:"doc_ids"`
	IdempotencyKey string           `json:"idempotency_key"`
	TimedOut       bool             `json:"timed_out,omitempty"`
	Terminal       map[string]any   `json:"terminal,omitempty"`
	Stages         []map[string]any `json:"stages,omitempty"`
}

func (s *Service) ingestAsync(ctx context.Context, _ *mcp.CallToolRequest, in ingestInput) (*mcp.CallToolResult, ingestOutput, error) {
	out, err := s.submitIngest(ctx, in.KBID, in.Documents, in.ChunkSize, in.GraphEnabled, in.IdempotencyKey, in.CorrelationID)
	return nil, out, err
}

func (s *Service) ingestSync(ctx context.Context, _ *mcp.CallToolRequest, in ingestSyncInput) (*mcp.CallToolResult, ingestOutput, error) {
	if !s.Config.AllowSyncIndexing {
		return nil, ingestOutput{}, fmt.Errorf("sync indexing tools are disabled")
	}
	out, err := s.submitIngest(ctx, in.KBID, in.Documents, in.ChunkSize, in.GraphEnabled, in.IdempotencyKey, in.CorrelationID)
	if err != nil {
		return nil, ingestOutput{}, err
	}
	timeout := s.Config.DefaultSyncTimeout
	if in.TimeoutMS > 0 {
		timeout = time.Duration(in.TimeoutMS) * time.Millisecond
	}
	if timeout > s.Config.MaxSyncTimeout {
		timeout = s.Config.MaxSyncTimeout
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(syncIngestPollInterval)
	defer ticker.Stop()
	for {
		status, statusErr := s.operationStatusOutput(ctx, out.EventID)
		switch {
		case statusErr == nil:
			out.Stages = status.Stages
			out.Terminal = status.Terminal
			if status.Terminal != nil {
				out.Status = "completed"
				return nil, out, nil
			}
		case errors.Is(statusErr, kb.ErrEventNotFound):
			// Event not yet visible (eventually-consistent inbox); keep polling.
		default:
			// Misconfiguration or backend failure: surface immediately rather
			// than spin until MaxSyncTimeout.
			return nil, ingestOutput{}, statusErr
		}
		if time.Now().After(deadline) {
			out.TimedOut = true
			out.Status = "timeout"
			return nil, out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ingestOutput{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

const syncIngestPollInterval = 100 * time.Millisecond

func (s *Service) submitIngest(ctx context.Context, rawKBID string, docsIn []ingestDocInput, chunkSize int, graphEnabled bool, idem, corr string) (ingestOutput, error) {
	if s.Config.ReadOnly || !s.Config.AllowIndexing {
		return ingestOutput{}, fmt.Errorf("indexing tools are disabled")
	}
	if s.AppendDocumentUpsert == nil {
		return ingestOutput{}, fmt.Errorf("ingest event pipeline not configured")
	}
	kbID := defaultKBID(rawKBID)
	if len(docsIn) == 0 {
		return ingestOutput{}, fmt.Errorf("documents are required")
	}
	docs := make([]kb.Document, 0, len(docsIn))
	docIDs := make([]string, 0, len(docsIn))
	for _, d := range docsIn {
		id := strings.TrimSpace(d.ID)
		text := strings.TrimSpace(d.Text)
		if id == "" {
			return ingestOutput{}, fmt.Errorf("document id is required")
		}
		if text == "" {
			return ingestOutput{}, fmt.Errorf("document text is required")
		}
		docs = append(docs, kb.Document{ID: id, Text: text, MediaIDs: d.MediaIDs, MediaRefs: d.MediaRefs, Metadata: d.Metadata})
		docIDs = append(docIDs, id)
	}
	graph := graphEnabled
	evtID, effectiveIdem, err := s.AppendDocumentUpsert(ctx, kb.DocumentUpsertPayload{
		KBID: kbID, Documents: docs, ChunkSize: chunkSize, Options: kb.UpsertDocsOptions{GraphEnabled: &graph},
	}, strings.TrimSpace(idem), strings.TrimSpace(corr))
	if err != nil {
		return ingestOutput{}, err
	}
	return ingestOutput{EventID: evtID, Status: "accepted", KBID: kbID, DocumentCount: len(docs), DocIDs: docIDs, IdempotencyKey: effectiveIdem}, nil
}

type operationStatusInput struct {
	EventID string `json:"event_id"`
}
type operationStatusOutput struct {
	EventID  string           `json:"event_id"`
	Root     map[string]any   `json:"root,omitempty"`
	Terminal map[string]any   `json:"terminal,omitempty"`
	Stages   []map[string]any `json:"stages,omitempty"`
}

func (s *Service) operationStatus(ctx context.Context, _ *mcp.CallToolRequest, in operationStatusInput) (*mcp.CallToolResult, operationStatusOutput, error) {
	out, err := s.operationStatusOutput(ctx, strings.TrimSpace(in.EventID))
	return nil, out, err
}

func (s *Service) operationStatusOutput(ctx context.Context, eventID string) (operationStatusOutput, error) {
	if eventID == "" {
		return operationStatusOutput{}, fmt.Errorf("event_id required")
	}
	if s.GetEvent == nil {
		return operationStatusOutput{}, fmt.Errorf("event subsystem not configured")
	}
	ev, err := s.GetEvent(ctx, eventID)
	if err != nil {
		return operationStatusOutput{}, err
	}
	out := operationStatusOutput{EventID: eventID, Root: eventPayload(ev)}
	if s.FindOperationTerminal != nil {
		terminal, terminalErr := s.FindOperationTerminal(ctx, eventID)
		switch {
		case terminalErr != nil && !errors.Is(terminalErr, kb.ErrEventNotFound):
			s.logWarn(ctx, "operation_status: terminal lookup failed", "event_id", eventID, "error", terminalErr)
		case terminal != nil:
			out.Terminal = eventPayload(terminal)
		}
	}
	if s.OperationStages != nil {
		stages, stagesErr := s.OperationStages(ctx, eventID)
		if stagesErr != nil {
			s.logWarn(ctx, "operation_status: stages lookup failed", "event_id", eventID, "error", stagesErr)
		} else {
			out.Stages = make([]map[string]any, 0, len(stages))
			for _, stage := range stages {
				item := eventPayload(stage.Event)
				if item == nil {
					item = map[string]any{}
				}
				if stage.Failure != nil {
					item["failure"] = eventPayload(stage.Failure)
				}
				out.Stages = append(out.Stages, item)
			}
		}
	}
	return out, nil
}

type listMediaInput struct {
	KBID   string `json:"kb_id"`
	Prefix string `json:"prefix,omitempty"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
type listMediaOutput struct {
	Items []kb.MediaObject `json:"items"`
	Next  string           `json:"next,omitempty"`
	Limit int              `json:"limit"`
}

func (s *Service) listMedia(ctx context.Context, _ *mcp.CallToolRequest, in listMediaInput) (*mcp.CallToolResult, listMediaOutput, error) {
	if s.ListMedia == nil {
		return nil, listMediaOutput{}, fmt.Errorf("media subsystem not configured")
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 500
	}
	page, err := s.ListMedia(ctx, defaultKBID(in.KBID), in.Prefix, in.After, limit)
	return nil, listMediaOutput{Items: page.Items, Next: page.NextToken, Limit: limit}, err
}

type mediaIDInput struct {
	MediaID string `json:"media_id"`
}
type mediaOutput struct {
	Media   *kb.MediaObject `json:"media,omitempty"`
	Deleted bool            `json:"deleted,omitempty"`
}

func (s *Service) getMedia(ctx context.Context, _ *mcp.CallToolRequest, in mediaIDInput) (*mcp.CallToolResult, mediaOutput, error) {
	if s.GetMedia == nil {
		return nil, mediaOutput{}, fmt.Errorf("media subsystem not configured")
	}
	media, err := s.GetMedia(ctx, strings.TrimSpace(in.MediaID))
	return nil, mediaOutput{Media: media}, err
}

func (s *Service) deleteMedia(ctx context.Context, _ *mcp.CallToolRequest, in mediaIDInput) (*mcp.CallToolResult, mediaOutput, error) {
	if !s.Config.AllowDestructive || s.Config.ReadOnly {
		return nil, mediaOutput{}, fmt.Errorf("destructive tools are disabled")
	}
	if s.DeleteMedia == nil {
		return nil, mediaOutput{}, fmt.Errorf("media subsystem not configured")
	}
	if err := s.DeleteMedia(ctx, strings.TrimSpace(in.MediaID)); err != nil {
		return nil, mediaOutput{}, err
	}
	return nil, mediaOutput{Deleted: true}, nil
}

type emptyInput struct{}
type statusOutput struct {
	Status string `json:"status"`
}

func (s *Service) sweepCache(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, statusOutput, error) {
	if !s.Config.AllowAdmin {
		return nil, statusOutput{}, fmt.Errorf("admin tools are disabled")
	}
	if s.SweepCache == nil {
		return nil, statusOutput{}, fmt.Errorf("kb unavailable")
	}
	return nil, statusOutput{Status: "ok"}, s.SweepCache(ctx)
}

func (s *Service) clearCache(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, statusOutput, error) {
	if !s.Config.AllowAdmin || !s.Config.AllowDestructive || s.Config.ReadOnly {
		return nil, statusOutput{}, fmt.Errorf("admin destructive tools are disabled")
	}
	if s.ClearCache == nil {
		return nil, statusOutput{}, fmt.Errorf("kb unavailable")
	}
	return nil, statusOutput{Status: "ok"}, s.ClearCache(ctx)
}

type kbIDInput struct {
	KBID string `json:"kb_id"`
}
type compactionOutput struct {
	Result *kb.CompactionPublishResult `json:"result,omitempty"`
}

func (s *Service) forceCompaction(ctx context.Context, _ *mcp.CallToolRequest, in kbIDInput) (*mcp.CallToolResult, compactionOutput, error) {
	if !s.Config.AllowAdmin {
		return nil, compactionOutput{}, fmt.Errorf("admin tools are disabled")
	}
	if s.ForceCompaction == nil {
		return nil, compactionOutput{}, fmt.Errorf("compaction unavailable")
	}
	kbID := strings.TrimSpace(in.KBID)
	if kbID == "" {
		return nil, compactionOutput{}, fmt.Errorf("kb_id required")
	}
	result, err := s.ForceCompaction(ctx, kbID)
	return nil, compactionOutput{Result: result}, err
}

type deleteKBOutput struct {
	Deleted bool   `json:"deleted"`
	KBID    string `json:"kb_id"`
}

func (s *Service) deleteKnowledgeBase(ctx context.Context, _ *mcp.CallToolRequest, in kbIDInput) (*mcp.CallToolResult, deleteKBOutput, error) {
	if !s.Config.AllowDestructive || s.Config.ReadOnly {
		return nil, deleteKBOutput{}, fmt.Errorf("destructive tools are disabled")
	}
	if s.DeleteKnowledgeBase == nil {
		return nil, deleteKBOutput{}, fmt.Errorf("delete knowledge base unavailable")
	}
	kbID := strings.TrimSpace(in.KBID)
	if kbID == "" {
		return nil, deleteKBOutput{}, fmt.Errorf("kb_id required")
	}
	if err := s.DeleteKnowledgeBase(ctx, kbID); err != nil {
		return nil, deleteKBOutput{}, err
	}
	return nil, deleteKBOutput{Deleted: true, KBID: kbID}, nil
}

func parseSearchMode(raw string) (kb.SearchMode, string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
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

func defaultKBID(raw string) string {
	if s := strings.TrimSpace(raw); s != "" {
		return s
	}
	return "default"
}

// logWarn emits a warn-level log via the configured slog.Logger when one is
// set, falling back to slog.Default(). Centralized so MCP handlers always have
// somewhere to surface non-fatal lookup failures instead of swallowing them.
func (s *Service) logWarn(ctx context.Context, msg string, attrs ...any) {
	if s == nil {
		return
	}
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.WarnContext(ctx, msg, attrs...)
}

func eventPayload(ev *kb.KBEvent) map[string]any {
	if ev == nil {
		return nil
	}
	out := map[string]any{"event_id": ev.EventID, "kb_id": ev.KBID, "kind": ev.Kind, "status": ev.Status, "attempt": ev.Attempt, "correlation_id": ev.CorrelationID, "causation_id": ev.CausationID, "created_at": ev.CreatedAt, "last_error": ev.LastError}
	switch ev.Kind {
	case kb.EventKBPublished:
		var payload kb.KBPublishedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["document_count"] = payload.DocumentCount
			out["chunk_count"] = payload.ChunkCount
			out["media_ids"] = payload.MediaIDs
			out["file_results"] = payload.FileResults
		}
	case kb.EventMediaUploaded:
		var payload kb.MediaUploadedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["media_id"] = payload.MediaID
			out["filename"] = payload.Filename
		}
	case kb.EventWorkerFailed:
		var payload kb.WorkerFailedPayload
		if json.Unmarshal(ev.Payload, &payload) == nil {
			out["stage"] = payload.Stage
			out["will_retry"] = payload.WillRetry
			out["file_results"] = payload.FileResults
		}
	}
	return out
}

func isNotFound(err error) bool {
	return errors.Is(err, kb.ErrEventNotFound) || errors.Is(err, kb.ErrMediaNotFound)
}
