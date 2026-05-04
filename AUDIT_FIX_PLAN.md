# Diago Audit Fix Plan

Generated from `diago -target ./... -format json -output diago_audit.json -summary-limit -1`.

Goal: drive `diago -target ./...` to pass without regressing `go test ./...` or `go vet ./...`.

## Ground rules

1. Fix correctness and resource-safety findings before style/shape findings.
2. After each batch, run:
   - `gofmt -w <changed go files>`
   - `go test ./...`
   - `go vet ./...`
   - `diago -target ./... -format json -output diago_audit.json -summary-limit -1`
3. Prefer small behavior-preserving refactors over large rewrites.
4. Treat test-only findings separately from production findings.
5. Do not hide findings with comments unless the finding is a proven false positive and no clearer refactor exists.

## Batch 1 — Production resource safety

Fix all high `resource-not-closed` and production `defer-in-loop` / `goroutine-in-loop` findings first.

Files:
- `cmd/handlers_rag.go`
- `kb/file_ingest.go`
- `kb/ollama.go`
- `kb/openai_compatible.go`
- `kb/scheduler.go`
- `kb/worker.go`
- `kb/duckdb/artifact_format.go`
- `kb/duckdb/query_vector.go`
- `kb/internal/cron/cron.go`
- `scripts/fetch_corpus/main.go`
- `sim/**/*.go`

Expected result: no production high resource/goroutine/defer findings remain.

## Batch 2 — Current-diff CLI/code-index complexity

Refactor current-feature code before touching older subsystems.

Files/functions:
- `main.go:parseIndexCLIOptions`
  - Replace the large switch with a table-driven parser.
  - Split index option application into helpers.
- `main.go:runIndexRefresh`
  - Split into parse/build/apply-overrides/execute/write-output.
- `kb/code_index.go`
  - Split file into focused files:
    - `code_index.go` orchestration/types
    - `code_index_scan.go`
    - `code_index_chunk.go`
    - `code_index_registry.go`
    - `code_index_search.go`
    - `code_index_resource.go` already exists
  - Split `IndexCodebase` into load/resolve/scan/diff/publish/delete/save helpers.
  - Split `scanCodebase` into path collection and per-file eligibility.
  - Convert `detectCodeLanguage` to table-driven maps.

Expected result: remove `large-file`, critical `IndexCodebase`, high `scanCodebase`, high `detectCodeLanguage`, critical `parseIndexCLIOptions`, high `runIndexRefresh`.

## Batch 3 — HTTP handlers complexity

Files/functions:
- `cmd/handlers_rag.go:registerRagRoutes`
- `cmd/handlers_rag.go:buildMultipartIngestInput`
- `cmd/handlers_media.go:registerMediaRoutes`

Plan:
- Extract each route handler into a named function.
- Move multipart field parsing to small helpers.
- Make file ownership explicit so the function that opens a file either closes it or returns an owned wrapper.

Expected result: remove critical/high complexity and function-length findings in handlers.

## Batch 4 — Config/runtime complexity

Files/functions:
- `kb/config/config.go:applyDefaults`
- `kb/config/validate.go` high complexity validators
- `cmd/configruntime/runtime.go:Build`

Plan:
- Split defaults by section (`applyHTTPDefaults`, `applyStorageDefaults`, etc.).
- Split runtime build into stores/embedder/app/scheduler/worker assembly helpers.
- Keep tests around default values and validation errors.

Expected result: remove critical `applyDefaults`, high validator/runtime complexity.

## Batch 5 — Graph/DuckDB internals

Files/functions:
- `kb/graph_pipeline.go:buildGraph`
- `kb/duckdb/graph_store.go`
- `kb/duckdb/mutation.go`
- `kb/duckdb/snapshot_shards.go`
- `kb/duckdb/duckdb_connection.go`
- `kb/duckdb/helpers.go` panic findings

Plan:
- Split graph build into chunk/entity/edge/dedupe phases.
- Split graph store row scanning and SQL execution helpers.
- Replace production panics with error-returning helpers where feasible.

Expected result: remove critical graph/DuckDB complexity and panic findings.

## Batch 6 — Admin/cache/media/worker complexity

Files/functions:
- `kb/admin.go:DeleteKnowledgeBase`
- `kb/cache_eviction.go:evictCacheSweepOnce`
- `kb/media_async.go:AppendMediaUploadDetailed`
- `kb/shard_gc.go`
- `kb/worker_publish.go`
- `kb/worker.go`

Plan:
- Extract step functions with explicit result structs.
- Keep tests behavior-focused.

Expected result: remove remaining production high complexity findings.

## Batch 7 — Test-only hygiene

Fix test findings after production code is stable.

Rules:
- `defer-in-loop`: wrap loop body in `t.Run` or helper closure so defer runs per iteration.
- `goroutine-in-loop`: capture loop vars explicitly and use helper functions.
- `background-context`: use a named `ctx := context.Background()` at test boundary or thread test context helper.

Files include:
- `cmd/app*_test.go`
- `kb/*_test.go`
- `kb/duckdb/*_test.go`
- `mcpserver/service_test.go`

## Batch 8 — Medium/low mechanical cleanup

Rules:
- `ignored-call-result`: check errors or explicitly document best-effort cleanup helpers.
- `missing-context-param`: add context where functions perform I/O or blocking work.
- `too-many-returns`: reduce only when readability improves.
- `duplicate-string-literal`: introduce constants for meaningful repeated literals.
- `magic-number`: introduce domain constants only.
- `dead-code`: delete or add tests/uses.
- `comment-debt`: remove TODO-style debt or create tracked issue comments.

## Batch 9 — Exported-surface and coverage

Files/packages:
- `kb/internal/cron`
- `sim`

Plan:
- Add focused unit tests for exported APIs or reduce exported API surface.
- Add coverage for new CLI/code-index helpers introduced in earlier batches.

## Final acceptance

The work is complete when all pass:

```bash
go test ./...
go vet ./...
diago -target ./... -format text -output diago_audit.txt -summary-limit -1
```

And `diago` reports overall PASS.
