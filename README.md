# Minnow

> [!WARNING]
> **Breaking changes expected.** The on-disk format, event schemas, and public API are still moving. Do not pin a production system to a commit SHA without pinning its data too. DuckDB VSS (HNSW) is an experimental upstream dependency - tail latency at 1M+ docs / 768 dim is a known soft spot (see [`BENCHMARK.md`](BENCHMARK.md)).

Embedded vector search for Go. DuckDB-backed, HNSW-indexed, multi-tenant via per-knowledge-base isolation. A small self-hosted alternative to managed vector databases.

## Quickstart

One command installs the `codeindex` CLI and starts a local Minnow in Docker:

```bash
curl -fsSL https://raw.githubusercontent.com/mikills/minnow/main/install.sh | sh
```

It defaults to [Ollama](https://ollama.com) for embeddings, so no API key is
needed. Run `ollama pull all-minilm` first. To use OpenAI instead:

```bash
curl -fsSL https://raw.githubusercontent.com/mikills/minnow/main/install.sh |
  MINNOW_EMBEDDER=openai OPENAI_API_KEY=sk-... sh
```

Switching embedders changes the vector width, so an index built with one is not
readable by the other. Pick one before indexing, or delete the `minnow-data`
volume when you switch.

To run the server without the installer:

```bash
docker compose up -d              # pulls ghcr.io/mikills/minnow
curl -s http://127.0.0.1:8080/healthz
```

From a source checkout, `make run` serves the same thing, and
`docker compose -f compose.yaml -f compose.build.yaml up --build` builds the
image locally instead of pulling it.

For globally installed MCP use with OpenAI embeddings:

```bash
# From this checkout (links the pinned DuckDB 2.0 alpha):
make install
go install github.com/mikills/minnow/codeindex@latest
$(go env GOPATH)/bin/minnow setup
minnow config init dev-openai
OPENAI_API_KEY=sk-... minnow mcp stdio
```

`minnow setup` checks whether Go's install directory is on your shell `PATH` and
can update your shell profile for you. Install `codeindex` separately when you
want codebase indexing. If `minnow` is already on `PATH`, just run `minnow setup`
directly.

Index a codebase through a running Minnow HTTP service. Codeindex keeps its own
connection config and never reads `minnow.yaml`:

```bash
go install github.com/mikills/minnow/codeindex@latest
minnow                                           # serves http://127.0.0.1:8080
codeindex setup --minnow-url http://127.0.0.1:8080
codeindex codebase                               # indexes the current branch
codeindex hooks install                          # refreshes after Git changes
```

Register `codeindex mcp` with a coding agent for read-only, branch-aware search.
It resolves the local repository and branch, then calls hosted Minnow over
HTTP. Git branches share repository vectors and use separate document scopes,
so unchanged chunks are reused while searches remain branch-aware. Outside Git,
identity is derived from the absolute directory.

Codeindex stores its connection at the user config path and branch state under
`.minnow/codeindex/`, which it adds to Git's repository-local excludes.
Completed files and acknowledged batches are journaled so an interrupted
refresh can continue without repeating confirmed work.
Override discovery with `CODEINDEX_CONFIG`, the endpoint
with `CODEINDEX_MINNOW_URL`, or either identity with `--kb`/`--index-key`.

The default `minnow.yaml` also exposes MCP for coding agents:

- Streamable HTTP: `http://127.0.0.1:8080/mcp`
- Stdio: `make run ARGS="mcp stdio"`

Codex CLI, Claude Code, and OpenCode can connect directly to a hosted streamable
HTTP endpoint; see [MCP endpoints](docs/getting-started.md#mcp-endpoints) for
verified registration commands and token handling.

The default config at `./minnow.yaml` is sufficient for local development (embedder-only, no external services). See [`docs/getting-started.md`](docs/getting-started.md) for a deployment-grade setup.

### DuckDB 2.0 preview

This checkout uses **DuckDB v2.0.0-alpha45672**, not a stable 2.0 release.
`make build`, `make run`, `make install`, and `make test` link the actual native
library using `scripts/duckdb.sh`. The first build downloads a checksum-pinned
static archive into `.cache/duckdb/`; later builds and runtime extension loads
work offline. Docker uses the same setup. Supported platforms are macOS ARM64
and Linux AMD64.

For other Go commands, use `bash scripts/duckdb.sh test ./kb/duckdb/...` (or
`build`, `run`, `install`, etc.). Plain `go build`/`go install` still select an
upstream preview archive containing DuckDB 1.5.4; database configuration rejects
that mismatch rather than silently running the wrong engine.

The bundled 2.0 extensions are `vss`, `fts`, and `httpfs`. DuckPGQ is not bundled
for this alpha; graph expansion uses the existing BFS implementation by default.
Keep `UseDuckPGQ` false. Back up existing data before trying an alpha; don't use
it for production or assume files written by 2.0 can be reopened with 1.5.

On Apple Silicon, Docker Desktop's default AMD64 emulation can report invalid
extension signatures for this preview even when the signatures are valid.
The same Linux binaries load successfully under QEMU. Use native Linux AMD64
or QEMU for container validation; do not enable `allow_unsigned_extensions`.

## What it does

- Vector and graph RAG over your corpus, exposed at `/rag/query` and `/rag/ingest`.
- Code indexing for coding agents: the separate `codeindex` CLI indexes the current repo (30+ file types; symbol-aware chunking for Go, JS/TS, Python, and Rust) through Minnow, with optional git hooks to keep it fresh.
- Per-tenant isolation through knowledge bases. Each one has its own manifest, shards, and HNSW indexes.
- Local, S3-backed, and [tiered storage](examples/minnow.tiered.yaml): tiered mode combines a per-shard SSD warm cache, a replaceable persistent local replication journal, and an S3 cold store.
- Event-driven ingest pipeline with at-least-once delivery, durable operation lineage, and retry semantics.

## Deployment

Run the container locally with persistent storage:

```bash
export OPENAI_API_KEY=sk-...
docker compose up --build -d
curl http://127.0.0.1:8080/healthz
```

Ready-to-customize examples are available for [Docker Compose, Fly.io, and AWS
Terraform](deploy/README.md). The container currently targets Linux x86-64
because that is the platform covered by the bundled DuckDB extensions.

## Documentation

- [Getting started](docs/getting-started.md) - install, configure, first request.
- [MCP](docs/mcp.md) - code search, hosted access, client setup, and tool permissions.
- [Architecture](docs/architecture.md) - components, concurrency, graph extraction.
- [Data and pipeline](docs/data-lifecycle.md) - storage model, write pipeline, event model, query path.
- [Configuration reference](docs/configuration.md) - every YAML knob.
- [Benchmark](BENCHMARK.md) - query latency, ingest throughput, sizing estimates, S3 vs local cost comparison.

## Status

Production-ready for small-to-medium corpora (up to ~1M docs per knowledge base). See [`BENCHMARK.md`](BENCHMARK.md) for latency numbers and the known-slow scenarios.

## License

Apache 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). Vendored code under `kb/internal/cron/` remains under its original MIT license (PocketBase); the MIT header stays on those files and the attribution is captured in `NOTICE`.
