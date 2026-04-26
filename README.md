# Minnow

> [!WARNING]
> **Breaking changes expected.** The on-disk format, event schemas, and public API are still moving. Do not pin a production system to a commit SHA without pinning its data too. DuckDB VSS (HNSW) is an experimental upstream dependency - tail latency at 1M+ docs / 768 dim is a known soft spot (see [`BENCHMARK.md`](BENCHMARK.md)).

Embedded vector search for Go. DuckDB-backed, HNSW-indexed, multi-tenant via per-knowledge-base isolation. A small self-hosted alternative to managed vector databases.

## Quickstart

```bash
go run .
curl -s http://127.0.0.1:8080/healthz
```

For globally installed MCP use with OpenAI embeddings:

```bash
go install github.com/mikills/minnow@latest
minnow config init dev-openai
OPENAI_API_KEY=sk-... minnow mcp stdio
```

The default `minnow.yaml` also exposes MCP for coding agents:

- Streamable HTTP: `http://127.0.0.1:8080/mcp`
- Stdio: `go run . mcp stdio`

The default config at `./minnow.yaml` is sufficient for local development (embedder-only, no external services). See [`docs/getting-started.md`](docs/getting-started.md) for a deployment-grade setup.

## What it does

- Vector and graph RAG over your corpus, exposed at `/rag/query` and `/rag/ingest`.
- Per-tenant isolation through knowledge bases. Each one has its own manifest, shards, and HNSW indexes.
- Two storage modes: local disk for always-hot workloads, S3-backed for SaaS with a long-tail distribution of cold tenants.
- Event-driven ingest pipeline with at-least-once delivery, durable operation lineage, and retry semantics.

## Documentation

- [Getting started](docs/getting-started.md) - install, configure, first request.
- [Architecture](docs/architecture.md) - components, concurrency, graph extraction.
- [Data and pipeline](docs/data-lifecycle.md) - storage model, write pipeline, event model, query path.
- [Configuration reference](docs/configuration.md) - every YAML knob.
- [Benchmark](BENCHMARK.md) - query latency, ingest throughput, sizing estimates, S3 vs local cost comparison.

## Status

Production-ready for small-to-medium corpora (up to ~1M docs per knowledge base). See [`BENCHMARK.md`](BENCHMARK.md) for latency numbers and the known-slow scenarios.

## License

Apache 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). Vendored code under `kb/internal/cron/` remains under its original MIT license (PocketBase); the MIT header stays on those files and the attribution is captured in `NOTICE`.
