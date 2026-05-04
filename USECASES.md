# Use Cases

Practical workflows for Minnow beyond the synthetic benchmarks in [`BENCHMARK.md`](BENCHMARK.md).

## Codebase Indexing

Minnow can index a local repository for MCP/code-agent retrieval. Agents should use a stable `index_key` from the repo-local registry instead of remembering the backing `kb_id`.

Example registry entry:

```json
{
  "schema_version": "minnow.codebase_indexes/v1",
  "codebase_indexes": {
    "default": {
      "kb_id": "minnow-source-e2e",
      "root": ".",
      "description": "Default source code index for Minnow repo",
      "include_untracked": true
    }
  }
}
```

## Minnow Source Tree

Environment:

- Apple M4, local SSD
- OpenAI `text-embedding-3-small` via `openai_compatible`
- DuckDB memory limit: 2 GB
- Include: `**/*.go`, `**/*.md`, `**/*.yaml`, `**/*.yml`
- Key: `default`; KB: `minnow-source-e2e`

Corpus:

| Files | Lines | Bytes | Chunks | Avg chunks/file | Chunks/KLOC |
|-------|-------|-------|--------|-----------------|-------------|
| 110 | 22,601 | 714,481 | 1,360 | 12.4 | 60.2 |

Timings:

| Case | Wall time | Files/s | Chunks/s | Notes |
|------|-----------|---------|----------|-------|
| First index | 5m 56s | 0.31 | 3.82 | Embeds and writes chunks. |
| No-op refresh | 0.26s | 423 | n/a | Hash check only. |

No-op result: `110` unchanged files, `0` indexed files, `0` new chunks.

## Django SWE-Bench Repo

Repository: `django__django-11333` at commit `55b68de643b5c2d5f0a8ea7587ab3b2966021ccc`.

Environment:

- Apple M4, local SSD
- OpenAI `text-embedding-3-small` via `openai_compatible`
- DuckDB memory limit: 4 GB
- Source-focused default includes/excludes
- Key: `django-11333-full-source-focused`; KB: `swe-django-11333-full-source-focused`

Corpus:

| Files | Skipped | Chunks | Avg chunks/file |
|-------|---------|--------|-----------------|
| 2,091 | 4,014 | 33,861 | 16.2 |

Timings:

| Case | Wall time | Files/s | Chunks/s | Notes |
|------|-----------|---------|----------|-------|
| First index | 8m 26s | 4.1 | 66.9 | Streams embedding batches into DuckDB. |
| No-op refresh | 2.41s | 868 | n/a | Hash check only. |

No-op result: `2,091` unchanged files, `0` indexed files, `0` new chunks.

## Defaults Learned

Avoid broad `include: ["**/*"]` unless paired with strong excludes. It can pull in binaries, fixtures, generated files, and large artifacts.

Minnow defaults are source-focused: common code, docs, and config files. Default excludes skip binaries, generated outputs, vendor/data/fixture directories, minified assets, DuckDB/Parquet files, and likely secrets.
