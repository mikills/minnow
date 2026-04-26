# minnow Configuration Reference

minnow is configured from a single YAML file, discovered at:

1. `$MINNOW_CONFIG` if set, or
2. `./minnow.yaml` in the process working directory, or
3. the per-user config path (`~/Library/Application Support/minnow/minnow.yaml`
   on macOS, `~/.config/minnow/minnow.yaml` on Linux).

If none are present the process exits with a clear error. See
[`examples/minnow.min.yaml`](../examples/minnow.min.yaml) for the smallest valid
file, [`examples/minnow.dev.openai.yaml`](../examples/minnow.dev.openai.yaml)
for OpenAI-backed MCP development, and [`examples/minnow.yaml`](../examples/minnow.yaml)
for a full-field reference.

For globally installed MCP use, generate a starter OpenAI-backed config with:

```bash
minnow config init dev-openai
```

## Rules

- **Unknown keys are rejected.** A typo like `concurency` fails the load at
  startup with the line number and key path.
- **Durations are strings** in Go's `time.ParseDuration` form: `500ms`, `5s`,
  `5m`, `2h30m`.
- **Secrets stay in the environment.** Any value sourced from a secret (Mongo
  URI, API tokens, credentials) must be written as `${VAR}` and `VAR` must be
  set before startup. Unresolved `${VAR}` references aggregate into a single
  error listing every missing name.
- **Relative paths** (`storage.blob.root`, `storage.cache.dir`,
  `format.duckdb.extension_dir`) resolve against the YAML file's directory,
  not the process working directory. Paths that resolve *outside* the config's
  base directory (e.g. `root: ../../etc/foo`) are rejected. Explicit absolute
  paths are allowed and audit-logged.
- **Only one YAML document per file.** A second `---` block fails the load.

## Bootstrap environment variables

These two env vars are read before the config file:

| Env var             | Purpose                                                                    |
| ------------------- | -------------------------------------------------------------------------- |
| `MINNOW_CONFIG`     | Path to the YAML config file. Overrides default discovery.                 |
| `MINNOW_LOG_FORMAT` | Logger format (`text` / `json`). Read before the YAML so parse errors log.|

All other deployment knobs are YAML fields.

## Schema

### `http`

| Field                 | Type     | Default           | Notes                            |
| --------------------- | -------- | ----------------- | -------------------------------- |
| `address`             | string   | `127.0.0.1:8080`  | Bind address.                    |
| `read_header_timeout` | duration | `5s`              |                                  |
| `shutdown_timeout`    | duration | `5s`              |                                  |

### `storage.blob`

| Field  | Type   | Default             | Notes                           |
| ------ | ------ | ------------------- | ------------------------------- |
| `kind` | string | `local`             | Only `local` is supported today.|
| `root` | path   | `./.temp/fixtures`  | Relative to YAML file.          |

### `storage.cache`

| Field            | Type     | Default         | Notes                        |
| ---------------- | -------- | --------------- | ---------------------------- |
| `dir`            | path     | `./.temp/cache` | Relative to YAML file.       |
| `max_bytes`      | int      | `0`             | `0` = unbounded.             |
| `entry_ttl`      | duration | `0s`            | `0` = no TTL.                |
| `evict_interval` | duration | `30s`           |                              |

### `format`

| Field                  | Type   | Default         | Notes                              |
| ---------------------- | ------ | --------------- | ---------------------------------- |
| `kind`                 | string | `duckdb`        | Only `duckdb` is supported today.  |
| `duckdb.memory_limit`  | string | `128MB`         | Passed to DuckDB verbatim.         |
| `duckdb.extension_dir` | path   | `./extensions`  | Relative to YAML file.             |
| `duckdb.offline`       | bool   | `false`         | If true, disables extension fetch. |

### `embedder`

| Field                            | Type   | Default                    | Notes                                                   |
| -------------------------------- | ------ | -------------------------- | ------------------------------------------------------- |
| `provider`                       | enum   | `ollama`                   | `ollama` \| `local` \| `openai_compatible`.             |
| `ollama.url`                     | string | `http://localhost:11434`   | Required when `provider = ollama`; uses `/api/embed`.   |
| `ollama.model`                   | string | `all-minilm`               |                                                         |
| `local.dim`                      | int    | `384`                      | Required (> 0) when `provider = local`.                 |
| `openai_compatible.base_url`     | string | `https://api.openai.com/v1` | Required when `provider = openai_compatible`.           |
| `openai_compatible.model`        | string | none                       | Required.                                               |
| `openai_compatible.token`        | string | none                       | Optional bearer token; omit for unauthenticated locals. |
| `openai_compatible.dimensions`   | int    | `0`                        | Optional; `0` omits the request field.                  |

The provider-specific block must be present when its provider is selected; the
loader does not auto-create an empty block. `openai_compatible` calls
`POST {base_url}/embeddings`, so use `base_url: http://localhost:11434/v1` for
Ollama's OpenAI-compatible API.

### `graph` (optional, RAG graph extraction)

| Field         | Type   | Default                  | Notes |
| ------------- | ------ | ------------------------ | ----- |
| `enabled`     | bool   | `false`                  |       |
| `url`         | string | `http://localhost:11434` |       |
| `model`       | string | `llama3`                 |       |
| `parallelism` | int    | `2`                      |       |

### `mongo` (optional - omit for local/dev mode)

Mongo controls **persistence**, not whether the event pipeline exists. The event
store, inbox, and worker pools always run; Mongo makes their state durable
across restarts.

- **Omit the `mongo` block** - local/dev mode:
  - **Manifests**: blob-backed under `storage.blob.root`. Survive restarts as
    long as the blob root persists.
  - **Event store + inbox**: in-memory. `/rag/ingest` returns 202 and workers
    process events, but the event log resets on restart.
  - **Media store** (when `media.enabled: true`): in-memory metadata index;
    uploaded bytes still land in the blob store and survive restarts.
- **Include the `mongo` block** - manifests, events, inbox, and (when enabled)
  media metadata move to Mongo collections. `uri` is required.

If `media.enabled: false`, no media store is wired regardless of Mongo, and
`/rag/media/*` routes return 503.

| Field                    | Type   | Default          | Notes                                |
| ------------------------ | ------ | ---------------- | ------------------------------------ |
| `uri`                    | string | -                | Required. Use `${VAR}` for secrets.  |
| `database`               | string | `minnow`         |                                      |
| `collections.manifests`  | string | `manifests`      |                                      |
| `collections.events`     | string | `kb_events`      |                                      |
| `collections.inbox`      | string | `kb_event_inbox` |                                      |
| `collections.media`      | string | `media`          |                                      |

The four `collections.*` names must be distinct - duplicates are rejected at
startup to prevent silent cross-store corruption.

### `scheduler`

| Field             | Type         | Default | Notes                                   |
| ----------------- | ------------ | ------- | --------------------------------------- |
| `enabled`         | bool         | `false` | Set to `true` in production deployments.|
| `tick_interval`   | duration     | -       | Required when `enabled: true`.          |
| `disabled_jobs`   | list[string] | `[]`    | Job IDs to skip registration for.       |

When `scheduler.enabled: true`, `tick_interval` must be set explicitly;
startup fails fast if it is missing.

### `workers`

| Field                                    | Type     | Default | Notes                        |
| ---------------------------------------- | -------- | ------- | ---------------------------- |
| `defaults.max_attempts`                  | int      | `5`     |                              |
| `defaults.poll_interval`                 | duration | `500ms` |                              |
| `defaults.visibility_timeout`            | duration | `5m`    |                              |
| `document_upsert.concurrency`            | int      | `4`     |                              |
| `document_chunked.concurrency`           | int      | `4`     |                              |
| `document_publish.concurrency`           | int      | `2`     |                              |
| `media_upload.concurrency`               | int      | `2`     |                              |

Any pool may set `max_attempts`, `poll_interval`, or `visibility_timeout` to
override the `defaults` block for that one pool.

### `media`

When `enabled` is `false`, no media store is wired and every `/rag/media/*`
route returns `503 Service Unavailable`. The other defaults in this block only
take effect when `enabled` is `true`.

| Field                     | Type         | Default    | Notes                                    |
| ------------------------- | ------------ | ---------- | ---------------------------------------- |
| `enabled`                 | bool         | `false`    | Set `true` to accept media uploads.      |
| `max_bytes`               | int          | `10485760` | Max upload size (10 MiB). Must be > 0 when `enabled: true`. |
| `content_type_allowlist`  | list[string] | `[]`       | Empty = accept any MIME type.            |
| `pending_ttl`             | duration     | `24h`      | Unreferenced uploads past this are GC'd. |
| `tombstone_grace`         | duration     | `1h`       | Grace before hard-delete. Must be `<= pending_ttl`. |
| `upload_completion_ttl`   | duration     | `15m`      | Upload completion window.                |

Constraints when `media.enabled: true`:

- `max_bytes` must be > 0. A zero is replaced with the default (10 MiB) at load
  time; a negative value is rejected.
- `tombstone_grace` must be `<=` `pending_ttl`.

### `sharding`

Presence of a key means "explicit"; omit a key to accept the default.

| Field                               | Type    | Default    | Constraint                                             |
| ----------------------------------- | ------- | ---------- | ------------------------------------------------------ |
| `shard_trigger_bytes`               | int     | `67108864` | > 0 when set.                                          |
| `shard_trigger_vector_rows`         | int     | `150000`   | > 0 when set.                                          |
| `target_shard_bytes`                | int     | `33554432` | > 0 when set.                                          |
| `max_vector_rows_per_shard`         | int     | `75000`    | > 0 when set.                                          |
| `query_shard_fanout`                | int     | `4`        | > 0 and ≤ 64.                                          |
| `query_shard_fanout_adaptive_max`   | int     | `6`        | ≤ 64 and ≥ `query_shard_fanout`.                       |
| `query_shard_parallelism`           | int     | `4`        | > 0 when set.                                          |
| `query_shard_local_topk_multiplier` | int     | `2`        | > 0 and ≤ 16.                                          |
| `small_kb_max_shards`               | int     | `2`        | > 0 when set.                                          |
| `compaction_enabled`                | bool    | `true`     |                                                        |
| `compaction_min_shard_count`        | int     | `8`        | > 0 when set.                                          |
| `compaction_tombstone_ratio`        | float   | `0.20`     | `(0, 1]`.                                              |

### `mcp`

MCP exposes Minnow for LLM agents. The schema default is disabled, 
but the repository's local developer configs
(`minnow.yaml`, `examples/minnow.min.yaml`, `examples/minnow.dev.openai.yaml`)
ship with MCP enabled and retrieval + indexing tools allowed. Destructive and
admin tools remain opt-in.

**Schema vs shipped defaults**: the table below documents schema defaults that
apply when `mcp.enabled` is `true` but a field is omitted. The shipped local
configs override `enabled` to `true` and `allow_indexing`/`allow_sync_indexing`
to `true` for developer ergonomics; production configs should set every field
explicitly.

The MCP `tools/list` reflects only the tools the gates allow: tools whose
required gate (e.g. `allow_destructive`, `allow_admin`) is off are not
registered. Agents therefore see exactly the surface they may call instead of
discovering tools that always error.

| Field                  | Type         | Default             | Notes |
| ---------------------- | ------------ | ------------------- | ----- |
| `enabled`              | bool         | `false`             | Enables MCP config and validation. Local examples set this to `true`. |
| `transports`           | list[string] | `[stdio, http]`     | Allowed values: `stdio`, `http`. Applies when enabled. |
| `http_path`            | string       | `/mcp`              | Streamable HTTP endpoint path. Must start with `/`. |
| `read_only`            | bool         | `false`             | Blocks indexing, destructive, and admin tools. |
| `allow_indexing`       | bool         | `false`             | Enables async document indexing tools. |
| `allow_sync_indexing`  | bool         | `false`             | Enables bounded wait indexing. Requires `allow_indexing`. |
| `allow_destructive`    | bool         | `false`             | Enables delete/tombstone tools. Keep off unless explicitly needed. |
| `allow_admin`          | bool         | `false`             | Enables maintenance tools such as cache sweep and compaction. |
| `default_sync_timeout` | duration     | `30s`               | Default wait for sync indexing. |
| `max_sync_timeout`     | duration     | `2m`                | Upper bound for caller-requested sync indexing waits. |
| `http_json_response`   | bool         | `false`             | Prefer JSON responses over SSE where the SDK supports it. |
| `http_stateless`       | bool         | `false`             | Runs HTTP MCP requests without retained sessions. |

Stdio mode is launched with:

```bash
go run . mcp stdio
```

In stdio mode, Minnow writes logs to stderr so stdout is reserved for MCP
messages.

Example local agent config:

```yaml
mcp:
  enabled: true
  transports: [stdio, http]
  http_path: /mcp
  allow_indexing: true
  allow_sync_indexing: true
  allow_admin: true
  allow_destructive: false
```

Destructive tools (`minnow_delete_knowledge_base`, `minnow_delete_media`,
`minnow_clear_cache`) require `allow_destructive: true`; admin maintenance tools
require `allow_admin: true`.

For editor MCP registration, prefer stdio:

```json
{
  "mcpServers": {
    "minnow": {
      "command": "minnow",
      "args": ["mcp", "stdio"],
      "env": {
        "MINNOW_CONFIG": "/path/to/minnow.yaml"
      }
    }
  }
}
```

## Secret policy

- Do not commit inline secret values to the repo.
- Fields that carry credentials (notably `mongo.uri`) may exist in YAML, but
  production configs must reference them via `${VAR}`.
