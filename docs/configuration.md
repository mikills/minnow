# minnow Configuration Reference

minnow is configured from a single YAML file, discovered at:

1. `$MINNOW_CONFIG` if set, or
2. `./minnow.yaml` in the process working directory.

If neither is present the process exits with a clear error. See
[`examples/minnow.min.yaml`](../examples/minnow.min.yaml) for the smallest valid
file and [`examples/minnow.yaml`](../examples/minnow.yaml) for a full-field
reference.

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
| `MINNOW_CONFIG`     | Path to the YAML config file. Defaults to `./minnow.yaml`.                 |
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

| Field            | Type   | Default                    | Notes                                          |
| ---------------- | ------ | -------------------------- | ---------------------------------------------- |
| `provider`       | enum   | `ollama`                   | `ollama` \| `local`.                           |
| `ollama.url`     | string | `http://localhost:11434`   | Required when `provider = ollama`.             |
| `ollama.model`   | string | `all-minilm`               |                                                |
| `local.dim`      | int    | `384`                      | Required (> 0) when `provider = local`.        |

The `ollama` / `local` block must be present when its provider is selected; the
loader does not auto-create an empty block.

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

## Secret policy

- Do not commit inline secret values to the repo.
- Fields that carry credentials (notably `mongo.uri`) may exist in YAML, but
  production configs must reference them via `${VAR}`.
