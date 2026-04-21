# Getting Started

## Prerequisites

- Go toolchain matching the module's version.
- CGO-capable environment for the DuckDB driver.
- DuckDB extensions in `extensions/` (shipped with the repo; no network access needed by default).

## Configuration

minnow is configured from a single YAML file, discovered at:

1. `$MINNOW_CONFIG` if set, or
2. `./minnow.yaml` in the process working directory.

See [configuration.md](configuration.md) for the full schema and
[`examples/minnow.min.yaml`](../examples/minnow.min.yaml) /
[`examples/minnow.yaml`](../examples/minnow.yaml) for ready-to-copy starting
points.

Only two environment variables are read directly by the binary; every other
deployment knob lives in the YAML.

| Env var             | Purpose                                                  |
| ------------------- | -------------------------------------------------------- |
| `MINNOW_CONFIG`     | Path to the YAML config. Defaults to `./minnow.yaml`.    |
| `MINNOW_LOG_FORMAT` | Logger format (`text` / `json`). Read before the config. |

Secret values (Mongo URI, tokens) are referenced from YAML via `${VAR}`
interpolation and set as regular environment variables.

## Run locally

```bash
cp examples/minnow.min.yaml minnow.yaml
go run .
```

The default bind address is `127.0.0.1:8080` (override `http.address` in the YAML).

## Validate a config

Before rolling out a config, run the built-in validator. It loads, interpolates
`${VAR}` references, applies defaults, and dry-runs the runtime builder - no
Mongo connection, no port bind.

```bash
go run . config validate ./minnow.yaml
# => config OK
```

The validator exits 1 on any error; wire it into CI to gate merges.

## Send a first request

Health check:

```bash
curl -s http://127.0.0.1:8080/healthz
# => {"status":"ok"}
```

`POST /rag/ingest` and `POST /rag/media/upload` are asynchronous and return an
operation handle. Poll `GET /rag/operations/:id` for terminal status.

## Optional: MongoDB for durable event state

Without a `mongo` block, minnow runs in local/dev mode: manifests are blob-backed
(and survive restarts as long as the blob root does), while the event store and
inbox are in-memory (and reset on restart). To make event and inbox state durable,
add a `mongo` block:

```yaml
mongo:
  uri: ${MINNOW_MONGO_URI}
  database: minnow
  collections:
    manifests: manifests
    events: kb_events
    inbox: kb_event_inbox
    media: media
```

Media wiring follows `media.enabled` independently of Mongo: with media
disabled, `/rag/media/*` routes return `503`.

See [configuration.md](configuration.md) for the full set of fields.
