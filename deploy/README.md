# Deployment examples

Minnow can run as a single container wherever it has persistent storage and an
embeddings provider.

| Target | Files | Best for | Important limitation |
|---|---|---|---|
| Docker Compose | [`../compose.yaml`](../compose.yaml) | Laptop, home server, VPS | Binds to loopback; add authenticated HTTPS for remote access |
| Fly.io | [`fly/`](fly/) | Fast managed HTTPS deployment | Fly Volume is single-region/single-Machine; included bearer auth is one shared token |
| AWS | [`aws/`](aws/) | Durable VM with EBS, DNS, and bearer-protected HTTPS | Terraform example is single-instance, not HA |

See the [deployment pricing comparison](pricing.md) for baseline Fly.io, AWS,
storage, network, snapshot, and embedding API estimates with source links.

## Container quick start

```bash
export OPENAI_API_KEY=sk-...
docker compose up --build -d
curl http://127.0.0.1:8080/healthz
```

The named `minnow-data` volume contains the blob store and DuckDB query cache.
Back it up before removing volumes. Without MongoDB, queued/in-flight operations
and operation history are in memory and may be lost during restarts; published
manifests and shards remain on the volume. The image runs as UID/GID `10001`, bundles
the tested Linux x86-64 DuckDB extensions, and sets conservative defaults for a
2 GiB memory limit.

The image is currently **linux/amd64 only**. It can run under emulation on an
Apple Silicon development machine, but hosted deployments should select x86-64.
QEMU user-mode emulation rejects the THP policy syscalls, so opt out explicitly
when using it (including QEMU-based Docker on Apple Silicon):

```sh
MINNOW_DISABLE_THP=0 docker compose up -d
```

For `docker run`, pass `-e MINNOW_DISABLE_THP=0`. A requested disable remains
fail-closed on unsupported execution environments.

## Configuration

### Linux transparent huge pages

The runtime and Fly images enable `MINNOW_DISABLE_THP=1`. A small `minnow-exec`
launcher disables transparent huge pages for Minnow before executing the server,
including Go and native-library startup. Docker Compose, Fly and AWS examples
enable it too. This affects Minnow and its descendants, not the host's global
policy or the separate Caddy process.

Set `MINNOW_DISABLE_THP=0` to opt out, or `1` to enable it (`false`/`true` also
work). Opting out preserves the inherited kernel policy. Invalid values or a
failed requested policy change stop startup with an error. Restart to change it.
This is a launcher environment option, not a YAML/DuckDB setting; it does not
replace memory limits or change extension signature verification.

On the Terraform AWS deployment, keep a persistent opt-out in
[`/opt/minnow/compose.override.yaml`](aws/README.md#transparent-huge-page-policy),
not the generated `.env` file that secret refresh replaces.

For a native Linux installation, build and use the same launcher explicitly:

```sh
cc -O2 -Wall -Wextra -Werror deploy/linux/minnow-exec.c -o minnow-exec
MINNOW_DISABLE_THP=1 ./minnow-exec ./minnow
```

The launcher itself is opt-in when the variable is unset. Running `minnow`
directly bypasses it; macOS execution is unchanged.

On the measured THP-`always` CI host, disabling THP reduced simulation peak RSS
by 27.9%, with 0.5% more total CPU and query latency changes from −0.9% to +1.8%.
These are workload-specific results, not a promise of identical savings on every
host. See [the experiment procedure](../BENCHMARK.md#transparent-huge-page-attribution).

### Application configuration

The image includes [`docker/minnow.yaml`](docker/minnow.yaml). To customize it,
mount your own file read-only:

```yaml
services:
  minnow:
    volumes:
      - ./minnow.yaml:/etc/minnow/minnow.yaml:ro
      - minnow-data:/var/lib/minnow
```

Keep secrets in environment variables referenced as `${VAR}` in YAML. The
provided configuration requires `OPENAI_API_KEY`. You may replace the embedder
with Ollama or Minnow's deterministic local embedder.

## Security

Minnow v0.2.2 does not validate bearer tokens. The root Compose file therefore
publishes port 8080 only on `127.0.0.1`. For remote clients, use Tailscale or put
an HTTPS reverse proxy/API gateway that validates credentials in front. The AWS
and Fly examples include a Caddy bearer-token boundary. It is one shared static
token—not user identity, fine-grained authorization, or native Minnow auth.
