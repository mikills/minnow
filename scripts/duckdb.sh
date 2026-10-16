#!/usr/bin/env bash
# Run Go against the actual 2.0 engine, not the stale archive in the Go preview.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ $# == 0 ]]; then
    echo "Usage: bash scripts/duckdb.sh <go command and arguments>" >&2
    exit 1
fi
version=v2.0.0-alpha45672
commit=f2f9329721
host_os=$(go env GOHOSTOS)
host_arch=$(go env GOHOSTARCH)
if [[ "$(go env GOOS)/$(go env GOARCH)" != "$host_os/$host_arch" ]]; then
    echo "DuckDB setup requires a native build; cross-compilation is not configured." >&2
    exit 1
fi
case "$host_os/$host_arch" in
    darwin/arm64)
        platform=osx_arm64
        dist=osx-arm64
        checksum=32a229b171c3d371b7d216b8d41c150ca81c18711eaa3c0002c613250d844f76
        system_libs='-lc++ -Wl,-export_dynamic'
        ;;
    linux/amd64)
        platform=linux_amd64
        dist=linux-amd64
        checksum=aa79795e00b78b56b6674643694201bca67faebefd149456aab83b79b0a08932
        system_libs='-lstdc++ -lm -ldl -lpthread -Wl,--export-dynamic'
        ;;
    *)
        echo "No bundled DuckDB 2.0 extensions for $host_os/$host_arch (supported: darwin/arm64, linux/amd64)." >&2
        exit 1
        ;;
esac

libdir="$root/.cache/duckdb/$version/$platform"
if [[ ! -s "$libdir/libduckdb_static.a" || ! -f "$libdir/.ready" ]]; then
    mkdir -p "$(dirname "$libdir")"
    stage=$(mktemp -d "$(dirname "$libdir")/.download.XXXXXX")
    trap 'rm -rf "$stage"' EXIT
    url="https://duckdb-staging.duckdb.org/$commit/$version/duckdb/duckdb/github_release/duckdb-static-libs-$dist.tar.gz"
    echo "Fetching pinned DuckDB $version for $platform" >&2
    curl -fsSL --retry 3 "$url" -o "$stage/archive.tar.gz"
    if command -v sha256sum >/dev/null 2>&1; then
        actual=$(sha256sum "$stage/archive.tar.gz" | cut -d ' ' -f 1)
    else
        actual=$(shasum -a 256 "$stage/archive.tar.gz" | cut -d ' ' -f 1)
    fi
    if [[ "$actual" != "$checksum" ]]; then
        echo "DuckDB archive checksum mismatch: $actual (expected $checksum)" >&2
        exit 1
    fi
    mkdir "$stage/lib"
    tar -xzf "$stage/archive.tar.gz" -C "$stage/lib"
    test -s "$stage/lib/duckdb_static_extension.h"
    test -s "$stage/lib/libduckdb_static.a"
    printf '%s\n' "$checksum" > "$stage/lib/.ready"
    # Install only after successful download, verification, and extraction.
    mkdir -p "$libdir"
    cp "$stage/lib/"*.a "$stage/lib/"*.h "$libdir/"
    cp "$stage/lib/.ready" "$libdir/.ready"
    rm -rf "$stage"
    trap - EXIT
fi

libraries='-lduckdb_loadable_extensions -lduckdb_httplib -lcore_functions_extension -lautocomplete_extension -licu_extension -ljson_extension -lparquet_extension -lduckdb_static'
if [[ "$host_os" == linux ]]; then
    libraries="-Wl,--start-group $libraries -Wl,--end-group"
fi
export CGO_ENABLED=1
export CGO_LDFLAGS_ALLOW='-Wl,(-export_dynamic|--export-dynamic|--start-group|--end-group)'
export CGO_CPPFLAGS="\"-I$libdir\" ${CGO_CPPFLAGS:-}"
export CGO_LDFLAGS="\"-L$libdir\" $libraries $system_libs ${CGO_LDFLAGS:-}"
export GOFLAGS="${GOFLAGS:-} -tags=duckdb_use_static_lib"

# Explicit -tags overrides GOFLAGS, so preserve the native-link tag as well.
args=()
tag_value=false
for arg in "$@"; do
    if $tag_value; then
        args+=("duckdb_use_static_lib,$arg")
        tag_value=false
    elif [[ "$arg" == -tags ]]; then
        args+=("$arg")
        tag_value=true
    elif [[ "$arg" == -tags=* ]]; then
        args+=("-tags=duckdb_use_static_lib,${arg#-tags=}")
    else
        args+=("$arg")
    fi
done
exec go "${args[@]}"
