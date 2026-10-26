# DuckDB 2.0.0 offline extensions

These signed binaries are downloaded from DuckDB's official extension repository:

```text
https://extensions.duckdb.org/v2.0.0/<platform>/<name>.duckdb_extension.gz
```

Platforms: `osx_arm64`, `linux_amd64`. Names: `fts`, `vss`, `httpfs`.
The checked-in files are decompressed; `SHA256SUMS` covers those bytes.
Verify with `sha256sum -c SHA256SUMS` (Linux) or `shasum -a 256 -c SHA256SUMS`
(macOS) from this directory. DuckDB also verifies upstream signatures at load
time; `allow_unsigned_extensions` must remain disabled.
