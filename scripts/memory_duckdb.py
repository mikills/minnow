"""CI-only DuckDB memory attribution; no perf, heaptrack, or throughput tests."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import statistics

from compare_duckdb import CONTROLS, require_ci
from profile_duckdb import run, validate_probe


MODES = ("core", "vss", "fts", "configured", "ingest")
LABELS = ("baseline", "candidate")
VERSIONS = {"baseline": "v1.5.0", "candidate": "v2.0.0"}
JE_KEYS = {"allocated", "active", "metadata", "resident", "mapped", "retained"}
POINTS = (("initial", 0), ("open", 0), ("load_vss", 0), ("load_fts", 0), ("append", -1),
          ("hnsw", 0), ("fts_index", 0), ("checkpoint", 0),
          ("close", 0), ("close", -1), ("after_gc", 0),
          ("after_jemalloc_purge", 0), ("after_trim", 0))


def schedule():
    for index in range(3):
        for position, mode in enumerate(MODES):
            labels = LABELS if (index + position) % 2 == 0 else tuple(reversed(LABELS))
            for label in labels:
                yield index, mode, label


def validate(data, mode, version):
    if data.get("memory_attribution") is not True:
        raise RuntimeError("memory attribution was not enabled")
    # Reuse the lifecycle validation without hiding the extra intervention.
    stages = Counter(s["stage"] for s in data["samples"])
    if stages["after_jemalloc_purge"] != 1:
        raise RuntimeError("missing or repeated jemalloc purge")
    ordinary = dict(data, samples=[s for s in data["samples"] if s["stage"] != "after_jemalloc_purge"])
    validate_probe(ordinary, mode, 20, version)
    if [s["stage"] for s in data["samples"]][-3:] != ["after_gc", "after_jemalloc_purge", "after_trim"]:
        raise RuntimeError("purge must follow GC and precede glibc trim")
    if data.get("diagnostics"):
        raise RuntimeError("; ".join(data["diagnostics"]))
    settings = data.get("settings", {})
    if settings.get("threads") != "1" or settings.get("memory_limit") != "122.0 MiB":
        raise RuntimeError(f"unexpected startup limits: {settings}")
    for sample in data["samples"]:
        je = sample.get("jemalloc", {})
        if je.get("errors") or not je.get("version") or set(je.get("bytes", {})) != JE_KEYS:
            raise RuntimeError(f"{sample['stage']}: unavailable jemalloc counters: {je}")
        if not sample.get("mappings"):
            raise RuntimeError("missing resident mapping counters")
        for value in je["bytes"].values():
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RuntimeError("invalid jemalloc byte counter")
        if "Rss" not in sample["os_kib"] or "Anonymous" not in sample["os_kib"]:
            raise RuntimeError("missing RSS/anonymous counters")
        open_stage = sample["stage"] not in {"initial", "close", "after_gc", "after_jemalloc_purge", "after_trim"}
        if open_stage and not sample.get("duckdb_memory"):
            raise RuntimeError(f"{sample['stage']}: missing DuckDB memory counters")
        if not open_stage and "duckdb_memory" in sample:
            raise RuntimeError("DuckDB counters must be absent when no database is open")
        tags = []
        for row in sample.get("duckdb_memory", []):
            tags.append(row["tag"])
            if row["memory_usage_bytes"] < 0 or row["temporary_storage_bytes"] < 0:
                raise RuntimeError("negative DuckDB memory counter")
        if len(tags) != len(set(tags)):
            raise RuntimeError("duplicate DuckDB memory tag")


def select_sample(data, stage, offset):
    samples = [s for s in data["samples"] if s["stage"] == stage]
    return samples[offset] if samples else None


def mib(value):
    return f"{value / 1048576:.2f}"


def report(records, errors, finished):
    lines = ["## DuckDB memory attribution", "",
             "Same hosted runner and probe source; sequential processes, three alternating rounds.",
             "All table values are median MiB. Each mode is a fresh process; rows are not additive.",
             "20 open/load/close cycles; ingest instead uses one database with 20 × 512 rows and both indexes.",
             "Signatures enabled; memory_limit=128MB and threads=1 are set at startup.",
             "SQL/mapping/allocator inspection perturbs these diagnostics. Do not compare their timings to benchmarks.", "",
             "jemalloc allocated is allocator-reported, with thread-cache/statistics limitations; resident is an estimate, not OS RSS.",
             "jemalloc retained is VIRTUAL address space, not retained physical RAM. Metrics overlap; do not add them.",
             "Counters cover the main binary's prefixed jemalloc, not necessarily allocators private to extensions.",
             "DuckDB counters cover its buffer manager, not all process memory (including HNSW).",
             "After close, DuckDB counters are unavailable (—), not zero. Purge is diagnostic only, after every DB is closed.",
             "Arena purge does not flush other threads' tcaches. Residual allocated/RSS is not proof of a leak.", ""]
    if not finished or errors or len(records) != 30:
        lines += ["**Incomplete or unavailable diagnostics — do not treat partial rows as a complete comparison.**", ""]
    for mode in MODES:
        lines += [f"### {mode}", "",
                  "| point | engine | RSS | anonymous | extension RSS | Go heap | DuckDB tracked | jemalloc allocated | active | metadata | resident estimate | retained virtual |",
                  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for stage, offset in POINTS:
            # Ingest has one close; don't print it twice as a supposed 20th close.
            if stage == "close" and offset == -1 and mode == "ingest":
                continue
            for label in LABELS:
                rows = [r for r in records if r["label"] == label and r["data"]["mode"] == mode]
                if len(rows) != 3 or {r["round"] for r in rows} != {0, 1, 2}:
                    continue
                samples = [select_sample(r["data"], stage, offset) for r in rows]
                if any(s is None for s in samples):
                    continue
                def median(get):
                    values = [get(s) for s in samples]
                    return "—" if any(v is None for v in values) else mib(statistics.median(values))
                values = [median(lambda s: s["os_kib"]["Rss"] * 1024),
                          median(lambda s: s["os_kib"]["Anonymous"] * 1024),
                          median(lambda s: sum(v["rss_kib"] * 1024 for k, v in s["mappings"].items()
                                               if ".duckdb_extension" in k)),
                          median(lambda s: s["go_heap_bytes"]),
                          median(lambda s: sum(r["memory_usage_bytes"] for r in s["duckdb_memory"])
                                 if "duckdb_memory" in s else None)]
                for key in ("allocated", "active", "metadata", "resident", "retained"):
                    values.append(median(lambda s: s.get("jemalloc", {}).get("bytes", {}).get(key)))
                point = stage if stage != "close" else ("first close" if offset == 0 else "20th close")
                lines.append(f"| {point} | {label} | " + " | ".join(values) + " |")
    lines += ["", "### Paired purge effect", "",
              "Median within-process RSS reduction from after_gc to after_jemalloc_purge (positive means released pages).", "",
              "| mode | 1.5 released MiB | 2.0 released MiB |", "| --- | ---: | ---: |"]
    for mode in MODES:
        values = []
        for label in LABELS:
            rows = [r for r in records if r["label"] == label and r["data"]["mode"] == mode]
            if len(rows) != 3 or {r["round"] for r in rows} != {0, 1, 2}:
                break
            drops = [(select_sample(r["data"], "after_gc", 0)["os_kib"]["Rss"] -
                      select_sample(r["data"], "after_jemalloc_purge", 0)["os_kib"]["Rss"]) * 1024 for r in rows]
            values.append(mib(statistics.median(drops)))
        if len(values) == 2:
            lines.append(f"| {mode} | " + " | ".join(values) + " |")
    lines += ["", "Raw JSON retains per-tag DuckDB counters, per-file resident mappings, glibc counters,",
              "Go reserved/released memory, all jemalloc byte counters, limits and every lifecycle snapshot.",
              "No production settings are changed. This is a bounded attribution experiment, not a leak test."]
    if errors:
        lines += ["", "### Failed or unavailable diagnostics"] + [f"- {e}" for e in errors]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    require_ci(os.environ)
    root = args.root.resolve()
    results = root / "memory"
    results.mkdir(exist_ok=True)
    engines = {label: json.loads((results / f"{label}.engine.json").read_text()) for label in LABELS}
    if {label: data["version"] for label, data in engines.items()} != VERSIONS:
        raise RuntimeError(f"expected 1.5.0 versus stable 2.0.0, got {engines}")
    env = dict(os.environ, **CONTROLS)
    # Do not silently inherit allocator tuning or injection from the runner.
    for key in ("MALLOC_CONF", "DUCKDB_JE_MALLOC_CONF", "LD_PRELOAD"):
        if env.get(key):
            raise RuntimeError(f"unexpected allocator override: {key}")
    records, errors, raw = [], [], []
    finished = False

    def save():
        (results / "measurements.json").write_text(json.dumps({
            "engines": engines, "controls": CONTROLS, "runs": raw,
            "errors": errors, "finished": finished,
        }, indent=2) + "\n")
        text = report(records, errors, finished)
        (results / "summary.md").write_text(text)
        return text

    try:
        for index, mode, label in schedule():
            name = f"{label}-{mode}-{index}"
            prefix = results / name
            home = results / f"{name}.home"
            home.mkdir()
            try:
                print(name, flush=True)
                command = [str(results / f"{label}-profile"), "-mode", mode, "-iterations", "20",
                           "-extensions", str(root / label / "extensions"), "-expect", VERSIONS[label],
                           "-memory-attribution"]
                run(command, prefix, root / label, dict(env, HOME=str(home)), timeout=120)
                data = json.loads(Path(str(prefix) + ".out").read_text())
                record = {"label": label, "round": index, "data": data}
                raw.append(record)  # Preserve missing-counter evidence even if validation fails.
                validate(data, mode, VERSIONS[label])
                if data["source"] != engines[label]["source"]:
                    raise RuntimeError("probe source differs from compiled engine")
                records.append(record)
            except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
                errors.append(f"{name}: {error}")
            finally:
                save()
        finished = True
    finally:
        text = save()
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
        print(text)
    if errors or len(records) != 30:
        raise SystemExit("Memory attribution incomplete; inspect the saved diagnostics")


if __name__ == "__main__":
    main()
