"""CI-only PEG versus legacy parser comparison within one stable DuckDB 2.0 binary."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import statistics

from compare_duckdb import CONTROLS, require_ci
from profile_duckdb import SPLIT_CASES, SPLIT_STAGES, has_hnsw, run, validate_probe


PARSERS = ("peg", "legacy")
MODES = {"core": 20, "configured": 20, "ingest": 20, "query_split": 300}
VERSION = "v2.0.0"
SETTINGS = {"allow_unsigned_extensions": "false", "autoload_known_extensions": "false",
            "autoinstall_known_extensions": "false", "threads": "1", "memory_limit": "122.0 MiB",
            "legacy_parser_loaded": "true", "allow_parser_override_extension": "strict"}
PARSER_STAGES = {"load_legacy_parser", "select_parser"}


def schedule():
    for index in range(3):
        for position, mode in enumerate(MODES):
            parsers = PARSERS if (index + position) % 2 == 0 else tuple(reversed(PARSERS))
            for parser in parsers:
                yield index, mode, parser


def validate(data, mode, parser, engine):
    opens = 1 if mode in ("ingest", "query_split") else MODES[mode]
    settings = dict(SETTINGS, enable_legacy_parser=str(parser == "legacy").lower())
    if (engine["version"] != VERSION or not engine.get("source") or data.get("source") != engine["source"]
            or data.get("parser") != parser or data.get("parser_checks") != 2 * opens
            or data.get("parser_settings") != settings):
        raise RuntimeError("parser/engine/limits/security verification mismatch")
    if data.get("error") or data.get("diagnostics") or data.get("memory_attribution"):
        raise RuntimeError("failed or instrumented parser probe")
    stages = Counter(s["stage"] for s in data["samples"])
    if any(stages[stage] != opens for stage in PARSER_STAGES):
        raise RuntimeError("missing or repeated parser selection/load")
    ordinary = dict(data, samples=[s for s in data["samples"] if s["stage"] not in PARSER_STAGES])
    validate_probe(ordinary, mode, MODES[mode], VERSION)
    if mode == "query_split":
        for name in ("vector", "bound_vector", "bound_vector_axis1"):
            if not has_hnsw(data["query_plans"][name]):
                raise RuntimeError(f"{name}: lost HNSW")
    for sample in data["samples"]:
        for key in ("elapsed_ns", "user_ns", "system_ns"):
            if type(sample[key]) is not int or sample[key] < 0:
                raise RuntimeError(f"invalid timing: {key}")
        if sample["stage"] in ("initial", "close", "after_gc", "after_trim"):
            for key in ("Rss", "Anonymous"):
                if type(sample["os_kib"][key]) is not int or sample["os_kib"][key] < 0:
                    raise RuntimeError(f"invalid memory counter: {key}")
            if type(sample["go_heap_bytes"]) is not int or sample["go_heap_bytes"] < 0:
                raise RuntimeError("invalid Go heap counter")


def pairs(records, mode):
    rows = [r for r in records if r["data"]["mode"] == mode]
    keyed = {(r["round"], r["parser"]): r["data"] for r in rows}
    expected = {(index, parser) for index in range(3) for parser in PARSERS}
    if len(rows) != 6 or set(keyed) != expected:
        return []
    return [(keyed[index, "peg"], keyed[index, "legacy"]) for index in range(3)]


def stage_mean(data, stage, key):
    return statistics.mean(s[key] / 1e6 for s in data["samples"] if s["stage"] == stage)


def paired_values(paired, get):
    values = [(get(a), get(b)) for a, b in paired]
    a, b = (statistics.median(v[i] for v in values) for i in (0, 1))
    change = (f"{statistics.median(100 * (b / a - 1) for a, b in values):+.1f}%"
              if all(a > 0 for a, _ in values) else "n/a")
    return a, b, change


def report(records, errors, finished):
    lines = ["## DuckDB 2.0 parser comparison", "",
             "One compiled binary and signed extension set; fresh sequential processes, three alternating paired rounds.",
             "Both arms load legacy_parser; only enable_legacy_parser changes. Startup limits: 128MB, one DuckDB thread.",
             "Both arms use strict parser override policy: legacy syntax errors cannot silently fall back to PEG.",
             "Table times are median per-operation ms; changes are medians of within-round legacy/PEG differences.",
             "Prepare includes parsing/binding/planning performed there; execution includes binding and deferred planning.",
             "Query warmups and memory inspection are outside query timers. Both bound vector directions retain HNSW.", ""]
    if not finished or errors or len(records) != 24:
        lines += ["**Incomplete diagnostics: only complete three-round pairs are tabulated.**", ""]
    lines += ["| mode / stage | PEG ms | legacy ms | paired change | PEG user CPU ms | legacy user CPU ms | paired CPU change |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for mode in MODES:
        paired = pairs(records, mode)
        if not paired:
            continue
        stages = ([f"{stage}_{case}" for case in SPLIT_CASES for stage in SPLIT_STAGES]
                  if mode == "query_split" else
                  ["open", "load_legacy_parser", "select_parser"] +
                  (["load_vss", "load_fts"] if mode != "core" else []) +
                  (["append", "hnsw", "fts_index", "checkpoint"] if mode == "ingest" else []) + ["close"])
        for stage in stages:
            a, b, delta = paired_values(paired, lambda d: stage_mean(d, stage, "elapsed_ns"))
            u, v, cpu_delta = paired_values(paired, lambda d: stage_mean(d, stage, "user_ns"))
            lines.append(f"| {mode} / {stage} | {a:.3f} | {b:.3f} | {delta} | {u:.3f} | {v:.3f} | {cpu_delta} |")
    lines += ["", "### Lifecycle memory observations", "",
              "Median MiB after close and Go GC, before diagnostic malloc_trim. RSS is not live heap or proof of a leak.",
              "The initial open still uses PEG, and both arms map legacy_parser; this cannot isolate all grammar residency.",
              "No allocator purge, heaptrack, perf, or huge-page policy changes are part of this comparison.", "",
              "| mode | counter | PEG MiB | legacy MiB | paired change |",
              "| --- | --- | ---: | ---: | ---: |"]
    for mode in MODES:
        paired = pairs(records, mode)
        if not paired:
            continue
        for key in ("Rss", "Anonymous", "go_heap_bytes"):
            def memory(data):
                sample = next(s for s in data["samples"] if s["stage"] == "after_gc")
                return sample[key] / 1048576 if key == "go_heap_bytes" else sample["os_kib"][key] / 1024
            a, b, delta = paired_values(paired, memory)
            lines.append(f"| {mode} | {key} | {a:.2f} | {b:.2f} | {delta} |")
    lines += ["", "This fixture is not the full production query benchmark. A production parser change still requires",
              "compatibility, end-to-end query and simulation validation. Raw samples retain system CPU and all lifecycle snapshots."]
    if errors:
        lines += ["", "### Failed or unavailable diagnostics"] + [f"- {e}" for e in errors]
    return "\n".join(lines) + "\n"


def collect(root, engine, env):
    require_ci(env)
    results = root / "parser"
    records, raw, errors = [], [], []
    finished = False

    def save():
        (results / "measurements.json").write_text(json.dumps({
            "engine": engine, "controls": CONTROLS, "runs": raw,
            "errors": errors, "finished": finished,
        }, indent=2) + "\n")
        text = report(records, errors, finished)
        (results / "summary.md").write_text(text)
        return text

    save()
    try:
        for index, mode, parser in schedule():
            name = f"{parser}-{mode}-{index}"
            prefix = results / name
            try:
                print(name, flush=True)
                home = results / f"{name}.home"
                home.mkdir()
                command = [str(results / "candidate-profile"), "-mode", mode, "-iterations", str(MODES[mode]),
                           "-extensions", str(results / "extensions"), "-expect", VERSION, "-parser", parser]
                run(command, prefix, root / "candidate", dict(env, **CONTROLS, HOME=str(home)), timeout=180)
                data = json.loads(Path(str(prefix) + ".out").read_text())
                record = {"parser": parser, "round": index, "data": data}
                raw.append(record)  # Keep invalid evidence, but exclude it from the report.
                validate(data, mode, parser, engine)
                records.append(record)
            except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
                errors.append(f"{name}: {error}")
            finally:
                save()
        finished = True
    finally:
        text = save()
        with open(env["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    if errors or len(records) != 24:
        raise RuntimeError("Parser comparison incomplete; inspect saved .out/.err and summary")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    require_ci(os.environ)
    for key in ("MALLOC_CONF", "DUCKDB_JE_MALLOC_CONF", "LD_PRELOAD"):
        if os.environ.get(key):
            raise RuntimeError(f"unexpected allocator override: {key}")
    root = args.root.resolve()
    engine = json.loads((root / "parser/candidate.engine.json").read_text())
    if engine["version"] != VERSION or not engine.get("source"):
        raise RuntimeError(f"expected stable 2.0 engine metadata, got {engine}")
    collect(root, engine, dict(os.environ))


if __name__ == "__main__":
    main()
