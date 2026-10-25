"""CI-only native CPU/allocation profiling, separate from performance comparisons."""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess

from compare_duckdb import CONTROLS, require_ci


MODES = {"core": 20, "vss": 20, "fts": 20, "configured": 20, "ingest": 20, "query": 1000}


def has_native_symbols(text):
    # A hash-dominated trace can have no duckdb:: entry above the report's
    # percentage threshold. mbedTLS is native engine code too, unlike Go's
    # crypto/internal/...sha256 symbols. Do not mislabel that as missing data.
    return bool(re.search(
        r"\[\.\]\s+(?:duckdb::|duckdb_mbedtls::|mbedtls_internal_sha256_process\b|mbedtls_sha256_|unum::usearch::)",
        text,
    ))


def run(command, prefix, cwd, env, timeout=120):
    """Keep stdout (JSON or report) separate from profiler diagnostics."""
    with Path(str(prefix) + ".out").open("w") as output, Path(str(prefix) + ".err").open("w") as errors:
        with subprocess.Popen(command, cwd=cwd, env=env, stdout=output, stderr=errors,
                              start_new_session=True) as process:
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise RuntimeError(f"{prefix.name}: timed out") from None
    if code:
        raise RuntimeError(f"{prefix.name}: exit {code}; see .err/.out")


def validate_probe(data, mode, iterations, version):
    if (data["version"], data["mode"], data["iterations"]) != (version, mode, iterations):
        raise RuntimeError("probe engine/workload mismatch")
    stages = defaultdict(int)
    for sample in data["samples"]:
        stages[sample["stage"]] += 1
    opens = 1 if mode in ("ingest", "query") else iterations
    expected = {"initial": 1, "open": opens, "close": opens, "after_gc": 1, "after_trim": 1}
    if mode in ("vss", "configured", "ingest", "query"):
        expected["load_vss"] = opens
    if mode in ("fts", "configured", "ingest", "query"):
        expected["load_fts"] = opens
    if mode in ("ingest", "query"):
        expected.update(append=8 if mode == "query" else iterations, hnsw=1, fts_index=1, checkpoint=1)
    if mode == "query":
        expected.update(vector_query=iterations, bm25_query=iterations)
    if dict(stages) != expected:
        raise RuntimeError(f"incomplete {mode} stages: {dict(stages)}, expected {expected}")


def summary(records, errors):
    grouped = defaultdict(list)
    memory = {}
    for record in records:
        data = record["data"]
        stages = defaultdict(list)
        for sample in data["samples"]:
            stages[sample["stage"]].append(sample)
        for stage, samples in stages.items():
            grouped[(record["label"], data["mode"], stage)].append({
                key: sum(s[key] for s in samples) / len(samples) / 1e6
                for key in ("elapsed_ns", "user_ns", "system_ns")
            })
        if data["mode"] == "configured" and record["round"] == 2:
            memory[record["label"]] = [stages["close"][0], stages["close"][-1],
                                       stages["after_gc"][0], stages["after_trim"][0]]
    lines = ["## DuckDB native profiling", "",
             "Same CI runner and probe source; three alternating phase runs **without perf/heaptrack**.",
             "Times are median per-operation milliseconds; setup is not charged to append/query stages.",
             "Memory snapshots are collected outside operation timers and outside the hot query loop.",
             "The open stage includes version/security-policy SQL checks. Signature verification stays enabled.",
             "Native perf/heaptrack traces are separate, instrumented runs; do not compare their wall times.", "",
             "| mode / stage | 1.5 elapsed ms | 2.0 elapsed ms | change | 1.5 user CPU ms | 2.0 user CPU ms |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for mode, stage in (("core", "open"), ("core", "close"), ("configured", "open"),
                        ("vss", "load_vss"), ("fts", "load_fts"),
                        ("configured", "load_vss"), ("configured", "load_fts"),
                        ("configured", "close"), ("ingest", "append"), ("ingest", "hnsw"),
                        ("ingest", "fts_index"), ("ingest", "checkpoint"),
                        ("query", "vector_query"), ("query", "bm25_query")):
        values = []
        for label in ("baseline", "candidate"):
            rows = grouped[(label, mode, stage)]
            if len(rows) != 3:
                break
            values.append({key: statistics.median(r[key] for r in rows) for key in rows[0]})
        if len(values) != 2:
            continue
        a, b = values
        delta = f"{100 * (b['elapsed_ns'] / a['elapsed_ns'] - 1):+.1f}%" if a["elapsed_ns"] else "n/a"
        lines.append(f"| {mode} / {stage} | {a['elapsed_ns']:.3f} | {b['elapsed_ns']:.3f} | {delta} | "
                     f"{a['user_ns']:.3f} | {b['user_ns']:.3f} |")
    lines += ["", "### Memory after configured open/load/close cycles (third round)", "",
              "All values MiB. RSS includes mapped code and retained pages, not just live allocations.",
              "glibc counters exclude Go's mmap heap and any non-glibc native allocator.", "",
              "| engine | point | RSS | anonymous | Go heap | glibc arena system | glibc arena free | glibc mmap |",
              "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for label, samples in memory.items():
        for point, sample in zip(("first close", "20th close", "Go GC", "diagnostic malloc_trim"), samples):
            allocator = sample["glibc_bytes"]
            values = [sample["os_kib"]["Rss"] / 1024, sample["os_kib"]["Anonymous"] / 1024,
                      sample["go_heap_bytes"] / 1048576, allocator["system_current"] / 1048576,
                      (allocator["fast"] + allocator["rest"]) / 1048576, allocator["mmap"] / 1048576]
            lines.append(f"| {label} | {point} | " + " | ".join(f"{value:.2f}" for value in values) + " |")
    lines += ["", "See `.leaf.txt` for native CPU self-time attribution and `.allocations.txt` for heaptrack stacks.",
              "Heaptrack's outstanding-at-exit totals are not proof of a leak; interception/symbolization can be incomplete.",
              "The malloc_trim experiment is diagnostic only and does not change production settings."]
    if errors:
        lines += ["", "### Failed or unavailable diagnostics"] + [f"- {error}" for error in errors]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    require_ci(os.environ)
    root = args.root.resolve()
    results = root / "profiles"
    results.mkdir(exist_ok=True)
    engines = {label: json.loads((results / f"{label}.engine.json").read_text())["version"]
               for label in ("baseline", "candidate")}
    if engines != {"baseline": "v1.5.0", "candidate": "v2.0.0"}:
        raise RuntimeError(f"expected 1.5.0 versus stable 2.0.0, got {engines}")
    env = dict(os.environ, **CONTROLS)
    records, errors, completed = [], [], []

    def save(finished=False):
        (results / "phases.json").write_text(json.dumps({
            "engines": engines, "runs": records, "errors": errors,
            "completed": completed, "finished": finished,
        }, indent=2))
        text = summary(records, errors)
        if not finished:
            text = "**Partial diagnostics: profiling has not finished.**\n\n" + text
        (results / "summary.md").write_text(text)
        return text

    def attempt(description, operation):
        print(description, flush=True)
        try:
            operation()
            completed.append(description)
        except (RuntimeError, OSError, ValueError) as error:
            errors.append(str(error))
        finally:
            save()

    def target(label, mode, iterations):
        return [str(results / f"{label}-profile"), "-mode", mode, "-iterations", str(iterations),
                "-extensions", str(root / label / "extensions"), "-expect", engines[label]]

    def phase(label, mode, iterations, index):
        prefix = results / f"phase-{label}-{mode}-{index}"
        run(["/usr/bin/time", "-v", "-o", str(prefix) + ".time"] + target(label, mode, iterations),
            prefix, root / label, env)
        data = json.loads(Path(str(prefix) + ".out").read_text())
        validate_probe(data, mode, iterations, engines[label])
        records.append({"label": label, "round": index, "data": data})

    def cpu(label, name, command, is_sim=False):
        prefix = results / f"cpu-{label}-{name}"
        perf = os.environ["PERF"]
        # DWARF capture/processing exceeded the budget on the 2.0 Go/C++ stacks
        # in the first run. Leaf samples retain native symbol attribution without that
        # unwind cost. Skip build-id post-processing too; matching ELF files
        # and the DSOs remain available for reports and in the artifact.
        run([perf, "record", "-e", "cpu-clock:u", "-F", "199", "--no-buildid", "--no-buildid-cache",
             "-o", str(prefix) + ".data", "--"] + command, prefix, root / label / "sim", env)
        if is_sim:
            output = Path(str(prefix) + ".out").read_text()
            if f"--- PASS: TestSim/{name}/seed_1 (" not in output or "--- SKIP:" in output:
                raise RuntimeError(f"{name}: simulation seed did not pass")
        report_prefix = results / f"{prefix.name}.leaf"
        run([perf, "report", "--stdio", "--no-children", "--call-graph", "none", "--sort", "dso,symbol",
             "--percent-limit", "0.5", "-i", str(prefix) + ".data"], report_prefix, root, env)
        Path(str(report_prefix) + ".out").rename(results / f"{prefix.name}.leaf.txt")
        text = (results / f"{prefix.name}.leaf.txt").read_text()
        if not has_native_symbols(text):
            raise RuntimeError(f"{prefix.name}: native DuckDB symbols unavailable in perf report")

    def heap(label, mode):
        prefix = results / f"heap-{label}-{mode}"
        run(["heaptrack", "--record-only", "-o", str(prefix)] + target(label, mode, 20),
            prefix, root / label, env)
        traces = [p for p in (Path(str(prefix) + ".gz"), Path(str(prefix) + ".zst")) if p.exists()]
        if len(traces) != 1:
            raise RuntimeError(f"{prefix.name}: heaptrack trace missing")
        report_prefix = results / f"{prefix.name}.allocations"
        run(["heaptrack_print", "-f", str(traces[0]), "-n", "20", "-s", "3", "--merge-backtraces=false"],
            report_prefix, root, env)
        Path(str(report_prefix) + ".out").rename(results / f"{prefix.name}.allocations.txt")

    finished = False
    try:
        for index in range(3):
            for position, (mode, iterations) in enumerate(MODES.items()):
                labels = ("baseline", "candidate") if (index + position) % 2 == 0 else ("candidate", "baseline")
                for label in labels:
                    attempt(f"phase {label} {mode} round={index}", lambda: phase(label, mode, iterations, index))
        for label in ("baseline", "candidate"):
            # Capture allocation evidence before CPU processing, so a sampling
            # tool problem cannot prevent both engines' allocation traces.
            for mode in ("configured", "ingest"):
                attempt(f"heap {label} {mode}", lambda: heap(label, mode))
            for mode in ("configured", "query"):
                attempt(f"cpu {label} {mode}", lambda: cpu(label, mode, target(label, mode, MODES[mode])))
            for scenario in ("repeated_small_ingests", "random_ops_fuzz"):
                command = [str(results / f"{label}.test"), "-test.v", "-test.count=1", "-test.parallel=1",
                           "-test.timeout=5m", f"-test.run=^TestSim$/^{scenario}$/^seed_1$"]
                attempt(f"cpu {label} {scenario}", lambda: cpu(label, scenario, command, is_sim=True))
        finished = True
    finally:
        text = save(finished=finished)
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
        print(text)
    if errors:
        raise SystemExit("Some diagnostics failed; see the summary and saved logs")


if __name__ == "__main__":
    main()
