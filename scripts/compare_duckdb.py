"""Measure compiled simulation binaries on one GitHub-hosted Linux runner only."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess


SEEDS = (1, 2, 3, 7, 42, 100)
SCENARIOS = (
    "warm_vs_cold", "flaky_blob_ingest", "concurrent_writers",
    "concurrent_many_writers", "async_document_pipeline", "cache_evicted_mid_query",
    "high_download_failure", "clock_jumps_backward", "dim_mismatch_query",
    "repeated_small_ingests", "concurrent_ingest_query", "random_ops_fuzz",
    "scope_upload_fault", "scope_read_fault", "scope_isolation",
)
WORKLOADS = tuple(("TestSim", name) for name in SCENARIOS) + (("TestTieredStorageSim", "recovery"),)
QUERY_CASES = ("full_32", "full_512", "refs_512", "filtered_512", "scoped_512", "bm25_512", "churn_512")
CONTROLS = {"GOMAXPROCS": "2", "GOMEMLIMIT": "384MiB", "GOWORK": "off"}


def require_ci(env):
    if env.get("GITHUB_ACTIONS") != "true" or env.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("performance measurement is restricted to GitHub-hosted Actions runners")


def validate_tests(output, parent, scenario):
    passed = set(re.findall(r"^\s*--- PASS: (\S+) \(", output, re.MULTILINE))
    expected = {f"{parent}/{scenario}/seed_{seed}" for seed in SEEDS}
    leaves = {name for name in passed if "/seed_" in name}
    if leaves != expected or "--- SKIP:" in output or "--- FAIL:" in output:
        raise RuntimeError(f"unmatched or incomplete workload {parent}/{scenario}: {sorted(leaves)}")
    return len(leaves)


def measure(root, label, round_index, workload):
    parent, scenario = workload
    name = f"{label}-{round_index}-{parent}-{scenario}"
    metrics = root / "results" / f"{name}.time"
    log = root / "results" / f"{name}.log"
    pattern = f"^{parent}$/^{scenario}$/^seed_({'|'.join(map(str, SEEDS))})$"
    env = dict(os.environ, **CONTROLS)
    # Keep user extension caches out of the comparison; the simulation harness
    # resolves the bundled version/platform from each checkout.
    home = root / "results" / f"{name}.home"
    home.mkdir()
    env["HOME"] = str(home)
    command = [
        "/usr/bin/time", "-f", "%e %U %S %M", "-o", str(metrics),
        str(root / "results" / f"{label}.test"),
        "-test.v", "-test.count=1", "-test.parallel=1", "-test.timeout=5m",
        f"-test.run={pattern}",
    ]
    with log.open("w") as output:
        with subprocess.Popen(command, cwd=root / label / "sim", env=env,
                              stdout=output, stderr=subprocess.STDOUT, start_new_session=True) as process:
            try:
                code = process.wait(timeout=330)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise RuntimeError(f"{name} timed out; see {log}") from None
    if code:
        raise RuntimeError(f"{name} failed ({code}); see {log}")
    count = validate_tests(log.read_text(), parent, scenario)
    elapsed, user, system, rss = map(float, metrics.read_text().split())
    return {"label": label, "round": round_index, "scenario": f"{parent}/{scenario}",
            "passed": count, "elapsed_s": elapsed, "user_s": user,
            "system_s": system, "rss_mib": rss / 1024}


def parse_query_result(output, case, parent="BenchmarkWarmShardQuery"):
    pattern = (rf"^{re.escape(parent)}/{re.escape(case)}(?:-\d+)?\s+(\d+)\s+"
               r"([\d.]+) ns/op\s+([\d.]+) B/op\s+([\d.]+) allocs/op\s*$")
    rows = re.findall(pattern, output, re.MULTILINE)
    if len(rows) != 1 or "--- SKIP:" in output or "--- FAIL:" in output:
        raise RuntimeError(f"missing, skipped, or ambiguous warm query result: {case}")
    count, ns, bytes_per_op, allocs = map(float, rows[0])
    if count < 1 or ns <= 0:
        raise RuntimeError(f"invalid warm query result: {case}")
    return {"iterations": int(count), "ns_per_op": ns, "go_bytes_per_op": bytes_per_op,
            "go_allocs_per_op": allocs}


def measure_query(root, label, round_index, case):
    name = f"query-{label}-{round_index}-{case}"
    metrics, log = root / "results" / f"{name}.time", root / "results" / f"{name}.log"
    home = root / "results" / f"{name}.home"
    home.mkdir()
    env = dict(os.environ, **CONTROLS, HOME=str(home))
    command = ["/usr/bin/time", "-f", "%e %U %S %M", "-o", str(metrics),
               str(root / "results" / f"{label}-query.test"), "-test.run=^$", "-test.parallel=1",
               "-test.benchtime=2s", "-test.count=1", "-test.timeout=90s",
               f"-test.bench=^BenchmarkWarmShardQuery$/^{case}$"]
    with log.open("w") as output:
        with subprocess.Popen(command, cwd=root / label / "kb" / "duckdb", env=env,
                              stdout=output, stderr=subprocess.STDOUT, start_new_session=True) as process:
            try:
                code = process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise RuntimeError(f"{name} timed out; see {log}") from None
    if code:
        raise RuntimeError(f"{name} failed ({code}); see {log}")
    result = parse_query_result(log.read_text(), case)
    elapsed, user, system, rss = map(float, metrics.read_text().split())
    return dict(result, label=label, round=round_index, case=case, process_elapsed_s=elapsed,
                process_user_s=user, process_system_s=system, process_peak_rss_mib=rss / 1024)


def query_report(records):
    lines = ["", "### Warm production-shard query paths", "",
             "Same 1,024-row fixture source; changing vectors; three alternating rounds, 2 seconds per case.",
             "Query timings exclude fixture construction and warmup. RSS is whole-process peak, including setup.",
             "Filtered/scoped paths remain literal; refs includes payload hydration; churn cycles ten k values.",
             "Go allocation counts in JSON/logs exclude native DuckDB allocations.", "",
             "| case | baseline ms/op | candidate ms/op | change | baseline peak MiB | candidate peak MiB |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for case in QUERY_CASES:
        values = []
        for label in ("baseline", "candidate"):
            rows = [r for r in records if r["case"] == case and r["label"] == label]
            if len(rows) != 3 or {r["round"] for r in rows} != {0, 1, 2}:
                raise RuntimeError(f"incomplete warm query comparison: {label}/{case}")
            values.append((statistics.median(r["ns_per_op"] for r in rows) / 1e6,
                           statistics.median(r["process_peak_rss_mib"] for r in rows)))
        (a, arss), (b, brss) = values
        lines.append(f"| {case} | {a:.3f} | {b:.3f} | {100 * (b / a - 1):+.1f}% | {arss:.2f} | {brss:.2f} |")
    return "\n".join(lines) + "\n"


def aggregate(records):
    result = {}
    for label in ("baseline", "candidate"):
        rounds = []
        for index in sorted({r["round"] for r in records if r["label"] == label}):
            rows = [r for r in records if r["label"] == label and r["round"] == index]
            if {r["scenario"] for r in rows} != {f"{p}/{s}" for p, s in WORKLOADS} or len(rows) != len(WORKLOADS):
                raise RuntimeError(f"incomplete round {label}/{index}")
            rounds.append({
                "elapsed_s": sum(r["elapsed_s"] for r in rows),
                "user_s": sum(r["user_s"] for r in rows),
                "system_s": sum(r["system_s"] for r in rows),
                "rss_mib": max(r["rss_mib"] for r in rows),
                "passed": sum(r["passed"] for r in rows),
            })
        if not rounds:
            raise RuntimeError(f"no results for {label}")
        result[label] = {key: statistics.median(r[key] for r in rounds) for key in rounds[0]}
    return result


def validate_engines(engines, same_engine=False):
    if same_engine:
        if (engines["baseline"]["version"] != "v2.0.0"
                or not engines["baseline"].get("source")
                or engines["candidate"] != engines["baseline"]):
            raise RuntimeError(f"same-engine comparison requires identical stable 2.0 engine probes, got {engines}")
    elif engines["candidate"]["version"] != "v2.0.0" or engines["baseline"]["version"] == "v2.0.0":
        raise RuntimeError(f"expected pre-2.0 base vs stable 2.0, got {engines}")


def report(summary, engines, same_engine=False):
    title = "DuckDB same-engine revision comparison" if same_engine else "DuckDB engine comparison"
    lines = [f"## {title}", "",
             "Same hosted runner; sequential processes; three alternating rounds; compilation excluded.",
             "GOMAXPROCS=2, GOMEMLIMIT=384MiB, build -p=1, test -parallel=1; signatures enabled.",
             "Elapsed/CPU are medians of round sums; RSS is the median of round peak RSS.",
             "This measures correctness-simulation execution, not query throughput.", "",
             "| revision | engine | seed cases/round | elapsed s | user CPU s | system CPU s | peak RSS MiB |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    keys = ("elapsed_s", "user_s", "system_s", "rss_mib")
    for label in ("baseline", "candidate"):
        row = summary[label]
        values = " | ".join(f"{row[key]:.2f}" for key in keys)
        lines.append(f"| {label} | {engines[label]['version']} | {row['passed']:.0f} | {values} |")
    changes = []
    for key in keys:
        base = summary["baseline"][key]
        changes.append(f"{100 * (summary['candidate'][key] / base - 1):+.1f}%" if base else "n/a")
    lines.append("| change | | | " + " | ".join(changes) + " |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3, choices=[3])
    parser.add_argument("--same-engine", action="store_true",
                        help="compare two application revisions with the identical stable 2.0 engine")
    parser.add_argument("--queries", action="store_true", help="also compare compiled warm production-query fixtures")
    args = parser.parse_args()
    require_ci(os.environ)
    root = args.root.resolve()
    results = root / "results"
    engines = {label: json.loads((results / f"{label}.engine.json").read_text())
               for label in ("baseline", "candidate")}
    validate_engines(engines, args.same_engine)
    records, queries = [], []
    try:
        for index in range(args.rounds):
            # Rotate scenarios and alternate which engine runs first, without
            # ever running competing measurements concurrently.
            workloads = WORKLOADS[index:] + WORKLOADS[:index]
            for position, workload in enumerate(workloads):
                labels = ("baseline", "candidate") if (index + position) % 2 == 0 else ("candidate", "baseline")
                for label in labels:
                    records.append(measure(root, label, index, workload))
        summary = aggregate(records)
        text = report(summary, engines, args.same_engine)
        (results / "summary.md").write_text(text)
        if args.queries:
            for index in range(args.rounds):
                for position, case in enumerate(QUERY_CASES):
                    labels = ("baseline", "candidate") if (index + position) % 2 == 0 else ("candidate", "baseline")
                    for label in labels:
                        queries.append(measure_query(root, label, index, case))
            text += query_report(queries)
        (results / "summary.md").write_text(text)
        print(text)
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    finally:
        (results / "measurements.json").write_text(json.dumps({
            "controls": CONTROLS, "engines": engines, "seeds": SEEDS,
            "rounds": args.rounds, "same_engine": args.same_engine, "measurements": records,
        }, indent=2) + "\n")
        if args.queries:
            (results / "queries.json").write_text(json.dumps({
                "controls": CONTROLS, "engines": engines, "rounds": args.rounds,
                "measurements": queries,
            }, indent=2) + "\n")


if __name__ == "__main__":
    main()
