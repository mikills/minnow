"""Measure compiled simulation binaries on one GitHub-hosted Linux runner only."""

import argparse
import hashlib
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


def thp_target(root, label, suffix):
    # Both arms execute the same physical binary and use the same checkout.
    policy = "default" if label == "baseline" else "disabled"
    return [str(root / "results" / "thp-exec"), policy,
            str(root / "results" / f"candidate{suffix}")]


def validate_thp_launch(output, label):
    states = re.findall(r"^THP_DISABLE_BEFORE_EXEC=(\d+)$", output, re.MULTILINE)
    if states != ["0" if label == "baseline" else "1"]:
        raise RuntimeError("missing, repeated or incorrect pre-exec THP policy check")


def measure(root, label, round_index, workload, thp=False):
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
    target = thp_target(root, label, ".test") if thp else [str(root / "results" / f"{label}.test")]
    revision = "candidate" if thp else label
    command = [
        "/usr/bin/time", "-f", "%e %U %S %M", "-o", str(metrics),
        *target,
        "-test.v", "-test.count=1", "-test.parallel=1", "-test.timeout=5m",
        f"-test.run={pattern}",
    ]
    with log.open("w") as output:
        with subprocess.Popen(command, cwd=root / revision / "sim", env=env,
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
    if thp:
        validate_thp_launch(log.read_text(), label)
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


def measure_query(root, label, round_index, case, thp=False):
    name = f"query-{label}-{round_index}-{case}"
    metrics, log = root / "results" / f"{name}.time", root / "results" / f"{name}.log"
    home = root / "results" / f"{name}.home"
    home.mkdir()
    env = dict(os.environ, **CONTROLS, HOME=str(home))
    target = thp_target(root, label, "-query.test") if thp else [str(root / "results" / f"{label}-query.test")]
    revision = "candidate" if thp else label
    command = ["/usr/bin/time", "-f", "%e %U %S %M", "-o", str(metrics),
               *target, "-test.run=^$", "-test.parallel=1",
               "-test.benchtime=2s", "-test.count=1", "-test.timeout=90s",
               f"-test.bench=^BenchmarkWarmShardQuery$/^{case}$"]
    with log.open("w") as output:
        with subprocess.Popen(command, cwd=root / revision / "kb" / "duckdb", env=env,
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
    if thp:
        validate_thp_launch(log.read_text(), label)
    elapsed, user, system, rss = map(float, metrics.read_text().split())
    return dict(result, label=label, round=round_index, case=case, process_elapsed_s=elapsed,
                process_user_s=user, process_system_s=system, process_peak_rss_mib=rss / 1024)


def query_report(records, paired=False):
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
        delta = 100 * (b / a - 1)
        if paired:
            keyed = {(r["label"], r["round"]): r["ns_per_op"] for r in records if r["case"] == case}
            delta = statistics.median(100 * (keyed["candidate", i] / keyed["baseline", i] - 1) for i in range(3))
        lines.append(f"| {case} | {a:.3f} | {b:.3f} | {delta:+.1f}% | {arss:.2f} | {brss:.2f} |")
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


def thp_report(records):
    summary = aggregate(records)
    lines = ["## DuckDB 2.0 application THP comparison", "",
             "One simulation binary, one query binary, one checkout and signed extension set on one hosted runner.",
             "Baseline = default THP; candidate = disabled before exec. Policy is checked by the launcher and inherited across exec.",
             "Three alternating rounds of fresh sequential processes; no allocator/parser tuning or production policy change.",
             "GOMAXPROCS=2, GOMEMLIMIT=384MiB, build -p=1, test -parallel=1; ordinary application DuckDB budgets remain in force.",
             "Simulation figures are medians of round sums (RSS: round maxima); changes are medians of within-round percentages.",
             "These measure simulation execution costs, not service throughput. Query timings below exclude fixture setup.", "",
             "| simulation metric | default | disabled | paired change |", "| --- | ---: | ---: | ---: |"]
    for metric in ("elapsed_s", "user_s", "system_s", "rss_mib"):
        changes = []
        for index in range(3):
            values = []
            for label in ("baseline", "candidate"):
                rows = [r[metric] for r in records if r["label"] == label and r["round"] == index]
                values.append(max(rows) if metric == "rss_mib" else sum(rows))
            changes.append(100 * (values[1] / values[0] - 1) if values[0] else None)
        delta = "n/a" if None in changes else f"{statistics.median(changes):+.1f}%"
        lines.append(f"| {metric} | {summary['baseline'][metric]:.2f} | {summary['candidate'][metric]:.2f} | {delta} |")
    return "\n".join(lines) + "\n"


def thp_schedule():
    for index in range(3):
        for workload in WORKLOADS[index:] + WORKLOADS[:index]:
            # Use the original position: rotating by index then using the
            # rotated position would cancel the intended policy alternation.
            labels = ("baseline", "candidate") if (index + WORKLOADS.index(workload)) % 2 == 0 else ("candidate", "baseline")
            for label in labels:
                yield index, workload, label


def verify_thp_binaries(root):
    expected = {}
    for line in (root / "results/binaries.sha256").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (\.\./results/candidate(?:-query)?\.test)", line)
        if not match or match[2] in expected:
            raise RuntimeError("invalid THP binary hash manifest")
        expected[match[2]] = match[1]
    if set(expected) != {"../results/candidate.test", "../results/candidate-query.test"}:
        raise RuntimeError("incomplete THP binary hash manifest")
    for name, digest in expected.items():
        with (root / "results" / Path(name).name).open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        if actual != digest:
            raise RuntimeError(f"measured binary changed: {name}")


def collect_thp(root, engines, env):
    require_ci(env)
    results = root / "results"
    records, queries, errors = [], [], []
    finished = integrity = workloads_complete = False
    expected_sim, expected_query = 6 * len(WORKLOADS), 6 * len(QUERY_CASES)

    def write(path, text):
        # Keep the previous complete snapshot if cancellation interrupts a write.
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(text)
        temporary.replace(path)

    def save():
        common = {"controls": CONTROLS, "engines": engines, "rounds": 3, "same_engine": True,
                  "thp_comparison": True, "finished": finished, "binary_integrity_verified": integrity, "errors": errors}
        write(results / "measurements.json", json.dumps(dict(common, seeds=SEEDS, measurements=records), indent=2) + "\n")
        write(results / "queries.json", json.dumps(dict(common, measurements=queries), indent=2) + "\n")
        text = (thp_report(records) if len(records) == expected_sim else
                "## DuckDB 2.0 application THP comparison\n")
        if len(queries) == expected_query:
            text += query_report(queries, paired=True)
        if not finished:
            text = "**Incomplete THP comparison: do not treat this as a completed application comparison.**\n\n" + text
        text += f"\nValidated processes: simulations {len(records)}/{expected_sim}; queries {len(queries)}/{expected_query}.\n"
        text += f"Final binary integrity verified: {integrity}.\n"
        if errors:
            text += "\n### Failures\n" + "\n".join(f"- {error}" for error in errors) + "\n"
        write(results / "summary.md", text)
        return text

    save()
    current = "initial binary integrity"
    try:
        verify_thp_binaries(root)
        for index, workload, label in thp_schedule():
            current = f"simulation {index}/{label}/{'/'.join(workload)}"
            print(current, flush=True)
            records.append(measure(root, label, index, workload, thp=True))
            save()
        for index in range(3):
            for position, case in enumerate(QUERY_CASES):
                labels = ("baseline", "candidate") if (index + position) % 2 == 0 else ("candidate", "baseline")
                for label in labels:
                    current = f"query {index}/{label}/{case}"
                    print(current, flush=True)
                    queries.append(measure_query(root, label, index, case, thp=True))
                    save()
        # Validate both complete tables before the final integrity gate.
        thp_report(records)
        query_report(queries, paired=True)
        workloads_complete = True
    except BaseException as error:
        errors.append(f"{current}: {type(error).__name__}: {error}")
    finally:
        try:
            verify_thp_binaries(root)
            integrity = True
        except (RuntimeError, OSError, ValueError) as error:
            errors.append(f"final binary integrity: {error}")
        finished = workloads_complete and integrity and not errors
        text = save()
        with open(env["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
        print(text)
    if not finished:
        raise RuntimeError("THP application comparison incomplete; inspect saved evidence")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3, choices=[3])
    parser.add_argument("--same-engine", action="store_true",
                        help="compare two application revisions with the identical stable 2.0 engine")
    parser.add_argument("--queries", action="store_true", help="also compare compiled warm production-query fixtures")
    parser.add_argument("--thp", action="store_true", help="same candidate binaries under default versus disabled pre-exec THP")
    args = parser.parse_args()
    require_ci(os.environ)
    root = args.root.resolve()
    results = root / "results"
    engines = {label: json.loads((results / f"{label}.engine.json").read_text())
               for label in ("baseline", "candidate")}
    validate_engines(engines, args.same_engine)
    if args.thp:
        if not args.same_engine or not args.queries:
            raise RuntimeError("THP comparison requires same-engine mode and query workloads")
        shas = [(results / f"{label}.sha").read_text().strip() for label in ("baseline", "candidate")]
        if not shas[0] or shas[0] != shas[1]:
            raise RuntimeError("THP comparison requires one application revision")
        for key in ("MALLOC_CONF", "DUCKDB_JE_MALLOC_CONF", "LD_PRELOAD", "GLIBC_TUNABLES", "GODEBUG"):
            if os.environ.get(key):
                raise RuntimeError(f"unexpected memory/runtime override: {key}")
        return collect_thp(root, engines, dict(os.environ))
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
