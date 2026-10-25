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


def report(summary, engines):
    lines = ["## DuckDB engine comparison", "",
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
    args = parser.parse_args()
    require_ci(os.environ)
    root = args.root.resolve()
    results = root / "results"
    engines = {label: json.loads((results / f"{label}.engine.json").read_text())
               for label in ("baseline", "candidate")}
    if engines["candidate"]["version"] != "v2.0.0" or engines["baseline"]["version"] == "v2.0.0":
        raise RuntimeError(f"expected pre-2.0 base vs stable 2.0, got {engines}")
    records = []
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
        text = report(summary, engines)
        (results / "summary.md").write_text(text)
        print(text)
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    finally:
        (results / "measurements.json").write_text(json.dumps({
            "controls": CONTROLS, "engines": engines, "seeds": SEEDS,
            "rounds": args.rounds, "measurements": records,
        }, indent=2) + "\n")


if __name__ == "__main__":
    main()
