"""CI-only, same-binary comparison of current and materialized BM25 query shapes."""

import argparse
import json
import os
from pathlib import Path

from compare_duckdb import CONTROLS, parse_query_result, require_ci
from parser_duckdb import paired_values
from profile_duckdb import run


WORKLOADS = ("full", "filtered", "scoped")
SHAPES = ("current", "materialized")


def schedule():
    for index in range(3):
        for position, workload in enumerate(WORKLOADS):
            shapes = SHAPES if (index + position) % 2 == 0 else tuple(reversed(SHAPES))
            for shape in shapes:
                yield index, workload, shape


def validate(output, case):
    metrics = parse_query_result(output, case, parent="BenchmarkBM25Shape")
    plans = [json.loads(line.removeprefix("BM25_PLAN ")) for line in output.splitlines() if line.startswith("BM25_PLAN ")]
    if len(plans) != 1 or plans[0].get("case") != case or not plans[0].get("sql") or not plans[0].get("plan"):
        raise RuntimeError("missing or ambiguous exact query/EXPLAIN evidence")
    if "PASS" not in output.splitlines():
        raise RuntimeError("benchmark process did not pass")
    return metrics, plans[0]


def report(records, errors, finished):
    lines = ["## BM25 query-shape comparison", "",
             "One stable 2.0 binary, signed extensions, same hosted runner; three alternating paired rounds in fresh processes.",
             "1024-row immutable shard; current production helper versus a test-only materialized CTE using the same FTS macro.",
             "Only physical row IDs/scores are materialized; payloads are joined after the ranked top-k limit.",
             "128MB, one DuckDB thread, GOMAXPROCS=2, GOMEMLIMIT=384MiB. Fixture setup, EXPLAIN and warmup are outside timers.",
             "Results are median ms/op; changes are median within-round percentages. This measures one warm shard, not end-to-end service latency.", ""]
    if not finished or errors or len(records) != 18:
        lines += ["**Incomplete diagnostics: only complete three-round pairs are tabulated.**", ""]
    lines += ["| workload | current ms | materialized ms | paired change |", "| --- | ---: | ---: | ---: |"]
    for workload in WORKLOADS:
        rows = [r for r in records if r["workload"] == workload]
        keyed = {(r["round"], r["shape"]): r for r in rows}
        if len(rows) != 6 or set(keyed) != {(i, s) for i in range(3) for s in SHAPES}:
            continue
        paired = [(keyed[i, "current"], keyed[i, "materialized"]) for i in range(3)]
        a, b, delta = paired_values(paired, lambda r: r["metrics"]["ns_per_op"] / 1e6)
        lines.append(f"| {workload} | {a:.3f} | {b:.3f} | {delta} |")
    lines += ["", "Raw logs retain exact SQL, EXPLAIN, operation counts and Go allocations; .time contains whole-process CPU/RSS",
              "including fixture setup. Materialization may be affected by optimizer filter pushdown; inspect plans before attribution.",
              "Correctness compares IDs, scores, payloads, ranking, filters/scopes, quoted terms and missing metadata.",
              "A separate reproducer records the current OR-filter/NULL-score precedence bug, which the candidate corrects."]
    if errors:
        lines += ["", "### Failed diagnostics"] + [f"- {e}" for e in errors]
    return "\n".join(lines) + "\n"


def collect(root, env):
    require_ci(env)
    results = root / "bm25"
    engine = json.loads((results / "candidate.engine.json").read_text())
    if engine.get("version") != "v2.0.0" or not engine.get("source"):
        raise RuntimeError("expected stable 2.0 engine metadata")
    records, errors = [], []
    finished = False

    def save():
        (results / "measurements.json").write_text(json.dumps({
            "engine": engine, "controls": CONTROLS, "runs": records, "errors": errors, "finished": finished,
        }, indent=2) + "\n")
        text = report(records, errors, finished)
        (results / "summary.md").write_text(text)
        return text

    save()
    try:
        for index, workload, shape in schedule():
            case = f"{workload}_{shape}"
            prefix = results / f"{case}-{index}"
            try:
                print(prefix.name, flush=True)
                home = results / f"{case}-{index}.home"
                home.mkdir()
                command = ["/usr/bin/time", "-f", "%e %U %S %M", "-o", str(prefix) + ".time",
                           str(results / "candidate-query.test"), "-test.run=^$", "-test.parallel=1",
                           "-test.benchtime=2s", "-test.count=1", "-test.timeout=90s",
                           f"-test.bench=^BenchmarkBM25Shape$/^{case}$"]
                run(command, prefix, root / "candidate", dict(env, **CONTROLS, HOME=str(home)), timeout=100)
                output = Path(str(prefix) + ".out").read_text()
                metrics, plan = validate(output, case)
                records.append({"round": index, "workload": workload, "shape": shape, "metrics": metrics, "explain": plan})
            except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
                errors.append(f"{prefix.name}: {error}")
            finally:
                save()
        finished = True
    finally:
        text = save()
        with open(env["GITHUB_STEP_SUMMARY"], "a") as output:
            output.write(text)
    if errors or len(records) != 18:
        raise RuntimeError("BM25 comparison incomplete; inspect saved diagnostics")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    collect(args.root.resolve(), dict(os.environ))


if __name__ == "__main__":
    main()
