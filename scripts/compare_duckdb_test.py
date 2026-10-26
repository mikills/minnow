"""Tooling-only tests: no benchmark processes are executed."""

from pathlib import Path
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from compare_duckdb import (QUERY_CASES, SEEDS, WORKLOADS, aggregate, collect_thp, measure, measure_query,
                           main, parse_query_result, query_report, report, require_ci, thp_report,
                           thp_schedule, validate_engines, validate_tests, validate_thp_launch,
                           verify_thp_binaries)


class ComparisonTest(unittest.TestCase):
    def test_engine_modes(self):
        stable = {"mode": "probe", "version": "v2.0.0", "source": "stable-source"}
        engines = {"baseline": dict(stable, version="v1.5.0"), "candidate": stable}
        validate_engines(engines)
        with self.assertRaisesRegex(RuntimeError, "identical stable"):
            validate_engines(engines, same_engine=True)
        engines["baseline"] = dict(stable)
        validate_engines(engines, same_engine=True)
        with self.assertRaisesRegex(RuntimeError, "expected pre-2.0"):
            validate_engines(engines)
        engines["baseline"]["source"] = "different-source"
        with self.assertRaisesRegex(RuntimeError, "identical stable"):
            validate_engines(engines, same_engine=True)

    def test_ci_only(self):
        for env in ({}, {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "self-hosted"}):
            with self.assertRaises(RuntimeError):
                require_ci(env)
        require_ci({"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted"})

    def test_matched_seeds(self):
        output = "\n".join(f"    --- PASS: TestSim/warm_vs_cold/seed_{seed} (0.01s)" for seed in SEEDS)
        self.assertEqual(validate_tests(output, "TestSim", "warm_vs_cold"), len(SEEDS))
        for bad in ("PASS\n", output.replace("seed_100", "seed_101"), output + "\n--- SKIP: missing"):
            with self.assertRaises(RuntimeError):
                validate_tests(bad, "TestSim", "warm_vs_cold")

    def test_summary(self):
        records = []
        for label in ("baseline", "candidate"):
            for index, value in enumerate((1, 100, 3)):
                for parent, scenario in WORKLOADS:
                    records.append({
                        "label": label, "round": index, "scenario": f"{parent}/{scenario}",
                        "elapsed_s": value, "user_s": value * 2, "system_s": 1,
                        "rss_mib": 128, "passed": len(SEEDS),
                    })
        summary = aggregate(records)
        self.assertEqual(summary["baseline"]["elapsed_s"], 3 * len(WORKLOADS))
        self.assertEqual(summary["candidate"]["passed"], len(SEEDS) * len(WORKLOADS))
        self.assertEqual(summary["candidate"]["rss_mib"], 128)
        text = report(summary, {"baseline": {"version": "v1.5.0"}, "candidate": {"version": "v2.0.0"}})
        self.assertIn("| change |", text)
        self.assertIn("+0.0%", text)
        same_engine = report(summary, {label: {"version": "v2.0.0"} for label in summary}, same_engine=True)
        self.assertIn("same-engine revision comparison", same_engine)
        with self.assertRaises(RuntimeError):
            aggregate(records[:-1])

    def test_process_controls(self):
        output = "\n".join(f"--- PASS: TestSim/warm_vs_cold/seed_{seed} (0.01s)" for seed in SEEDS)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results").mkdir()
            process = MagicMock()
            process.__enter__.return_value = process
            process.wait.return_value = 0

            def spawn(command, **kwargs):
                kwargs["stdout"].write(output)
                Path(command[command.index("-o") + 1]).write_text("1 2 3 4096")
                self.assertEqual(kwargs["env"]["GOMAXPROCS"], "2")
                self.assertEqual(kwargs["env"]["GOMEMLIMIT"], "384MiB")
                self.assertEqual(kwargs["cwd"], root / "baseline" / "sim")
                self.assertTrue(kwargs["start_new_session"])
                self.assertIn("-test.parallel=1", command)
                self.assertIn(str(root / "results" / "baseline.test"), command)
                return process

            with patch("compare_duckdb.subprocess.Popen", side_effect=spawn):
                result = measure(root, "baseline", 0, ("TestSim", "warm_vs_cold"))
            self.assertEqual(result["rss_mib"], 4)
            self.assertEqual(result["passed"], len(SEEDS))

    def test_timeout_kills_process_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results").mkdir()
            process = MagicMock(pid=12345)
            process.__enter__.return_value = process
            process.wait.side_effect = [subprocess.TimeoutExpired("test", 330), -9]
            with patch("compare_duckdb.subprocess.Popen", return_value=process), patch("compare_duckdb.os.killpg") as kill:
                with self.assertRaisesRegex(RuntimeError, "timed out"):
                    measure(root, "candidate", 0, ("TestSim", "warm_vs_cold"))
                kill.assert_called_once()
                self.assertEqual(kill.call_args.args[0], 12345)

    def test_query_results(self):
        output = "BenchmarkWarmShardQuery/full_512-2 123 1500000 ns/op 4096 B/op 12 allocs/op\nPASS\n"
        self.assertEqual(parse_query_result(output, "full_512")["ns_per_op"], 1500000)
        for bad in ("PASS\n", output + output, output + "--- SKIP: missing", output.replace("123 ", "0 ")):
            with self.assertRaises(RuntimeError):
                parse_query_result(bad, "full_512")
        records = [{"label": label, "case": case, "round": index,
                    "ns_per_op": ns, "process_peak_rss_mib": 128}
                   for index in range(3) for case in QUERY_CASES
                   for label, ns in (("baseline", 2000000), ("candidate", 1000000))]
        self.assertIn("2.000 | 1.000 | -50.0%", query_report(records))
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            query_report(records[:-1])

    def test_query_process_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results").mkdir()
            process = MagicMock()
            process.__enter__.return_value = process
            process.wait.return_value = 0

            def spawn(command, **kwargs):
                kwargs["stdout"].write("BenchmarkWarmShardQuery/full_512-2 123 1500000 ns/op 4096 B/op 12 allocs/op\nPASS\n")
                Path(command[command.index("-o") + 1]).write_text("1 2 3 4096")
                self.assertEqual(kwargs["env"]["GOMAXPROCS"], "2")
                self.assertEqual(kwargs["cwd"], root / "baseline" / "kb" / "duckdb")
                self.assertIn("-test.benchtime=2s", command)
                self.assertIn("-test.run=^$", command)
                return process

            with patch("compare_duckdb.subprocess.Popen", side_effect=spawn):
                result = measure_query(root, "baseline", 0, "full_512")
            self.assertEqual(result["process_peak_rss_mib"], 4)
            self.assertEqual(result["ns_per_op"], 1500000)

    def test_thp_uses_one_binary_and_checks_launcher(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results").mkdir()
            for query in (False, True):
                for label in ("baseline", "candidate"):
                    policy = "default" if label == "baseline" else "disabled"
                    state = "0" if label == "baseline" else "1"
                    suffix = "-query.test" if query else ".test"
                    process = MagicMock()
                    process.__enter__.return_value = process
                    process.wait.return_value = 0

                    def spawn(command, **kwargs):
                        target = command.index(str(root / "results/thp-exec"))
                        self.assertEqual(command[target:target+3], [str(root / "results/thp-exec"), policy,
                                                                  str(root / "results" / f"candidate{suffix}")])
                        expected_cwd = root / "candidate" / ("kb/duckdb" if query else "sim")
                        self.assertEqual(kwargs["cwd"], expected_cwd)
                        self.assertEqual(kwargs["env"]["GOMAXPROCS"], "2")
                        self.assertEqual(kwargs["env"]["GOMEMLIMIT"], "384MiB")
                        output = ("BenchmarkWarmShardQuery/full_512-2 123 1500000 ns/op 4096 B/op 12 allocs/op\nPASS\n"
                                  if query else "\n".join(f"--- PASS: TestSim/warm_vs_cold/seed_{seed} (0.01s)" for seed in SEEDS))
                        kwargs["stdout"].write(f"THP_DISABLE_BEFORE_EXEC={state}\n" + output)
                        Path(command[command.index("-o") + 1]).write_text("1 2 3 4096")
                        return process

                    with patch("compare_duckdb.subprocess.Popen", side_effect=spawn):
                        if query:
                            measure_query(root, label, 0, "full_512", thp=True)
                        else:
                            measure(root, label, 0, ("TestSim", "warm_vs_cold"), thp=True)
            for bad in ("", "THP_DISABLE_BEFORE_EXEC=0\n", "THP_DISABLE_BEFORE_EXEC=3\n",
                        "THP_DISABLE_BEFORE_EXEC=1\nTHP_DISABLE_BEFORE_EXEC=1\n"):
                with self.assertRaises(RuntimeError):
                    validate_thp_launch(bad, "candidate")

    def test_thp_reports_within_round_changes(self):
        records, queries = [], []
        for i, (a, b) in enumerate(((1, 10), (100, 101), (3, 3))):
            for label, value in (("baseline", a), ("candidate", b)):
                for parent, scenario in WORKLOADS:
                    records.append({"label": label, "round": i, "scenario": f"{parent}/{scenario}", "passed": 6,
                                    "elapsed_s": value, "user_s": value, "system_s": value, "rss_mib": value})
                for case in QUERY_CASES:
                    queries.append({"label": label, "round": i, "case": case,
                                    "ns_per_op": value * 1e6, "process_peak_rss_mib": 1})
        self.assertIn("| rss_mib | 3.00 | 10.00 | +1.0% |", thp_report(records))
        self.assertIn("3.000 | 10.000 | +1.0%", query_report(queries, paired=True))

    def test_thp_rejects_mixed_revisions_and_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            results = root / "results"
            results.mkdir()
            engine = {"version": "v2.0.0", "source": "stable"}
            for label in ("baseline", "candidate"):
                (results / f"{label}.engine.json").write_text(json.dumps(engine))
                (results / f"{label}.sha").write_text(label)
            env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted"}
            args = ["compare_duckdb", "--root", str(root), "--same-engine", "--queries", "--thp"]
            with patch.dict("os.environ", env, clear=True), patch.object(sys, "argv", args):
                with self.assertRaisesRegex(RuntimeError, "one application revision"):
                    main()
            (results / "baseline.sha").write_text("candidate")
            with patch.dict("os.environ", dict(env, LD_PRELOAD="unexpected"), clear=True), patch.object(sys, "argv", args):
                with self.assertRaisesRegex(RuntimeError, "override"):
                    main()

    def test_thp_schedule(self):
        runs = list(thp_schedule())
        expected = {(i, workload, label) for i in range(3) for workload in WORKLOADS
                    for label in ("baseline", "candidate")}
        self.assertEqual(len(runs), len(expected))
        self.assertEqual(set(runs), expected)
        for first, second in zip(runs[::2], runs[1::2]):
            self.assertEqual(first[:2], second[:2])
            self.assertNotEqual(first[2], second[2])
        for workload in WORKLOADS:
            orders = [[label for i, w, label in runs if i == index and w == workload] for index in range(3)]
            self.assertEqual(orders[0], orders[1][::-1])
            self.assertEqual(orders[1], orders[2][::-1])

    def test_thp_evidence(self):
        # All workloads are fake records: exercise persistence/failure handling,
        # never start the launcher, a database, or a benchmark process.
        for failure in (None, "simulation", "query", "integrity", "write"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                results = root / "results"
                results.mkdir()
                manifest = []
                for name in ("candidate.test", "candidate-query.test"):
                    content = name.encode()
                    (results / name).write_bytes(content)
                    manifest.append(f"{hashlib.sha256(content).hexdigest()}  ../results/{name}\n")
                (results / "binaries.sha256").write_text("".join(manifest))
                verify_thp_binaries(root)
                env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
                       "GITHUB_STEP_SUMMARY": str(root / "step.md")}
                counts = {"simulation": 0, "query": 0}
                writes = []
                replace = Path.replace

                def atomic_replace(source, target):
                    self.assertEqual(source.name, target.name + ".tmp")
                    if target.suffix == ".json":
                        incoming = json.loads(source.read_text())
                        previous = json.loads(target.read_text()) if target.exists() else None
                        self.assertIn("finished", incoming)
                        if (failure == "write" and target.name == "measurements.json"
                                and len(incoming["measurements"]) == 1 and not writes):
                            self.assertEqual(previous["measurements"], [])
                            writes.append("interrupted")
                            raise OSError("interrupted atomic replacement")
                    return replace(source, target)

                def check_progress(kind):
                    for filename, key in (("measurements.json", "simulation"), ("queries.json", "query")):
                        saved = json.loads((results / filename).read_text())
                        self.assertEqual(len(saved["measurements"]), counts[key])
                        self.assertFalse(saved["finished"])
                    self.assertIn("Incomplete THP comparison", (results / "summary.md").read_text())
                    if failure == kind and counts[kind] == 1:
                        raise RuntimeError(f"injected {kind} failure")
                    counts[kind] += 1

                def simulation(root, label, index, workload, thp):
                    self.assertTrue(thp)
                    check_progress("simulation")
                    return {"label": label, "round": index, "scenario": "/".join(workload), "passed": len(SEEDS),
                            "elapsed_s": 1, "user_s": 2, "system_s": 1, "rss_mib": 128}

                def query(root, label, index, case, thp):
                    self.assertTrue(thp)
                    check_progress("query")
                    if failure == "integrity" and counts["query"] == 6 * len(QUERY_CASES):
                        (results / "candidate-query.test").write_bytes(b"changed")
                    return {"label": label, "round": index, "case": case,
                            "ns_per_op": 1000, "process_peak_rss_mib": 128}

                with patch("compare_duckdb.measure", side_effect=simulation), \
                        patch("compare_duckdb.measure_query", side_effect=query), \
                        patch.object(Path, "replace", atomic_replace), patch("builtins.print"):
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, "comparison incomplete"):
                            collect_thp(root, {}, env)
                    else:
                        collect_thp(root, {}, env)
                for filename, kind in (("measurements.json", "simulation"), ("queries.json", "query")):
                    saved = json.loads((results / filename).read_text())
                    self.assertEqual(saved["finished"], failure is None)
                    self.assertEqual(saved["binary_integrity_verified"], failure != "integrity")
                    self.assertEqual(len(saved["measurements"]), counts[kind])
                    self.assertEqual(bool(saved["errors"]), failure is not None)
                text = (results / "summary.md").read_text()
                self.assertEqual("Incomplete THP comparison" in text, failure is not None)
                self.assertEqual((root / "step.md").read_text(), text)
                if failure == "integrity":
                    self.assertIn("final binary integrity: measured binary changed", text)
                elif failure == "query":
                    self.assertIn("queries 1/42", text)
                    self.assertIn("injected query failure", text)
                elif failure == "write":
                    self.assertEqual(writes, ["interrupted"])
                    self.assertIn("interrupted atomic replacement", text)
                elif failure is None:
                    self.assertEqual(counts, {"simulation": 6 * len(WORKLOADS), "query": 6 * len(QUERY_CASES)})


if __name__ == "__main__":
    unittest.main()
