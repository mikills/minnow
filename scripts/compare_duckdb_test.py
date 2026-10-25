"""Tooling-only tests: no benchmark processes are executed."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from compare_duckdb import (QUERY_CASES, SEEDS, WORKLOADS, aggregate, measure, measure_query,
                           parse_query_result, query_report, report, require_ci, validate_engines, validate_tests)


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


if __name__ == "__main__":
    unittest.main()
