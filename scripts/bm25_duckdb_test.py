"""Pure query-shape harness checks, without running benchmarks or databases."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bm25_duckdb import collect, report, schedule, validate


def output(case):
    plan = json.dumps({"case": case, "sql": "SELECT 1", "plan": "Projection"})
    return f"BM25_PLAN {plan}\nBenchmarkBM25Shape/{case}-2 100 2000000 ns/op 100 B/op 2 allocs/op\nPASS\n"


class BM25Test(unittest.TestCase):
    def test_evidence(self):
        text = output("full_current")
        metrics, _ = validate(text, "full_current")
        self.assertEqual(metrics["ns_per_op"], 2000000)
        for bad in (text.replace("BM25_PLAN ", "missing "), text.replace("PASS", "FAIL"),
                    text + "--- SKIP: test\n", text + text, text.replace("full_current", "full_materialized")):
            with self.subTest(output=bad), self.assertRaises(RuntimeError):
                validate(bad, "full_current")

    def test_pairs(self):
        runs = list(schedule())
        self.assertEqual(len(set(runs)), 18)
        for a, b in zip(runs[::2], runs[1::2]):
            self.assertEqual(a[:2], b[:2])
            self.assertNotEqual(a[2], b[2])
        self.assertEqual([r[2] for r in runs if r[1] == "full"][::2], ["current", "materialized", "current"])
        records = [{"round": i, "workload": "full", "shape": shape, "metrics": {"ns_per_op": ms * 1000000}}
                   for i, values in enumerate(((2, 1), (10, 20), (100, 50)))
                   for shape, ms in zip(("current", "materialized"), values)]
        self.assertIn("| full | 10.000 | 20.000 | -50.0% |", report(records, [], True))
        for bad in (records[:-1], records + [records[0]]):
            self.assertNotIn("| full |", report(bad, [], True))

    def test_partial_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bm25").mkdir()
            (root / "bm25/candidate.engine.json").write_text(json.dumps({"version": "v2.0.0", "source": "test"}))
            env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "GITHUB_STEP_SUMMARY": str(root / "step.md")}
            calls = []

            def fake_run(command, prefix, cwd, env, timeout):
                calls.append((command, env))
                Path(str(prefix) + ".out").write_text("partial evidence")
                raise RuntimeError("child failed")

            with patch("bm25_duckdb.run", side_effect=fake_run), patch("bm25_duckdb.schedule", return_value=iter(((0, "full", "current"),))):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    collect(root, env)
            saved = json.loads((root / "bm25/measurements.json").read_text())
            self.assertIn("child failed", saved["errors"][0])
            self.assertEqual((root / "bm25/full_current-0.out").read_text(), "partial evidence")
            self.assertEqual(calls[0][1]["GOMAXPROCS"], "2")
            self.assertIn("-test.parallel=1", calls[0][0])
            self.assertIn("-test.bench=^BenchmarkBM25Shape$/^full_current$", calls[0][0])
            with self.assertRaisesRegex(RuntimeError, "restricted"):
                collect(root, {})


if __name__ == "__main__":
    unittest.main()
