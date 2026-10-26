"""Pure parser-experiment control, reporting, and failure-evidence tests."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from parser_duckdb import MODES, SETTINGS, collect, report, schedule, validate
from profile_duckdb_test import configured, query_split


ENGINE = {"version": "v2.0.0", "source": "test-source"}


def fixture(parser="peg", mode="configured"):
    data = query_split(iterations=MODES[mode]) if mode == "query_split" else configured()
    opens = 1 if mode == "query_split" else 20
    data.update(parser=parser, parser_checks=2 * opens, source=ENGINE["source"],
                parser_settings=dict(SETTINGS, enable_legacy_parser=str(parser == "legacy").lower()))
    for stage in ("load_legacy_parser", "select_parser"):
        data["samples"] += [dict(data["samples"][0], stage=stage) for _ in range(opens)]
    return data


class ParserTest(unittest.TestCase):
    def test_schedule(self):
        runs = list(schedule())
        self.assertEqual(len(runs), 24)
        self.assertEqual(len(set(runs)), 24)
        for position in range(0, len(runs), 2):
            a, b = runs[position:position + 2]
            self.assertEqual(a[:2], b[:2])
            self.assertEqual({a[2], b[2]}, {"peg", "legacy"})
        for mode in MODES:
            firsts = [r[2] for r in runs[::2] if r[1] == mode]
            self.assertEqual(firsts[0], firsts[2])
            self.assertNotEqual(firsts[0], firsts[1])

    def test_controls_and_completeness(self):
        for parser in ("peg", "legacy"):
            for mode in ("configured", "query_split"):
                validate(fixture(parser, mode), mode, parser, ENGINE)
        mutations = [
            lambda d: d.update(parser="legacy"), lambda d: d.update(source="other-engine"),
            lambda d: d.update(parser_checks=2), lambda d: d.update(error="failed"),
            lambda d: d.update(memory_attribution=True),
            lambda d: d["parser_settings"].update(allow_unsigned_extensions="true"),
            lambda d: d["parser_settings"].update(threads="2"),
            lambda d: d["parser_settings"].update(enable_legacy_parser="true"),
            lambda d: d["parser_settings"].update(allow_parser_override_extension="fallback"),
            lambda d: d["parser_settings"].update(legacy_parser_loaded="false"),
            lambda d: d["samples"].pop(),
            lambda d: d["samples"].append(dict(d["samples"][0], stage="unknown")),
            lambda d: d["samples"][0].update(user_ns=-1),
            lambda d: d["samples"][0]["os_kib"].update(Rss=False),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                data = fixture()
                mutate(data)
                with self.assertRaises(RuntimeError):
                    validate(data, "configured", "peg", ENGINE)
        for plan in ("vector", "bound_vector", "bound_vector_axis1"):
            with self.subTest(plan=plan):
                data = fixture(mode="query_split")
                data["query_plans"][plan] = "Sequential Scan"
                with self.assertRaisesRegex(RuntimeError, "lost HNSW"):
                    validate(data, "query_split", "peg", ENGINE)

    def test_paired_reporting(self):
        records = []
        # Ratio-of-medians is +100%; median paired change is -50%.
        for index, (peg, legacy) in enumerate(((2, 1), (10, 20), (100, 50))):
            for parser, ms in (("peg", peg), ("legacy", legacy)):
                data = fixture(parser)
                for sample in data["samples"]:
                    sample["elapsed_ns"] = ms * 1000000
                records.append({"parser": parser, "round": index, "data": data})
        text = report(records, [], True)
        self.assertIn("configured / open | 10.000 | 20.000 | -50.0%", text)
        self.assertIn("configured | Rss | 2.00 | 2.00 | +0.0%", text)
        self.assertIn("cannot isolate all grammar residency", text)
        self.assertIn("Incomplete diagnostics", text)
        for bad in (records[:-1], records + [records[0]]):
            self.assertNotIn("| configured / open |", report(bad, [], True))
        duplicate = copy.deepcopy(records)
        duplicate[-1]["round"] = 0
        self.assertNotIn("| configured / open |", report(duplicate, [], True))

    def test_failed_run_preserves_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "parser").mkdir()
            env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
                   "GITHUB_STEP_SUMMARY": str(root / "step.md")}
            calls = []

            def fake_run(command, prefix, cwd, env, timeout):
                calls.append((command, env["HOME"]))
                data = fixture(command[-1])
                if command[-1] == "peg":
                    data["parser_settings"]["threads"] = "2"
                Path(str(prefix) + ".out").write_text(json.dumps(data))
                if command[-1] == "legacy":
                    raise RuntimeError("simulated child failure")

            with patch("parser_duckdb.schedule", return_value=iter(((0, "configured", "peg"), (0, "configured", "legacy")))), \
                    patch("parser_duckdb.run", side_effect=fake_run):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    collect(root, ENGINE, env)
            saved = json.loads((root / "parser/measurements.json").read_text())
            self.assertEqual(len(saved["errors"]), 2)
            self.assertEqual(len(saved["runs"]), 1)
            self.assertEqual(saved["runs"][0]["data"]["parser_settings"]["threads"], "2")
            self.assertTrue((root / "parser/legacy-configured-0.out").exists())
            self.assertNotIn("| configured / open |", (root / "parser/summary.md").read_text())
            self.assertEqual(calls[0][0][0], calls[1][0][0])
            self.assertEqual(calls[0][0][:-1], calls[1][0][:-1])
            self.assertNotEqual(calls[0][1], calls[1][1])
            with self.assertRaisesRegex(RuntimeError, "restricted"):
                collect(root, ENGINE, {})


if __name__ == "__main__":
    unittest.main()
