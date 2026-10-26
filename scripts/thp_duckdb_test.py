"""Pure validation/report tests; never execute the launcher or native probe."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from memory_duckdb_test import fixture as memory_fixture
from thp_duckdb import LABELS, MODES, POLICIES, RUNS, VERSIONS, collect, main, pairs, report, schedule, validate


def fixture(mode, label, policy):
    data = memory_fixture(mode, VERSIONS[label])
    data.update(thp_policy=policy, thp_checks=2 if mode == "ingest" else 40,
                thp_ingest_checked=mode == "ingest")
    for sample in data["samples"]:
        sample["thp_disabled"] = int(policy == "disabled")
        sample["os_kib"]["AnonHugePages"] = 0 if policy == "disabled" else 2048
        for mapping in sample["mappings"].values():
            mapping["anon_huge_pages_kib"] = 0 if policy == "disabled" else 1024
    return data


ENGINES = {label: {"version": VERSIONS[label], "source": "test"} for label in LABELS}


class THPTest(unittest.TestCase):
    def test_pairs_and_schedule(self):
        runs = list(schedule())
        self.assertEqual(len(runs), RUNS)
        self.assertEqual(set(runs), {(i, mode, label, policy) for i in range(3)
                                    for mode in MODES for label in LABELS for policy in POLICIES})
        self.assertEqual(runs[:4], [(0, "core", "baseline", "default"), (0, "core", "baseline", "disabled"),
                                   (0, "core", "candidate", "disabled"), (0, "core", "candidate", "default")])
        self.assertEqual(runs[20:22], [(1, "core", "candidate", "default"), (1, "core", "candidate", "disabled")])
        records = [{"round": i, "label": label, "policy": policy, "data": fixture(mode, label, policy)}
                   for i, mode, label, policy in runs]
        self.assertEqual(len(pairs(records, "core", "baseline")), 3)
        self.assertEqual(pairs(records[1:], "core", "baseline"), [])
        self.assertEqual(pairs(records + [records[0]], "core", "baseline"), [])
        text = report(records, [], True)
        self.assertNotIn("Incomplete", text)
        self.assertIn("| core | v2.0.0 | AnonHugePages | 2.00 | 0.00 | -100.0% |", text)
        self.assertIn("| core | +0.00 | +0.00 |", text)
        self.assertIn("Incomplete", report(records[1:], [], True))

    def test_validation(self):
        for mode in MODES:
            for label in LABELS:
                for policy in POLICIES:
                    validate(fixture(mode, label, policy), mode, label, policy, ENGINES[label])
        mutations = [lambda d: d.update(thp_policy="default"), lambda d: d.update(thp_checks=39),
                     lambda d: d.update(error="native failure"), lambda d: d.update(parser="legacy"),
                     lambda d: d.update(source="different"), lambda d: d["samples"][0].pop("thp_disabled"),
                     lambda d: d["samples"][0].update(thp_disabled=True),
                     lambda d: d["samples"][-1].update(thp_disabled=0),
                     lambda d: d["samples"][0]["os_kib"].update(AnonHugePages=2048),
                     lambda d: d["samples"][0]["mappings"]["/vss.duckdb_extension"].pop("anon_huge_pages_kib")]
        for mutation in mutations:
            data = fixture("configured", "candidate", "disabled")
            mutation(data)
            with self.subTest(data=data), self.assertRaises(RuntimeError):
                validate(data, "configured", "candidate", "disabled", ENGINES["candidate"])
        data = fixture("ingest", "candidate", "disabled")
        data["thp_ingest_checked"] = False
        with self.assertRaises(RuntimeError):
            validate(data, "ingest", "candidate", "disabled", ENGINES["candidate"])

    def test_preserves_failed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "thp").mkdir()
            calls = []

            def fake_run(command, prefix, cwd, env, timeout):
                calls.append(command)
                label = cwd.name
                policy = command[1]
                mode = command[command.index("-mode") + 1]
                self.assertEqual(command[0], str(root / "thp/thp-exec"))
                self.assertEqual(command[-2:], ["-thp-policy", policy])
                self.assertEqual(command[2], str(root / "thp" / f"{label}-profile"))
                self.assertEqual(env["GOMAXPROCS"], "2")
                self.assertEqual(env["GOMEMLIMIT"], "384MiB")
                self.assertTrue(Path(env["HOME"]).is_dir())
                data = fixture(mode, label, policy)
                if len(calls) == 1:
                    data["thp_checks"] = 0
                Path(str(prefix) + ".out").write_text(json.dumps(data))

            env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
                   "GITHUB_STEP_SUMMARY": str(root / "step.md")}
            with patch("thp_duckdb.run", side_effect=fake_run), patch("builtins.print"):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    collect(root, ENGINES, env)
            saved = json.loads((root / "thp/measurements.json").read_text())
            self.assertEqual(len(calls), RUNS)
            self.assertEqual(len(saved["runs"]), RUNS)
            self.assertEqual(len(saved["errors"]), 1)
            self.assertTrue(saved["finished"])

    def test_local_execution_refused(self):
        with patch.dict("os.environ", {}, clear=True), patch.object(sys, "argv", ["thp_duckdb", "--root", "."]):
            with self.assertRaisesRegex(RuntimeError, "restricted"):
                main()


if __name__ == "__main__":
    unittest.main()
