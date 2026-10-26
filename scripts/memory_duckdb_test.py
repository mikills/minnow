"""Pure memory-diagnostic tooling tests; never start a database or native probe."""

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from memory_duckdb import (JE_KEYS, LABELS, MODES, VERSIONS, main, report,
                           schedule, validate)


def fixture(mode="configured", version="v2.0.0"):
    stages = ["initial"]
    for _ in range(1 if mode == "ingest" else 20):
        stages += ["open"]
        if mode in ("vss", "configured", "ingest"):
            stages += ["load_vss"]
        if mode in ("fts", "configured", "ingest"):
            stages += ["load_fts"]
        if mode == "ingest":
            stages += ["append"] * 20 + ["hnsw", "fts_index", "checkpoint"]
        stages += ["close"]
    stages += ["after_gc", "after_jemalloc_purge", "after_trim"]
    samples = []
    for stage in stages:
        closed = stage in {"initial", "close", "after_gc", "after_jemalloc_purge", "after_trim"}
        row = {"stage": stage, "elapsed_ns": 1, "user_ns": 1, "system_ns": 1,
               "os_kib": {"Rss": 10240 if stage == "after_gc" else 8192, "Anonymous": 4096},
               "go_heap_bytes": 1048576,
               "mappings": {"/vss.duckdb_extension": {"rss_kib": 2048, "anonymous_kib": 1024}},
               "jemalloc": {"version": "5.3.0", "bytes": {k: 2097152 for k in JE_KEYS}}}
        if not closed:
            row["duckdb_memory"] = [{"tag": "BASE_TABLE", "memory_usage_bytes": 1048576,
                                     "temporary_storage_bytes": 0}]
        samples.append(row)
    return {"mode": mode, "version": version, "source": "test", "iterations": 20,
            "memory_attribution": True, "settings": {"threads": "1", "memory_limit": "122.0 MiB"},
            "samples": samples}


class MemoryTest(unittest.TestCase):
    def test_schedule(self):
        runs = list(schedule())
        self.assertEqual(len(runs), 30)
        self.assertEqual(len(set(runs)), 30)
        self.assertEqual(runs[:4], [(0, "core", "baseline"), (0, "core", "candidate"),
                                    (0, "vss", "candidate"), (0, "vss", "baseline")])
        self.assertEqual(runs[10:12], [(1, "core", "candidate"), (1, "core", "baseline")])

    def test_lifecycle(self):
        for mode in MODES:
            for version in VERSIONS.values():
                with self.subTest(mode=mode, version=version):
                    validate(fixture(mode, version), mode, version)

    def test_reject_incomplete(self):
        def missing_purge(d):
            d["samples"] = [s for s in d["samples"] if s["stage"] != "after_jemalloc_purge"]

        def wrong_order(d):
            d["samples"][-3], d["samples"][-2] = d["samples"][-2], d["samples"][-3]

        mutations = [lambda d: d.update(memory_attribution=False), missing_purge, wrong_order,
                     lambda d: d.update(diagnostics=["purge failed"]),
                     lambda d: d["settings"].update(threads="2"),
                     lambda d: d["settings"].update(memory_limit="1.0 GiB"),
                     lambda d: d["samples"][0]["jemalloc"].update(errors=["stats unavailable"]),
                     lambda d: d["samples"][0]["jemalloc"]["bytes"].pop("allocated"),
                     lambda d: d["samples"][0]["jemalloc"]["bytes"].update(allocated=-1),
                     lambda d: d["samples"][0].pop("mappings"),
                     lambda d: d["samples"][1].pop("duckdb_memory"),
                     lambda d: d["samples"][-1].update(duckdb_memory=[]),
                     lambda d: d["samples"][1]["duckdb_memory"].append(copy.deepcopy(d["samples"][1]["duckdb_memory"][0])),
                     lambda d: d["samples"][1]["duckdb_memory"][0].update(memory_usage_bytes=-1)]
        for mutation in mutations:
            data = fixture()
            mutation(data)
            with self.subTest(data=data), self.assertRaises(RuntimeError):
                validate(data, "configured", "v2.0.0")

    def test_report(self):
        records = [{"label": label, "round": index, "data": fixture(mode, VERSIONS[label])}
                   for index in range(3) for mode in MODES for label in LABELS]
        text = report(records, [], True)
        self.assertNotIn("Incomplete", text)
        self.assertIn("| after_gc | baseline | 10.00 | 4.00 | 2.00 | 1.00 | — | 2.00", text)
        self.assertIn("| open | candidate | 8.00 | 4.00 | 2.00 | 1.00 | 1.00 | 2.00", text)
        self.assertIn("| configured | 2.00 | 2.00 |", text)
        self.assertIn("VIRTUAL", text)
        self.assertIn("not proof of a leak", text)
        self.assertIn("Incomplete", report(records[:-1], [], True))
        self.assertIn("Incomplete", report(records, ["purge failed"], True))
        self.assertIn("purge failed", report(records, ["purge failed"], True))

    def test_main_controls_and_partial_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            results = root / "memory"
            results.mkdir()
            for label in LABELS:
                (results / f"{label}.engine.json").write_text(json.dumps({"version": VERSIONS[label], "source": "test"}))
            summary = root / "summary.md"
            calls = []

            def fake_run(command, prefix, cwd, env, timeout):
                calls.append(command)
                label = "baseline" if "baseline-profile" in command[0] else "candidate"
                self.assertEqual(cwd, root / label)
                self.assertEqual(env["GOMAXPROCS"], "2")
                self.assertEqual(env["GOMEMLIMIT"], "384MiB")
                self.assertTrue(Path(env["HOME"]).is_dir())
                self.assertEqual(timeout, 120)
                self.assertIn("-memory-attribution", command)
                data = fixture(command[command.index("-mode") + 1], VERSIONS[label])
                if len(calls) == 1:
                    data["diagnostics"] = ["purge unavailable"]
                Path(str(prefix) + ".out").write_text(json.dumps(data))

            env = {"GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted", "GITHUB_STEP_SUMMARY": str(summary)}
            with patch.dict("os.environ", env, clear=True), patch.object(sys, "argv", ["memory_duckdb", "--root", directory]), \
                    patch("memory_duckdb.run", side_effect=fake_run), patch("builtins.print"):
                with self.assertRaisesRegex(SystemExit, "incomplete"):
                    main()
            self.assertEqual(len(calls), 30)
            saved = json.loads((results / "measurements.json").read_text())
            self.assertTrue(saved["finished"])
            self.assertEqual(len(saved["runs"]), 30, "unavailable native evidence must not be discarded")
            self.assertEqual(len(saved["errors"]), 1)
            self.assertIn("purge unavailable", summary.read_text())

    def test_ci_only(self):
        with patch.dict("os.environ", {}, clear=True), patch.object(sys, "argv", ["memory_duckdb", "--root", "."]):
            with self.assertRaisesRegex(RuntimeError, "restricted"):
                main()


if __name__ == "__main__":
    unittest.main()
