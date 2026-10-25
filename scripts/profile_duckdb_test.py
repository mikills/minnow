"""Pure tooling tests. No database, perf, or heaptrack processes are run."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from profile_duckdb import run, summary, validate_probe


def configured(version="v2.0.0", iterations=20, elapsed=1500000):
    def sample(stage):
        return {"stage": stage, "elapsed_ns": elapsed, "user_ns": 1000000, "system_ns": 0,
                "go_heap_bytes": 1048576, "os_kib": {"Rss": 2048, "Anonymous": 1024},
                "glibc_bytes": {"system_current": 1048576, "fast": 0, "rest": 524288, "mmap": 0}}
    stages = ["initial"] + [s for _ in range(iterations) for s in ("open", "load_vss", "load_fts", "close")]
    stages += ["after_gc", "after_trim"]
    return {"mode": "configured", "version": version, "iterations": iterations,
            "samples": [sample(stage) for stage in stages]}


class ProfileTest(unittest.TestCase):
    def test_stage_completeness(self):
        data = configured()
        validate_probe(data, "configured", 20, "v2.0.0")
        with self.assertRaisesRegex(RuntimeError, "mismatch"):
            validate_probe(data, "configured", 20, "v1.5.0")
        data["samples"].pop()
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            validate_probe(data, "configured", 20, "v2.0.0")

    def test_report_units_and_limitations(self):
        records = []
        for index in range(3):
            records.append({"label": "baseline", "round": index, "data": configured("v1.5.0", elapsed=1000000)})
            records.append({"label": "candidate", "round": index, "data": configured()})
        text = summary(records, ["perf unavailable"])
        self.assertIn("1.000 | 1.500 | +50.0%", text)
        self.assertIn("2.00 | 1.00 | 1.00 | 1.00 | 0.50 | 0.00", text)
        self.assertIn("perf unavailable", text)
        self.assertIn("not proof of a leak", text)

    def test_dotted_output_names_and_timeout_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "cpu-baseline-configured.leaf"
            process = MagicMock(pid=12345)
            process.__enter__.return_value = process
            process.wait.return_value = 0
            with patch("profile_duckdb.subprocess.Popen", return_value=process):
                run(["fake-perf"], prefix, Path(directory), {})
            self.assertTrue(Path(str(prefix) + ".out").exists())
            self.assertFalse(prefix.with_suffix(".out").exists())
            process.wait.side_effect = [subprocess.TimeoutExpired("fake-perf", 1), -9]
            with patch("profile_duckdb.subprocess.Popen", return_value=process), patch("profile_duckdb.os.killpg") as kill:
                with self.assertRaisesRegex(RuntimeError, "timed out"):
                    run(["fake-perf"], prefix, Path(directory), {}, timeout=1)
                kill.assert_called_once()


if __name__ == "__main__":
    unittest.main()
