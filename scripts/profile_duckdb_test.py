"""Pure tooling tests. No database, perf, or heaptrack processes are run."""

from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from profile_duckdb import (SPLIT_CASES, SPLIT_PATH_STAGES,
                           has_native_symbols, run, split_summary, summary, validate_probe)


def configured(version="v2.0.0", iterations=20, elapsed=1500000):
    def sample(stage):
        return {"stage": stage, "elapsed_ns": elapsed, "user_ns": 1000000, "system_ns": 0,
                "go_heap_bytes": 1048576, "os_kib": {"Rss": 2048, "Anonymous": 1024},
                "glibc_bytes": {"system_current": 1048576, "fast": 0, "rest": 524288, "mmap": 0}}
    stages = ["initial"] + [s for _ in range(iterations) for s in ("open", "load_vss", "load_fts", "close")]
    stages += ["after_gc", "after_trim"]
    return {"mode": "configured", "version": version, "iterations": iterations,
            "samples": [sample(stage) for stage in stages]}


def query_split(version="v2.0.0", iterations=2, path="all"):
    data = configured(version, iterations=1)
    data.update(mode="query_split", iterations=iterations, query_path=path,
                query_plans={"vector": "Hnsw Index Scan", "bound_vector": "Hnsw Index Scan",
                             "bound_vector_axis1": "Hnsw Index Scan", "bm25": "Projection"})
    # One database, fixed seed data, both index builds and the checkpoint.
    for stage in ["append"] * 8 + ["hnsw", "fts_index", "checkpoint"]:
        data["samples"].append(dict(data["samples"][0], stage=stage))
    for case in SPLIT_CASES:
        for stage in SPLIT_PATH_STAGES[path]:
            elapsed = 8000000 if stage == "adhoc" else 2000000
            for _ in range(iterations):
                data["samples"].append(dict(data["samples"][0], stage=f"{stage}_{case}", elapsed_ns=elapsed))
    return data


class ProfileTest(unittest.TestCase):
    def test_native_hash_symbols(self):
        self.assertTrue(has_native_symbols("90.83% candidate-profile [.] mbedtls_internal_sha256_process - -"))
        self.assertTrue(has_native_symbols("1.96% candidate-profile [.] duckdb::LogicalOperatorVisitor::VisitExpression()"))
        self.assertFalse(has_native_symbols("90.83% candidate-profile [.] crypto/internal/fips140/sha256.blockSHANI.abi0"))
        self.assertFalse(has_native_symbols("90.83% candidate-profile [.] 0x0000000012345678"))

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

    def test_query_split_completeness(self):
        data = query_split()
        validate_probe(data, "query_split", 2, "v2.0.0")
        data["samples"].pop()
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            validate_probe(data, "query_split", 2, "v2.0.0")
        data = query_split()
        data["query_path"] = "reuse"
        with self.assertRaisesRegex(RuntimeError, "requested measured paths"):
            validate_probe(data, "query_split", 2, "v2.0.0")
        for path in ("prepare", "reuse", "adhoc"):
            with self.subTest(path=path):
                validate_probe(query_split(path=path), "query_split", 2, "v2.0.0", path)
        data = query_split()
        del data["query_plans"]["bound_vector_axis1"]
        with self.assertRaisesRegex(RuntimeError, "both parameter plans"):
            validate_probe(data, "query_split", 2, "v2.0.0")
        data = query_split()
        data["query_plans"]["vector"] = "Seq Scan"
        with self.assertRaisesRegex(RuntimeError, "lost HNSW"):
            validate_probe(data, "query_split", 2, "v2.0.0")

    def test_query_split_summary_rejects_lost_index(self):
        records = [{"label": label, "round": index, "data": query_split(version)}
                   for index in range(3)
                   for label, version in (("baseline", "v1.5.0"), ("candidate", "v2.0.0"))]
        text = split_summary(records)
        self.assertIn("8.000 | 2.000 | 2.000 | 2.000 | 2.000 | +75.0% | yes", text)
        self.assertIn("deferred optimization", text)
        for record in records:
            if record["label"] == "candidate":
                record["data"]["query_plans"]["bound_vector_axis1"] = "Seq Scan"
        text = split_summary(records)
        self.assertIn("not comparable | NO — reject variant", text)

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
