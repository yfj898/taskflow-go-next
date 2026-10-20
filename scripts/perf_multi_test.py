import json
import tempfile
import unittest
from pathlib import Path

from perf_multi import build_summary, discover_runs


def write_run(root: Path, index: int, rps: float, p95: float, concurrency: int = 100) -> Path:
    run_dir = root / f"run-{index}"
    run_dir.mkdir()
    payload = {
        "concurrency": concurrency,
        "duration_s": 15.0 + index / 1000,
        "requests": int(rps * 15),
        "rps": rps,
        "latency_ms": {
            "avg": p95 / 2,
            "p50": p95 / 2.5,
            "p95": p95,
            "p99": p95 * 1.2,
            "max": p95 * 2,
        },
        "status": {"201": int(rps * 15)},
        "errors": {},
    }
    path = run_dir / "load.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class PerfMultiTest(unittest.TestCase):
    def test_build_summary_reports_median_and_sample_variance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_run(root, 1, 100.0, 30.0)
            write_run(root, 2, 200.0, 20.0)
            write_run(root, 3, 300.0, 10.0)

            summary = build_summary(discover_runs(root))

            self.assertEqual(summary["run_count"], 3)
            self.assertEqual(summary["aggregates"]["rps"]["median"], 200.0)
            self.assertEqual(summary["aggregates"]["rps"]["sample_variance"], 10000.0)
            self.assertAlmostEqual(summary["aggregates"]["rps"]["cv_pct"], 50.0)
            self.assertEqual(summary["aggregates"]["latency_p95_ms"]["median"], 20.0)
            self.assertEqual(summary["error_totals"], {})

    def test_rejects_mixed_concurrency(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_run(root, 1, 100.0, 20.0, concurrency=50)
            write_run(root, 2, 100.0, 20.0, concurrency=100)

            with self.assertRaisesRegex(ValueError, "different concurrency"):
                build_summary(discover_runs(root))


if __name__ == "__main__":
    unittest.main()
