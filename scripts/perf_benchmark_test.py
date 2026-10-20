#!/usr/bin/env python3
"""Regression checks for the fixed-hardware benchmark harness."""

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PerfBenchmarkHarnessTest(unittest.TestCase):
    def test_archive_directory_is_writable_in_runtime_image(self) -> None:
        benchmark = (ROOT / "scripts/perf-benchmark.sh").read_text()
        dockerfile = (ROOT / "Dockerfile").read_text()

        archive_dir = "/var/lib/taskflow/dlq-archive"
        self.assertIn(f"QUEUE_DLQ_ARCHIVE_DIR={archive_dir}", benchmark)
        self.assertIn(archive_dir, dockerfile)
        self.assertIn("chown -R app:app /var/lib/taskflow", dockerfile)

    def test_repeated_benchmark_defaults_to_no_access_log(self) -> None:
        benchmark = (ROOT / "scripts/perf-benchmark.sh").read_text()
        multi = (ROOT / "scripts/perf-multi.sh").read_text()

        expected = 'ACCESS_LOG="${ACCESS_LOG:-false}"'
        self.assertIn(expected, benchmark)
        self.assertIn(expected, multi)
        self.assertIn('ACCESS_LOG="$ACCESS_LOG"', multi)


if __name__ == "__main__":
    unittest.main()
