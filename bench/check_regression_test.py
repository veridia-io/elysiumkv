#!/usr/bin/env python3

import json
import os
import platform
import subprocess
import sys
import tempfile
import unittest


CHECKER = os.path.join(os.path.dirname(__file__), "check_regression.py")


class RegressionCheckerTest(unittest.TestCase):
    counters = {
        "gets_per_op": 1.0,
        "opens_per_op": 0.0,
        "gets_per_scan": 2.0,
        "write_amp": 3.0,
    }

    def run_checker(self, baseline_entry, measured_entry):
        with tempfile.TemporaryDirectory() as work:
            baselines = os.path.join(work, "baselines")
            os.mkdir(baselines)
            baseline_name = f"{platform.system().lower()}-{platform.machine().lower()}.json"
            with open(os.path.join(baselines, baseline_name), "w", encoding="utf-8") as stream:
                json.dump(baseline_entry, stream)

            results = os.path.join(work, "results.json")
            with open(results, "w", encoding="utf-8") as stream:
                json.dump({"benchmarks": [measured_entry]}, stream)

            return subprocess.run(
                [sys.executable, CHECKER, results, "--baselines", baselines],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_any_structural_increase_fails(self):
        baseline = {f"BM_Read::{key}": value for key, value in self.counters.items()}
        measured = {"name": "BM_Read", "real_time": 100.0, **self.counters}
        measured["gets_per_op"] = 1.001
        result = self.run_checker(
            baseline,
            measured,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("STRUCTURE BM_Read::gets_per_op", result.stderr)

    def test_wall_clock_slowdown_is_not_a_gate(self):
        baseline = {"BM_Read": 100.0}
        baseline.update({f"BM_Read::{key}": value for key, value in self.counters.items()})
        result = self.run_checker(
            baseline,
            {"name": "BM_Read", "real_time": 1000.0, **self.counters},
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_doubled_round_trip_names_the_counter(self):
        baseline = {f"BM_Read::{key}": value for key, value in self.counters.items()}
        measured = {"name": "BM_Read", "real_time": 100.0, **self.counters}
        measured["gets_per_op"] = 2.0

        result = self.run_checker(baseline, measured)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("STRUCTURE BM_Read::gets_per_op", result.stderr)

    def test_a_report_without_every_gate_counter_fails(self):
        result = self.run_checker(
            {"BM_Read::gets_per_op": 1.0},
            {"name": "BM_Read", "real_time": 100.0, "gets_per_op": 1.0},
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("benchmark report lacks structural counters", result.stderr)


if __name__ == "__main__":
    unittest.main()
