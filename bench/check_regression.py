#!/usr/bin/env python3
"""Compare deterministic benchmark counters against a stored baseline.

ARCHITECTURE.md "Benchmarks" — benchmarks are gating, not reported. The differential oracle validates
answers, not cost — nothing in ARCHITECTURE.md "The differential oracle" distinguishes a prefix scan served by SST
range pruning from one that walks the whole keyspace. Both are correct, both pass
a million seeded operations, and one is ten times slow.

Wall-clock measurements remain in the benchmark report but are never gated. The
baseline contains only work-per-operation counters, which are independent of the
machine running them.
"""

import argparse
import json
import os
import platform
import sys

# The counters `bench_main.cpp` labels "The gate": round trips per lookup, readers opened per lookup,
# round trips per scan, and bytes compaction rewrote per byte written. They are properties of the
# read path and picker rather than measurements of the machine.
STRUCTURAL = ("gets_per_op", "opens_per_op", "gets_per_scan", "write_amp")


def baseline_path(directory: str) -> str:
    name = f"{platform.system().lower()}-{platform.machine().lower()}.json"
    return os.path.join(directory, name)


def structural(report: dict) -> dict:
    """Map "name::counter" to value for the counters in STRUCTURAL.

    A median aggregate wins over the raw runs it summarises.
    """
    out = {}
    for entry in report.get("benchmarks", []):
        if entry.get("run_type") == "aggregate" and entry.get("aggregate_name") != "median":
            continue
        name = entry["name"].removesuffix("_median")
        for counter in STRUCTURAL:
            if counter not in entry:
                continue
            key = f"{name}::{counter}"
            if key in out and entry.get("run_type") != "aggregate":
                continue
            out[key] = float(entry[counter])
    return out


def relative_change(reference: float, measured: float) -> float:
    """Signed change, defined when the reference is zero.

    A counter legitimately sits at zero — a lookup that opens no reader — and a ratio against it is
    not a small number, it is a new cost appearing where there was none.
    """
    if reference == 0:
        return 0.0 if measured == 0 else float("inf")
    return (measured - reference) / reference


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", help="google-benchmark --benchmark_format=json output")
    parser.add_argument("--baselines", default=os.path.join(os.path.dirname(__file__), "baselines"))
    parser.add_argument("--update", action="store_true",
                        help="write the structural counters as the new baseline")
    args = parser.parse_args()

    with open(args.results) as f:
        report = json.load(f)
    current_counters = structural(report)
    present = {key.rsplit("::", 1)[1] for key in current_counters}
    missing_types = sorted(set(STRUCTURAL) - present)
    if missing_types:
        print(f"benchmark report lacks structural counters: {', '.join(missing_types)}",
              file=sys.stderr)
        return 1

    path = baseline_path(args.baselines)
    os.makedirs(args.baselines, exist_ok=True)

    if args.update or not os.path.exists(path):
        with open(path, "w") as f:
            json.dump(current_counters, f, indent=2, sort_keys=True)
            f.write("\n")
        action = "updated" if args.update else "bootstrapped"
        print(f"{action} baseline {path} with {len(current_counters)} structural counters")
        return 0

    with open(path) as f:
        baseline = json.load(f)

    regressions = []
    missing = []
    for key, value in sorted(current_counters.items()):
        if key not in baseline:
            missing.append(key)
            continue
        reference = baseline[key]
        if value > reference:
            regressions.append((key, reference, value, relative_change(reference, value)))

    for name in missing:
        print(f"NEW       {name}: no baseline yet — rerun with --update")
    for key, reference, value, change in regressions:
        print(f"STRUCTURE {key}: {reference:.4g} -> {value:.4g} ({change:+.1%}) "
              f"— the work done per operation changed, not the speed of it", file=sys.stderr)

    if regressions:
        print(f"{len(regressions)} structural counter(s) increased.", file=sys.stderr)
        return 1

    judged = len(current_counters) - len(missing)
    summary = f"{judged} structural counter(s) did not increase"
    if missing:
        summary += f", {len(missing)} new"
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
