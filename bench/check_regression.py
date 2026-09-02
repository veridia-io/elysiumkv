#!/usr/bin/env python3
"""Compare Google Benchmark output against a committed baseline.

ARCHITECTURE.md "Benchmarks" — benchmarks are gating, not reported. The differential oracle validates
answers, not cost — nothing in ARCHITECTURE.md "The differential oracle" distinguishes a prefix scan served by SST
range pruning from one that walks the whole keyspace. Both are correct, both pass
a million seeded operations, and one is ten times slow.

Baselines are per *machine*, not per platform, and are never committed: 73ns on
one arm64 machine is not a 73ns threshold on another arm64 machine, and treating
it as one turns a hardware difference into a red build. A missing baseline is
written rather than failed, so every machine bootstraps its own on first run; an
improvement ratchets it down with --update.

The file name is keyed on system+architecture only because that is enough to keep
one machine's runs apart from each other across a dual-boot or a container. CI
keeps its baseline in a cache outside the working tree, so the runners compare
against their own previous numbers rather than against a developer's laptop.
"""

import argparse
import json
import os
import platform
import sys

THRESHOLD = 0.10  # fail on a >10% regression

# The counters `bench_main.cpp` labels "The gate": round trips per lookup, readers opened per lookup,
# round trips per scan, and bytes compaction rewrote per byte written. They are properties of the
# read path and the picker rather than of the machine, so they are the half of this file that ought
# to be comparable at all — and until they were compared here, a change doubling the round trips per
# lookup passed whenever wall time happened to hold.
STRUCTURAL = ("gets_per_op", "opens_per_op", "gets_per_scan", "write_amp")

# Tight, but not zero. These are ratios over however many iterations google-benchmark chose, and a
# faster machine amortises the fixed cost of opening readers over more of them, so the quotient moves
# a little without the structure moving at all. Five times tighter than the wall-clock band, which is
# enough to catch a round trip appearing and nowhere near loose enough to hide one.
COUNTER_THRESHOLD = 0.02

# How noisy a measurement may be before a comparison against it means nothing.
#
# **A ratchet that cannot tell a regression from a bad afternoon on a shared runner is worse than no
# ratchet**: it fails builds that changed nothing, and the response to a flaky gate is to stop
# reading it. Google-benchmark reports the coefficient of variation across repetitions, so the run
# says how well it measured itself. Above this the benchmark is reported and skipped rather than
# judged — a third of the threshold, so the noise band cannot swallow the signal it is guarding.
MAX_CV = THRESHOLD / 3


def baseline_path(directory: str) -> str:
    name = f"{platform.system().lower()}-{platform.machine().lower()}.json"
    return os.path.join(directory, name)


def measurements(report: dict) -> dict:
    """name -> nanoseconds, preferring aggregated medians when present."""
    out = {}
    for entry in report.get("benchmarks", []):
        if entry.get("run_type") == "aggregate" and entry.get("aggregate_name") != "median":
            continue
        name = entry["name"].removesuffix("_median")
        # A median aggregate wins over the raw runs it summarises.
        if name in out and entry.get("run_type") != "aggregate":
            continue
        out[name] = entry["real_time"]
    return out


def structural(report: dict) -> dict:
    """"name::counter" -> value, for the counters in STRUCTURAL.

    Keyed into the same flat baseline as the times, so a baseline recorded before these were
    compared still loads: its counter keys are simply absent and report as new.
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
    if abs(reference) < 1e-12:
        return 0.0 if abs(measured) < 1e-12 else float("inf")
    return (measured - reference) / reference


def dispersion(report: dict) -> dict:
    """name -> coefficient of variation across repetitions, where the run reported one.

    Absent for a single-repetition run, which is why a missing entry is treated as measured rather
    than as noisy: an old report should still be comparable, and the flags that produce one are the
    caller's choice.
    """
    out = {}
    for entry in report.get("benchmarks", []):
        if entry.get("aggregate_name") != "cv":
            continue
        out[entry["name"].removesuffix("_cv")] = entry["real_time"]
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", help="google-benchmark --benchmark_format=json output")
    parser.add_argument("--baselines", default=os.path.join(os.path.dirname(__file__), "baselines"))
    parser.add_argument("--update", action="store_true",
                        help="write the measured times as the new baseline")
    args = parser.parse_args()

    with open(args.results) as f:
        report = json.load(f)
    current = measurements(report)
    if not current:
        print("no benchmark results found", file=sys.stderr)
        return 1
    current_counters = structural(report)

    path = baseline_path(args.baselines)
    os.makedirs(args.baselines, exist_ok=True)

    if args.update or not os.path.exists(path):
        with open(path, "w") as f:
            json.dump({**current, **current_counters}, f, indent=2, sort_keys=True)
            f.write("\n")
        action = "updated" if args.update else "bootstrapped"
        print(f"{action} baseline {path} with {len(current)} benchmarks")
        return 0

    with open(path) as f:
        baseline = json.load(f)

    spread = dispersion(report)
    regressions, improvements, missing, noisy = [], [], [], []
    for name, time_ns in sorted(current.items()):
        if name not in baseline:
            missing.append(name)
            continue
        reference = baseline[name]
        change = (time_ns - reference) / reference
        cv = spread.get(name)
        if cv is not None and cv > MAX_CV:
            # Reported either way, because a benchmark that has become *unstable* is itself worth
            # knowing about — it is how a new source of variance announces itself.
            noisy.append((name, cv, change))
            continue
        if change > THRESHOLD:
            regressions.append((name, reference, time_ns, change))
        elif change < -THRESHOLD:
            improvements.append((name, reference, time_ns, change))

    # No dispersion escape here, deliberately. A counter that has become noisy has itself changed —
    # the structure it measures is not supposed to vary between repetitions — so declining to judge
    # it would be declining to report the finding.
    counter_regressions = []
    for key, value in sorted(current_counters.items()):
        if key not in baseline:
            missing.append(key)
            continue
        reference = baseline[key]
        change = relative_change(reference, value)
        if change > COUNTER_THRESHOLD:
            counter_regressions.append((key, reference, value, change))

    for name, reference, time_ns, change in improvements:
        print(f"IMPROVED  {name}: {reference:.1f}ns -> {time_ns:.1f}ns ({change:+.1%}) "
              f"— rerun with --update to ratchet the baseline down")
    for name in missing:
        print(f"NEW       {name}: no baseline yet — rerun with --update")
    for name, cv, change in noisy:
        print(f"UNSTABLE  {name}: cv {cv:.1%} exceeds {MAX_CV:.1%}, so the {change:+.1%} change "
              f"cannot be told from noise — not judged")
    for name, reference, time_ns, change in regressions:
        print(f"REGRESSED {name}: {reference:.1f}ns -> {time_ns:.1f}ns ({change:+.1%})",
              file=sys.stderr)
    for key, reference, value, change in counter_regressions:
        print(f"STRUCTURE {key}: {reference:.4g} -> {value:.4g} ({change:+.1%}) "
              f"— the work done per operation changed, not the speed of it", file=sys.stderr)

    if regressions or counter_regressions:
        if regressions:
            print(f"\n{len(regressions)} benchmark(s) regressed by more than "
                  f"{THRESHOLD:.0%}.", file=sys.stderr)
        if counter_regressions:
            print(f"{len(counter_regressions)} structural counter(s) rose by more than "
                  f"{COUNTER_THRESHOLD:.0%}.", file=sys.stderr)
        return 1

    judged = len(current) - len(noisy) - len(missing)
    summary = f"{judged} benchmark(s) within {THRESHOLD:.0%} of baseline"
    if noisy:
        summary += f", {len(noisy)} too noisy to judge"
    if missing:
        summary += f", {len(missing)} new"
    counters_judged = len(current_counters) - sum(1 for k in current_counters if k not in baseline)
    if counters_judged:
        summary += f"; {counters_judged} structural counter(s) within {COUNTER_THRESHOLD:.0%}"
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
