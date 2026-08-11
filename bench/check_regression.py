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

    path = baseline_path(args.baselines)
    os.makedirs(args.baselines, exist_ok=True)

    if args.update or not os.path.exists(path):
        with open(path, "w") as f:
            json.dump(current, f, indent=2, sort_keys=True)
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

    if regressions:
        print(f"\n{len(regressions)} benchmark(s) regressed by more than "
              f"{THRESHOLD:.0%}.", file=sys.stderr)
        return 1

    judged = len(current) - len(noisy) - len(missing)
    summary = f"{judged} benchmark(s) within {THRESHOLD:.0%} of baseline"
    if noisy:
        summary += f", {len(noisy)} too noisy to judge"
    if missing:
        summary += f", {len(missing)} new"
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
