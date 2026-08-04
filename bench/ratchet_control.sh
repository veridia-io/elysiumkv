#!/bin/sh
# ARCHITECTURE.md "Negative controls" — the negative control for the benchmark ratchet.
#
# Records a baseline with the synthetic benchmark running fast, then runs it slow
# and asserts check_regression.py fails *naming that benchmark*. Asserting only a
# non-zero exit would pass if the script died for an unrelated reason — a missing
# baseline directory, a filter matching nothing — which is the degradation this
# control exists to detect.
#
# The engine is never slowed: the subject under test is the ratchet.
set -eu
BENCH="$1"
CHECKER="$2"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Checks its own precondition. Without this, a benchmark binary that failed to
# build or matched no filter leaves an empty file, the checker dies on the JSON,
# and the control reports a failure that has nothing to do with the ratchet —
# which is the vacuous-control problem, in the control itself.
run() {
    ELYSIUMKV_RATCHET_SPIN_NS="$1" "$BENCH" --benchmark_filter='BM_RatchetControl' \
        --benchmark_min_time=0.05s --benchmark_format=json --benchmark_out="$2" > /dev/null 2>&1
    if ! grep -q BM_RatchetControl "$2" 2>/dev/null; then
        echo "the benchmark produced no BM_RatchetControl result at spin=$1;" >&2
        echo "this control cannot say anything about the ratchet." >&2
        exit 1
    fi
}

run 1000 "$WORK/fast.json"
python3 "$CHECKER" "$WORK/fast.json" --baselines "$WORK/baselines" > /dev/null

run 20000 "$WORK/slow.json"
if output="$(python3 "$CHECKER" "$WORK/slow.json" --baselines "$WORK/baselines" 2>&1)"; then
    echo "the ratchet accepted a 20x slowdown. Every green it has produced means" >&2
    echo "only that it ran, not that nothing regressed." >&2
    echo "$output" >&2
    exit 1
fi

case "$output" in
    *BM_RatchetControl*) ;;
    *) echo "the ratchet failed, but never named BM_RatchetControl — it failed for" >&2
       echo "some other reason, so this control proves nothing about the threshold." >&2
       echo "--- what it said ---" >&2; echo "$output" >&2; exit 1 ;;
esac

echo "ratchet tripped on a 20x slowdown, naming BM_RatchetControl"
