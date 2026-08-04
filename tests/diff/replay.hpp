#ifndef ELYSIUMKV_TESTS_DIFF_REPLAY_HPP
#define ELYSIUMKV_TESTS_DIFF_REPLAY_HPP

#include "diff/op_stream.hpp"
#include "elysiumkv/options.hpp"

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace elysiumkv::test {

/// ARCHITECTURE.md "The differential oracle" — the suite runs under every configuration axis. A case that only runs
/// in one configuration is not a contract.
struct ReplayConfig {
    std::string name = "Default";
    Compression compression = Compression::None;
    bool split_stores = false;
    size_t memtable_bytes = 1u << 20;
    /// Levels 0 and 1 on a Transient store over a Durable bottom (ARCHITECTURE.md "A tier is not a level"). The
    /// oracle is unchanged by placement: a transient band is a durability
    /// decision, not a semantic one.
    bool transient_band = false;
    /// ARCHITECTURE.md "Caches chain" — wraps every tier's store in the decorator chain: memory over disk over
    /// the local store. The oracle is unchanged by it, which is the claim: a cache is
    /// invisible to the engine, so **every case in this suite must produce identical
    /// results with one in the chain**. The layers are deliberately small so eviction
    /// happens constantly rather than at the end of a long run.
    ///
    /// The chain outlives the close-and-reopen the replay performs, so the reopen
    /// reads through a *warm* cache — which is where an entry that should have been
    /// invalidated would surface.
    bool cached = false;
    /// ARCHITECTURE.md "A process-wide memory budget" — a shared `MemoryBudget` of this size, or none when zero.
    ///
    /// **Shedding forces a flush at an arbitrary point in the op stream**, which is
    /// exactly the perturbation this oracle is good at invalidating: the frozen-memtable
    /// handoff now happens for a reason unrelated to the memtable being full. It is also
    /// new write-path code that no other suite reaches.
    size_t budget_bytes = 0;
    /// ARCHITECTURE.md "The differential oracle" — **the differential suite runs in both modes.** Synchronous is the
    /// gating, reproducible, shrinkable pass: it tests the logic. Threaded is a
    /// nightly randomized pass sampling interleavings, alongside TSan and the
    /// soak: it tests the scheduling, which is what ships. Neither covers the
    /// other.
    ///
    /// Shrinking is only meaningful in the synchronous mode — delta-debugging
    /// cannot tell whether dropping a span fixed the bug or merely got lucky.
    bool threaded = false;
};

struct DiffFailure {
    size_t op_index = 0;
    std::string message;
};

/// Replays an op list against a fresh store and the `std::map` oracle, returning
/// the first mismatch. Deliberately free of gtest assertions: the shrinker calls
/// this hundreds of times, and it must be a pure function of (ops, config).
///
/// Runs in `BackgroundMode::Inline`, so the op list — and nothing else —
/// determines the execution.
std::optional<DiffFailure> replay(const std::vector<DiffOp>& ops, const ReplayConfig& config);

/// ARCHITECTURE.md "The differential oracle" — **shrinking is mandatory, and is the highest-leverage part of the
/// harness.** A mismatch at operation 743,291 is close to undebuggable; the same
/// failure minimized to five operations is readable. Repeatedly drops spans and
/// re-runs, keeping any reduction that still fails.
///
/// `max_replays` bounds the work; the result is minimal with respect to the
/// spans tried, not globally minimal.
std::vector<DiffOp> shrink(std::vector<DiffOp> ops,
                           const std::function<bool(const std::vector<DiffOp>&)>& still_fails,
                           int max_replays = 3000);

/// The usual case: "still fails" means "still mismatches the oracle".
std::vector<DiffOp> shrink(std::vector<DiffOp> ops, const ReplayConfig& config,
                           int max_replays = 3000);

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_DIFF_REPLAY_HPP
