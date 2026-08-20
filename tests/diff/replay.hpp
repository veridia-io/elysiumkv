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
    /// invisible to the engine, so every case in this suite must produce identical
    /// results with one in the chain. The layers are deliberately small so eviction
    /// happens constantly rather than at the end of a long run.
    ///
    /// The chain outlives the close-and-reopen the replay performs, so the reopen
    /// reads through a *warm* cache — which is where an entry that should have been
    /// invalidated would surface.
    bool cached = false;
    /// Rounds every cache miss out to a chunk of this size. Only meaningful with `cached`.
    ///
    /// A cache that reads more than it was asked for must still answer exactly what it was
    /// asked for. The chunk runs past the requested range at both ends and past the end of the
    /// object at the last one, so an off-by-one in the slicing shows up as a wrong value or a
    /// short read — which the oracle catches and a request-count test cannot.
    size_t cache_fetch_granularity = 0;
    /// Compacts a file once this fraction of its entries are tombstones; zero leaves it off.
    ///
    /// Changes when compaction happens, and nothing else. Every answer must be identical to
    /// the same stream without it — a trigger that fired on the wrong file, or dropped a live key
    /// while reclaiming a tombstone, is a difference the oracle sees and a picker unit test does
    /// not, because the unit test never replays the compaction it asked for.
    /// `Options::age_jitter`. Changes when a file crosses to a colder tier, and nothing else —
    /// every answer must be identical to the same stream without it, which is the property. Only
    /// meaningful alongside a tier that bounds age.
    double jitter = 0.0;
    /// `Options::max_compaction_bytes`. Zero leaves the engine default, which no replay ever
    /// reaches. Changes which files a compaction takes, and nothing about the answers — the
    /// budget trims an overlapping level's input set oldest-first, so every file left behind is
    /// newer than the output. Get that direction wrong and reads return stale values, which is
    /// exactly what the oracle sees.
    size_t max_compaction_bytes = 0;
    /// `Tier::max_bytes` on the hot tier of a `split_stores` configuration; zero leaves it
    /// unbounded. The deterministic way to drive migration, and the only one that works here:
    /// age-driven placement puts a compaction output on the tier its *oldest* write belongs to at
    /// the moment it is written, so with a bound short relative to the run every output is born
    /// cold and the migrator never has anything to move. A byte cap does not depend on the clock
    /// at all, which also keeps the replay reproducible.
    size_t tier0_max_bytes = 0;
    /// `Tier::max_age` on the hot tier, in milliseconds; zero leaves the 50 ms default. Set it far
    /// past the run's duration to take age out of the placement decision, so `tier0_max_bytes` is
    /// the only thing moving files.
    uint64_t tier_max_age_ms = 0;
    /// Distinct keys the op stream draws from; zero leaves the generator's default. A narrow
    /// keyspace makes every operation land on top of another, which is what puts a *stale* value
    /// under a newer one — without that there is nothing for a mis-ordered compaction to uncover.
    int distinct_keys = 0;
    double tombstone_density_trigger = 0.0;
    /// ARCHITECTURE.md "A process-wide memory budget" — a shared `MemoryBudget` of this size, or none when zero.
    ///
    /// Shedding forces a flush at an arbitrary point in the op stream, which is
    /// exactly the perturbation this oracle is good at invalidating: the frozen-memtable
    /// handoff now happens for a reason unrelated to the memtable being full. It is also
    /// new write-path code that no other suite reaches.
    size_t budget_bytes = 0;
    /// ARCHITECTURE.md "The differential oracle" — the differential suite runs in both modes. Synchronous is the
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

/// ARCHITECTURE.md "The differential oracle" — shrinking is mandatory, and is the highest-leverage part of the
/// harness. A mismatch at operation 743,291 is close to undebuggable; the same
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
