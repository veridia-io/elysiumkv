#ifndef ELYSIUMKV_STATS_HPP
#define ELYSIUMKV_STATS_HPP

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace elysiumkv {

using Duration = std::chrono::milliseconds;

/// ARCHITECTURE.md "Statistics are a buffer, not a struct" — **never a derived global where per-level facts will do.**
struct LevelStats {
    int level = 0;
    int file_count = 0;
    uint64_t bytes = 0;
    /// Zero when the level is empty. On a `Transient` level this is how far back
    /// a loss of that store would reach; on a `Durable` one it is compaction lag,
    /// which is what you want when diagnosing why a level is over its score.
    Duration oldest_file_age{0};
    bool age_triggered = false;  ///< oldest file is past max_age
    bool stalling = false;       ///< oldest file is past stall_age

    /// ARCHITECTURE.md "Statistics are a buffer, not a struct" — files not yet rewritten under the level's current compression.
    /// Reaches zero on its own as compaction sweeps the keyspace, **except in
    /// key ranges that receive no writes** (ARCHITECTURE.md "Inside an SST"): a dormant range never
    /// triggers a compaction into that part of the level, so its files can sit
    /// indefinitely. `compact_level()` forces completion, and this is how an
    /// operator knows it finished.
    ///
    /// There is no `files_stale_store` counterpart: placement is no longer
    /// derived from the level, so the question does not arise.
    int files_stale_codec = 0;
};

/// ARCHITECTURE.md "Statistics are a buffer, not a struct" — the storage axis. Tier and level are independent (ARCHITECTURE.md "A tier is not a level"), so a level's
/// files are scattered across tiers and a tier's files across levels.
struct TierStats {
    int tier = 0;
    int file_count = 0;
    uint64_t bytes = 0;
    /// Zero when the tier is empty. On a `Transient` tier this is how far back a
    /// loss of that store would reach — the number an embedder alarms on.
    Duration oldest_file_age{0};
    /// Files whose placement no longer matches this tier: past `max_age`, or the
    /// tier is over `max_bytes`.
    int files_pending_migration = 0;
    /// `Transient` only: past `stall_age`, so writes are being held.
    bool stalling = false;
};

/// ARCHITECTURE.md "Statistics are a buffer, not a struct". There is deliberately **no single derived horizon metric**: it would be
/// a global computed from per-level facts that are more useful raw. An embedder
/// tracking exposure reads `oldest_file_age` for the levels it placed on a
/// transient store — it chose them, so it knows which.
struct Stats {
    std::vector<LevelStats> levels;
    std::vector<TierStats> tiers;

    /// ARCHITECTURE.md "A tier is not a level" — true from a discard at open until `mark_recovery_complete()`.
    /// After a discard the store is *wrong*, not merely incomplete; the engine
    /// reports it and does not enforce read blocking.
    bool requires_recovery = false;

    size_t memtable_bytes = 0;
    /// Age of the oldest write in the memtable; zero when it holds nothing.
    Duration memtable_age{0};

    uint64_t compactions = 0;
    uint64_t compaction_bytes_read = 0;
    uint64_t compaction_bytes_written = 0;
    /// ARCHITECTURE.md "Migration between tiers" — migration moves bytes without interpreting them, so its cost is
    /// exactly the bytes moved, unlike compaction's write amplification.
    uint64_t migrations = 0;
    uint64_t migration_bytes = 0;

    Duration stalled_total{0};
    uint64_t stall_count = 0;

    uint64_t block_cache_hits = 0;
    uint64_t block_cache_misses = 0;
    size_t block_cache_bytes = 0;

    /// The open-`SstReader` cache: index blocks and bloom filters. Reported because
    /// this was the one cache in the engine with neither a bound nor a number, which
    /// is the combination that makes a memory problem invisible until it is a
    /// production incident. A rising miss count against a steady byte count means
    /// `Options::reader_cache_bytes` is too small for the working set — and each miss
    /// is three reads to reopen the file.
    uint64_t reader_cache_hits = 0;
    uint64_t reader_cache_misses = 0;
    size_t reader_cache_bytes = 0;
    uint64_t open_readers = 0;

    /// ARCHITECTURE.md "A process-wide memory budget" — the shared budget, so an embedder can see whether its instances
    /// collectively fit. `used` may exceed `total`: a memtable arena charges
    /// unconditionally for a write already accepted, and the overage is precisely the
    /// signal the write path sheds on. `budget_sheds` counts how often it has.
    size_t memory_budget_used = 0;
    size_t memory_budget_total = 0;
    uint64_t budget_sheds = 0;

    /// ARCHITECTURE.md "The ABI boundary" — nonzero at close is a leak. A leaked pin holds a block-cache entry
    /// forever, so this is a first-class invariant rather than a diagnostic.
    uint64_t pins_outstanding = 0;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_STATS_HPP
