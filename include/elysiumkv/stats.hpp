#ifndef ELYSIUMKV_STATS_HPP
#define ELYSIUMKV_STATS_HPP

#include "elysiumkv/io_counters.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
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

    /// **Records, not keys**, and the distinction is the whole contract. Every superseded version of
    /// a key is a record until compaction merges it, and every tombstone is a record until a
    /// compaction whose output is bottommost drops it. So a sum over levels is an **upper bound on
    /// the number of distinct live keys** — provable rather than typical, and equal once everything
    /// has merged into the bottommost level.
    ///
    /// Exact per file: the SST builder counts as it appends. The approximation is entirely in the
    /// cross-file dimension, and nothing here is sampled.
    ///
    /// Reported per level rather than as one total because **the accuracy depends on the
    /// distribution**: a million records in the bottommost level is close to a million keys, and a
    /// million spread across L0 after an update storm is not.
    uint64_t entries = 0;
    /// How many of `entries` are deletes.
    ///
    /// **`entries - tombstones` is the tighter upper bound, and it is what a total should use.**
    /// A live key's newest record is always a put, never a tombstone, so tombstones are disjoint
    /// from the records representing live keys: `records >= live + tombstones`, hence
    /// `records - tombstones >= live`. Both quantities converge on the true count once compaction
    /// has merged everything into the bottommost level and dropped its tombstones — the subtraction
    /// simply gets there sooner.
    uint64_t tombstones = 0;
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
    /// This tier's authoritative store's traffic, which against object storage is the bill. A
    /// cache in front of the tier is not counted here; its effect is its hit rate.
    ///
    /// **Two tiers naming one store report the same figures** — they are the store's, not the
    /// tier's, so summing across tiers double-counts.
    IoCounters io;

    /// `Transient` only: the oldest file here is past `stall_age`.
    ///
    /// **The condition as observed at this instant, which is a moment ahead of the valve.** The
    /// maintenance coordinator owns the decision the write path acts on and publishes it on its
    /// tick, so this can read true up to one `maintenance_interval` before writes are actually
    /// held. That is the right direction for an alarm and the wrong one for control flow, which is
    /// why the write path reads the published flag and not this.
    ///
    /// Read it as: from here on, the log is expiring while the durable position stops advancing.
    /// **Recovery capability is what degrades, on a deadline** — the action is to extend log
    /// retention, and `durable_watermark` against the log's earliest offset is the margin.
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
    /// Records in the live and frozen memtables together. Same meaning as `LevelStats::entries`:
    /// puts and deletes alike. The memtable deduplicates on insert, so an overwrite does not add one.
    uint64_t memtable_entries = 0;
    /// How many of those are deletes.
    uint64_t memtable_tombstones = 0;

    /// Memtable rotations that became an L0 file. Beside `compactions` because it is the *cause*
    /// of most of them: `Options::flush_interval` set too short produces many small L0 files and
    /// therefore more compaction, and flush rate is the first place that shows up. It is also the
    /// only way to confirm the interval fires at all on a quiet partition — `memtable_age` is a
    /// gauge read at scrape time, so a flush between two scrapes leaves no trace in it, and a
    /// counter cannot be derived from a gauge.
    uint64_t flushes = 0;

    uint64_t compactions = 0;
    uint64_t compaction_bytes_read = 0;
    uint64_t compaction_bytes_written = 0;
    /// ARCHITECTURE.md "Migration between tiers" — migration moves bytes without interpreting them, so its cost is
    /// exactly the bytes moved, unlike compaction's write amplification.
    uint64_t migrations = 0;
    uint64_t migration_bytes = 0;

    Duration stalled_total{0};
    uint64_t stall_count = 0;

    /// Background operations that failed — a flush or a compaction. **Counted even when the engine
    /// retries and succeeds**, which is the case that otherwise leaves no trace: a store working
    /// its way through a degraded object store looks identical to a healthy one.
    uint64_t background_failures = 0;

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

    /// The **live** watermark frontier: the position up to which the store's state would survive
    /// losing every transient tier. Deliberately *not* the maximum watermark over current files,
    /// which is tier-blind and would advance on a flush to transient storage — a flush that
    /// changes nothing an operator can rely on.
    ///
    ///     durable_watermark = min(low) over files currently on a transient tier,
    ///                         or max(high) when no transient files remain
    ///
    /// the same expression recovery uses, evaluated live instead of at open. Distinct from
    /// `DB::recovered_watermark()`, which is fixed at open and must not be confused with this:
    /// sharing a name would silently change the getter's meaning after the first write.
    ///
    /// **This is the numerator of the only margin an operator can act on.** When migration is
    /// failing, this value stops advancing while the changelog keeps expiring, and the distance
    /// between the log's earliest retained offset and this value is how much recovery capability
    /// is left. Without it that distance is not computable.
    ///
    /// Absent — not zero — when no watermark has been set. Zero is a valid position, so an
    /// exporter must omit the series rather than publish zero.
    ///
    /// A precision caveat for exporters: many metrics systems carry gauge samples as IEEE-754
    /// doubles, exact only below 2^53. The metric is observational, for the retention margin and
    /// for alerting; **a restore must use the exact value** from `DB::recovered_watermark()`,
    /// never one that has been through a metrics pipeline.
    std::optional<uint64_t> durable_watermark;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_STATS_HPP
