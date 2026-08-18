#ifndef ELYSIUMKV_OPTIONS_HPP
#define ELYSIUMKV_OPTIONS_HPP

#include "elysiumkv/blob_store.hpp"
#include "elysiumkv/manifest_catalog.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>

namespace elysiumkv {

class BlockCache;
class MemoryBudget;

/// ARCHITECTURE.md "Inside an SST" — **per level, not per tier.** A tier-scoped codec would force migration
/// (ARCHITECTURE.md "Migration between tiers") to decompress and recompress every file it moves, turning a byte copy
/// into a full rewrite.
enum class Compression : uint8_t {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
};

inline bool is_known_compression(uint8_t raw) {
    return raw <= static_cast<uint8_t>(Compression::Zstd);
}

/// ARCHITECTURE.md "A tier is not a level" — **which recovery path a loss of this tier's store triggers.** That is
/// the whole meaning of the enum; it is not a rating of the storage medium.
enum class Durability : uint8_t {
    /// Loss is not expected and is treated as corruption: open aborts and the
    /// embedder performs a full rebuild. ElysiumKV cannot verify the assertion.
    Durable,
    /// Loss is expected and routine. Every file on that store is dropped and the
    /// embedder restores them by its own means. Bounded by the tier's `max_age`,
    /// which actively migrates data off the tier on a timer — so this is only
    /// correct for storage that may genuinely vanish.
    Transient,
};

using Duration = std::chrono::milliseconds;

/// ARCHITECTURE.md "The differential oracle" — how background work is scheduled.
enum class BackgroundMode : uint8_t {
    /// Production: a flush thread and a background thread driving migration and
    /// compaction.
    Threaded,
    /// Testing: background work is performed inline when the driver calls for
    /// it, so the op stream fully determines the execution. **Determinism is a
    /// build requirement of the harness, not a property it inherits** — and
    /// without it, shrinking is impossible.
    Inline,
};

/// ARCHITECTURE.md "A tier is not a level" — **where files live.** Tier and level are independent axes: level is
/// LSM structure, tier is storage. A file lives in exactly one store, chosen per
/// file by its **age**, so a single level routinely spans several tiers at
/// once — recent files on fast storage, older ones on cheap storage.
struct Tier {
    std::shared_ptr<BlobStore> store;  ///< may itself be a cache chain
    Durability durability = Durability::Durable;

    std::optional<Duration> max_age;        ///< files older than this move to the next tier

    /// Tier capacity; oldest files evicted first.
    ///
    /// **Not to be confused with a per-*file* size bound**, which this type used to have and no
    /// longer does. That gave size a second, independent route to a colder tier, and placement has
    /// to be a monotone function of age alone: a file's age only ever grows, so its tier only ever
    /// descends, whereas a size bound could place a *new* file cold on the day it was written. If
    /// the intent is to cap what a tier holds, this is the field — it evicts oldest-first, which is
    /// the mechanism that was actually wanted. If the intent is to keep large files off a small fast
    /// tier, lower that level's `target_file_bytes` so large files are not produced.
    std::optional<size_t> max_bytes;
    std::optional<Duration> stall_age;      ///< Transient only; default 2 * max_age
};

/// ARCHITECTURE.md "Compaction" — LSM structure, and nothing about storage.
struct LevelOptions {
    Compression compression = Compression::None;

    // Capacity. Score is the max of whichever ratios are set.
    std::optional<size_t> max_bytes;  ///< deeper levels
    std::optional<int> max_files;     ///< L0-style levels
    std::optional<int> slowdown_at;   ///< files; throttle writes
    std::optional<int> stop_at;       ///< files; block writes

    size_t target_file_bytes = 16ull << 20;

    /// The classic layout, for embedders who want one. Capacities are otherwise
    /// explicit: there is no base size and no multiplier, because those are a
    /// formula for producing this map and stating it directly is clearer.
    ///
    /// Choose `count` against expected total size: more levels means lower write
    /// amplification, and extra configured levels sitting empty cost nothing.
    static std::map<int, LevelOptions> geometric(size_t base, int multiplier, int count);
};

/// Ordered: a sink receives everything at or above `Options::min_log_level`.
enum class LogLevel : int { Debug = 0, Info = 1, Warn = 2, Error = 3, Off = 4 };

/// The machine-readable half of a log line, so an embedder can route and count without
/// parsing the message. Values are stable across releases; append, never renumber.
enum class LogEvent : int {
    FlushComplete = 0,
    CompactionComplete = 1,
    CompactionFailed = 2,
    MigrationComplete = 3,
    /// A background operation failed. `BackgroundRetry` follows if it is retried.
    BackgroundFailure = 4,
    BackgroundRetry = 5,
    StallEntered = 6,
    StallLeft = 7,
    StoresDiscarded = 8,
    Fenced = 9,
    GenerationRolled = 10,
    OrphansReclaimed = 11,
};

/// A vtable rather than `std::function` so it crosses the C ABI — see `Options::clock` for the
/// asymmetry this exists to avoid.
///
/// **Called on engine threads, with no engine lock held, synchronously.** A slow sink applies
/// backpressure to flush and compaction; use an async appender. `message` is valid only for the
/// duration of the call and is not NUL-terminated.
struct Logger {
    void* context = nullptr;
    void (*write)(void* context, LogLevel level, LogEvent event, const char* message,
                  size_t len) = nullptr;
};

struct Options {
    /// ARCHITECTURE.md "A tier is not a level" — ordered hot to cold. The last tier catches everything.
    std::vector<Tier> tiers;
    /// ARCHITECTURE.md "Compaction" — gaps inherit the nearest shallower entry.
    std::map<int, LevelOptions> levels;

    std::shared_ptr<ManifestCatalog> manifest_catalog;
    std::shared_ptr<BlockCache> block_cache;
    std::shared_ptr<MemoryBudget> memory_budget;

    size_t memtable_bytes = 64ull << 20;

    /// Flush the memtable once it has been open this long, even if it never reaches
    /// `memtable_bytes`. **Size and age are alternatives: whichever comes first
    /// flushes.** Unset means size alone decides, which is the historical behaviour.
    ///
    /// This closes the front of the durability story. A tier's `max_age` starts
    /// counting from the memtable's creation time, but it can only act on a *file* —
    /// so under a trickle of writes that never fills a memtable, data stays in memory
    /// indefinitely and no tier bound applies to it. An interval bounds how long a
    /// write can sit somewhere a crash would lose it, independent of write rate.
    ///
    /// Costs write amplification: a short interval on a quiet store produces small L0
    /// files, and small files mean more compaction to merge them away. Pick it from
    /// how much recent data you are willing to lose, not from a latency target.
    std::optional<Duration> flush_interval;

    /// Spreads the flush interval across `[interval × (1 − j), interval × (1 + j)]`, per memtable.
    /// Zero, the default, keeps it exact. `Status::Config` outside `[0, 1]`.
    ///
    /// **Both directions, unlike `age_jitter`.** A late flush costs replay on restart and breaks
    /// no promise, so there is no reason to only pull it earlier.
    ///
    /// What it smooths is compaction queue depth: instances opened together flush together, and
    /// the L0 files they produce arrive at the compactor as one wave.
    double flush_interval_jitter = 0.0;

    /// Spreads each file's tier `max_age` crossing across `[max_age × (1 − j), max_age]`, per
    /// file. Zero, the default, keeps it exact. `Status::Config` outside `[0, 1]`.
    ///
    /// **Earlier only.** A `Transient` tier's `max_age` is an exposure bound the engine promises,
    /// so a file may cross early but never late.
    ///
    /// Stores normally drift apart on their own, and this is for the times they do not: a rebuild
    /// stamps `min_write_time_ms` on everything it replays within the same few minutes, so the
    /// whole store crosses together and migrates as one burst. That repeats after every rebalance
    /// for an embedder that rebuilds on assignment.
    ///
    /// The offset is derived from the file's number and write time, not rolled, so it survives a
    /// reopen instead of re-clustering the files it just spread. `stall_age` is deliberately left
    /// exact — it is an alarm, and blurring it would only make the alarm harder to read.
    double age_jitter = 0.0;

    /// How often the maintenance coordinator reconciles: it evaluates every background
    /// policy — flush, compaction, migration off a transient tier, capacity eviction,
    /// obsolete-object collection — against current state and the clock, and dispatches what
    /// is due.
    ///
    /// **It exists because a policy driven by time needs a trigger that is not a write.** Every
    /// age bound in this engine used to be evaluated only when something arrived, so a store
    /// that went quiet with a file sitting on a transient tier left it there indefinitely. The
    /// coordinator is what asks.
    ///
    /// Short and boring on purpose. It is not a latency knob: the interval is the smallest term
    /// in the exposure window — `max_age + interval + queueing behind an in-flight compaction +
    /// the migration itself` — so spending accuracy here buys nothing. An idle tick is two
    /// comparisons and no version scan, which is what makes a one-second default affordable
    /// across dozens of instances in one process.
    Duration maintenance_interval{1000};

    /// How long data lives before the engine drops it, measured from when it was written.
    ///
    /// **Expiry by manifest edit: a file whose every write has outlived this is unlinked whole.**
    /// Nothing is read and nothing is rewritten, which is what makes it affordable to run
    /// continuously — the same trick `truncate_below` uses, keyed on age instead of key order.
    ///
    /// Three things it is not, each worth knowing before relying on it:
    ///
    /// - **The granularity is the file, not the key.** The manifest names files, so a file is the
    ///   smallest thing an edit can drop; reaching inside one means rewriting it, which is
    ///   compaction and no longer free. This buys "data older than X disappears", not "this key
    ///   expires at X".
    /// - **At or after, never before.** A file is dropped when the sweep next runs and finds it
    ///   expired, so data may outlive the limit by up to `orphan_sweep_interval`. It is never
    ///   dropped early, which is the direction that matters.
    /// - **Only where nothing older sits beneath it.** Dropping a file that shadows an older
    ///   version of the same key would *uncover* that older version rather than remove the key —
    ///   a resurrection, not an expiry. So a file expires only once no older file overlaps its
    ///   range, which in practice means once it has reached the bottom of the tree.
    ///
    /// Unset — the default — never expires anything.
    std::optional<Duration> ttl;

    /// How long an object *this instance obsoleted* is kept after nothing local references it.
    ///
    /// **Protects readers, and only readers.** A read-only instance in another process holds a
    /// version this one has already superseded, and the collector cannot see it — `live_versions_`
    /// is a process-local list. Deferring the delete is what makes a reader in another process
    /// safe, and it costs storage rather than coordination: no registration, no leases, no limit on
    /// how many readers there are, and nothing to go wrong when one crashes.
    ///
    /// Unset — the default — deletes as soon as the object is locally unreferenced, which is
    /// correct when there are no readers.
    std::optional<Duration> obsolete_retention;

    /// How long an object must be *continuously observed* unreferenced before the orphan sweep
    /// deletes it.
    ///
    /// **Protects a concurrently-writing process**, and is needed whether or not readers exist. An
    /// object unreferenced at the instant we happen to look is indistinguishable from another
    /// writer's file whose edit became durable between our manifest read and our store listing —
    /// which is why deleting on a single observation was removed. A *sustained* observation is a
    /// claim the engine can actually make.
    ///
    /// Deliberately not optional: there is no configuration in which deleting an object seen
    /// unreferenced once is correct. Turn the sweep off with `orphan_sweep_interval` instead, which
    /// says what it means. Must be at least `obsolete_retention` — checked at open — because a
    /// crash empties the pending queue and an obsoleted object comes back as an orphan, protected
    /// by this window and nothing else.
    Duration orphan_retention{std::chrono::hours(24)};

    /// How often to list the stores looking for orphans. Unset disables the sweep, which costs
    /// storage and nothing else: the engine's correctness never depends on reclamation happening.
    /// Stepping the file-number counter over what the stores already hold is what makes *not*
    /// deleting safe, and that is unconditional.
    ///
    /// The sweep is O(objects) with a paginated list per store, so this belongs in hours, not
    /// seconds. An object can only be *first seen* on a sweep, so effective patience is
    /// `orphan_retention` plus up to one interval.
    std::optional<Duration> orphan_sweep_interval;

    size_t block_bytes = 4096;
    int restart_interval = 16;
    int bloom_bits_per_key = 10;
    size_t max_compaction_bytes = 400ull << 20;

    /// How much of a compaction input is read at a time.
    ///
    /// **The knob that decides what a compaction costs against object storage.** A merge reads
    /// every block of every input exactly once and asks for them one at a time, which is a round
    /// trip per block; the window makes it one per `compaction_window_bytes`. Total requests are
    /// therefore `input bytes / this`, and against a store with 20 ms of latency that number *is*
    /// the compaction's duration.
    ///
    /// **Traded directly against memory, which is why it is not simply large.** Every input of a
    /// compaction is read concurrently — the merge interleaves them — so the windows are all live
    /// at once, and each input holds two of them: the one being merged and the one being fetched
    /// ahead of it. The footprint is `2 x this x inputs x concurrent compactions`, and with the
    /// default `max_compaction_bytes` and a 16 MiB `target_file_bytes` that is about 25 inputs. It
    /// is charged to `memory_budget` when one is set, so the cost is visible rather than inferred.
    size_t compaction_window_bytes = 2ull << 20;

    /// Serve reads while a discarded transient store is still unreplayed.
    ///
    /// **Off, so the safe path is the default one.** A discard leaves the store *wrong* rather than
    /// merely incomplete — a key whose newer value lived on the lost store now reads as its older
    /// one — and reporting that through a flag makes noticing it opt-in. Reads therefore fail with
    /// `Status::RecoveryRequired` until `mark_recovery_complete()`.
    ///
    /// **Writes are never refused either way**: the replay that discharges the condition is made of
    /// them. Turn this on for a replay that also reads, which is the shape a changelog consumer
    /// usually has — and in doing so accept that those reads may be behind, which for a replayer
    /// about to overwrite them is exactly the trade it wants.
    bool allow_reads_before_recovery = false;

    /// Compact a file once this fraction of its entries are tombstones. Zero — the default — is off.
    ///
    /// **The trigger the size ratios cannot express.** A tombstone is not an erasure: it shadows
    /// older copies of its key and can only be dropped once it reaches the bottommost level for its
    /// range. Until then every scan over a deleted region pays to skip it. A delete-heavy store
    /// whose levels stay within their byte and file budgets therefore never trips a compaction, and
    /// the tombstones accumulate — visible only as scans getting slower, which is the hardest kind
    /// of regression to attribute.
    ///
    /// Expressed as a score against this threshold rather than as a separate trigger, so it
    /// competes with the size ratios on one scale and ARCHITECTURE.md's "score is the only trigger"
    /// stays true.
    ///
    /// Off by default because it is a workload judgement, not a safety property: a store that
    /// deletes little pays for the check and gains nothing, and a store that deletes in bulk
    /// usually wants `truncate_below` instead, which reclaims without rewriting anything.
    double tombstone_density_trigger = 0.0;

    /// Entries a file needs before its density is allowed to trigger anything.
    ///
    /// Without a floor a file holding two entries, one of them a tombstone, scores 0.5 and would
    /// fire a compaction that rewrites almost nothing — repeatedly, since compacting it produces
    /// another small file. The bound is on entries rather than bytes because the cost being
    /// avoided is per-entry skipping during a scan.
    uint64_t tombstone_density_min_entries = 1024;

    /// Bytes of open-`SstReader` state — each file's index block and bloom filter —
    /// kept resident, least-recently-used first. Zero means unbounded.
    ///
    /// **The filter is what makes this matter:** at the default 10 bits per key it is
    /// ~1.25 MB for a million-entry file, so a store with a thousand such files held
    /// over a gigabyte before this had a bound. It is generous rather than tight on
    /// purpose — evicting a reader costs three reads to reopen it (footer, index,
    /// filter), which against a remote store is three round trips, so a reader cache
    /// too small for the working set is a far worse deal than the memory it saves.
    size_t reader_cache_bytes = 64ull << 20;
    int manifest_edits_per_generation = 1000;

    BackgroundMode background = BackgroundMode::Threaded;

    /// Injectable for tests — the drain cases of ARCHITECTURE.md "Fault injection" need to move time without
    /// waiting for it.
    std::function<uint64_t()> clock;
    bool paranoid_checks = false;

    /// When false, a write that would stall returns `Status::Stalled` instead of
    /// blocking. the valve is not configurable off; this only chooses how the
    /// caller learns about it.
    bool block_on_stall = true;

    /// Null means no logging, and no message is formatted.
    std::shared_ptr<Logger> logger;
    LogLevel min_log_level = LogLevel::Info;
};

uint64_t default_clock();

}  // namespace elysiumkv

#endif  // ELYSIUMKV_OPTIONS_HPP
