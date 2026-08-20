#ifndef ELYSIUMKV_OPTIONS_HPP
#define ELYSIUMKV_OPTIONS_HPP

#include "elysiumkv/blob_store.hpp"
#include "elysiumkv/encryption.hpp"
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

/// Per level, not per tier (ARCHITECTURE.md "Inside an SST"). A tier-scoped codec would force
/// migration to decompress and recompress every file it moves, turning a byte copy into a rewrite.
enum class Compression : uint8_t {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
};

inline bool is_known_compression(uint8_t raw) {
    return raw <= static_cast<uint8_t>(Compression::Zstd);
}

/// Which recovery path a loss of this tier's store triggers (ARCHITECTURE.md "A tier is not a
/// level"). Not a rating of the storage medium.
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
    /// Testing: background work runs inline when the driver calls for it, so the op stream fully
    /// determines the execution. Shrinking a failing stream requires that determinism.
    Inline,
};

/// Where files live (ARCHITECTURE.md "A tier is not a level"). Tier and level are independent axes:
/// level is LSM structure, tier is storage. A file lives in exactly one store, chosen by its age, so
/// a single level routinely spans several tiers.
struct Tier {
    std::shared_ptr<BlobStore> store;  ///< may itself be a cache chain
    Durability durability = Durability::Durable;

    std::optional<Duration> max_age;        ///< files older than this move to the next tier

    /// Tier capacity; oldest files evicted first.
    ///
    /// There is no per-file size bound, because placement must be monotone in age alone: a file's
    /// age only grows, so its tier only descends, where a size bound could place a new file cold on
    /// the day it was written. To keep large files off a small fast tier, lower that level's
    /// `target_file_bytes`.
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
    /// One file rewritten under the primary encryption provider. See
    /// `EncryptionOptions::rewrite_to_primary`.
    EncryptionRewritten = 12,
};

/// A vtable rather than `std::function` so it crosses the C ABI — see `Options::clock` for the
/// asymmetry this exists to avoid.
///
/// Called on engine threads, with no engine lock held, synchronously. A slow sink applies
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
    /// `memtable_bytes`. Whichever comes first flushes; unset means size alone decides.
    ///
    /// Bounds how long a write sits somewhere a crash would lose it, independent of write rate. No
    /// tier `max_age` can do that: those act on files, and an unflushed memtable is not one.
    ///
    /// Costs write amplification — a short interval on a quiet store produces small L0 files, and
    /// small files mean more compaction. Pick it from how much recent data you are willing to lose,
    /// not from a latency target.
    std::optional<Duration> flush_interval;

    /// Spreads the flush interval across `[interval × (1 − j), interval × (1 + j)]`, per memtable.
    /// Zero, the default, keeps it exact. `Status::Config` outside `[0, 1]`.
    ///
    /// Both directions, unlike `age_jitter`: a late flush costs replay on restart and breaks no
    /// promise. Smooths compaction queue depth, since instances opened together flush together.
    double flush_interval_jitter = 0.0;

    /// Spreads each file's tier `max_age` crossing across `[max_age × (1 − j), max_age]`, per
    /// file. Zero, the default, keeps it exact. `Status::Config` outside `[0, 1]`.
    ///
    /// Earlier only: a `Transient` tier's `max_age` is an exposure bound the engine promises, so a
    /// file may cross early but never late.
    ///
    /// For a store whose files all carry nearly the same `min_write_time_ms` — a rebuild from a log
    /// — which would otherwise cross together and migrate as one burst. The offset is derived from
    /// the file's number and write time rather than rolled, so a reopen recomputes the same one.
    /// `stall_age` is left exact, being an alarm.
    double age_jitter = 0.0;

    /// How often the maintenance coordinator evaluates every background policy — flush,
    /// compaction, migration off a transient tier, capacity eviction, obsolete-object collection —
    /// against current state and the clock, dispatching what is due.
    ///
    /// A policy driven by time needs a trigger that is not a write: without this, a store that goes
    /// quiet with a file on a transient tier leaves it there.
    ///
    /// Not a latency knob. The interval is the smallest term in the exposure window
    /// (`max_age + interval + queueing behind an in-flight compaction + the migration itself`), so
    /// accuracy here buys nothing. An idle tick is two comparisons and no version scan.
    Duration maintenance_interval{1000};

    /// How long data lives before the engine drops it, measured from when it was written.
    ///
    /// Expiry by manifest edit: a file whose every write has outlived this is unlinked whole,
    /// nothing read and nothing rewritten.
    ///
    /// Three limits it carries:
    ///
    /// - The granularity is the file, not the key. The manifest names files, so reaching inside one
    ///   means rewriting it. This buys "data older than X disappears", not "this key expires at X".
    /// - At or after, never before. A file is dropped when the sweep next finds it expired, so data
    ///   may outlive the limit by up to `orphan_sweep_interval`.
    /// - Only where nothing older sits beneath it. Dropping a file that shadows an older version of
    ///   the same key would uncover that version rather than remove the key, so a file expires only
    ///   once no older file overlaps its range.
    ///
    /// Unset — the default — never expires anything.
    std::optional<Duration> ttl;

    /// How long an object *this instance obsoleted* is kept after nothing local references it.
    ///
    /// Protects readers, and only readers. A read-only instance in another process holds a version
    /// this one has superseded, and the collector cannot see it: `live_versions_` is process-local.
    /// Deferring the delete costs storage rather than coordination — no registration, no leases, no
    /// limit on reader count, nothing to go wrong when one crashes.
    ///
    /// Unset — the default — deletes as soon as the object is locally unreferenced.
    std::optional<Duration> obsolete_retention;

    /// How long an object must be *continuously observed* unreferenced before the orphan sweep
    /// deletes it.
    ///
    /// Protects a concurrently-writing process, whether or not readers exist. An object
    /// unreferenced at one instant is indistinguishable from another writer's file whose edit
    /// became durable between this instance's manifest read and its store listing; only a sustained
    /// observation distinguishes them.
    ///
    /// Not optional, because deleting an object seen unreferenced once is never correct. Turn the
    /// sweep off with `orphan_sweep_interval` instead. Must be at least `obsolete_retention`,
    /// checked at open: a crash empties the pending queue, and an obsoleted object then returns as
    /// an orphan protected by this window alone.
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
    /// Decides what a compaction costs against object storage: a merge reads every block of every
    /// input once, so requests are `input bytes / this`, and against a store with 20 ms of latency
    /// that number is the compaction's duration.
    ///
    /// Traded against memory. The merge interleaves its inputs, so every window is live at once and
    /// each input holds two — one being merged, one fetched ahead. The footprint is
    /// `2 x this x inputs x concurrent compactions`.
    ///
    /// Charged to `memory_budget` unconditionally but not bounded by it, since refusing a
    /// compaction's buffer would turn a memory decision into a durability one. `open` refuses a
    /// window two of which would exceed the whole budget.
    size_t compaction_window_bytes = 2ull << 20;

    /// Serve reads while a discarded transient store is still unreplayed.
    ///
    /// Off by default: a discard leaves the store wrong rather than merely incomplete, since a key
    /// whose newer value lived on the lost store now reads as its older one. Reads fail with
    /// `Status::RecoveryRequired` until `mark_recovery_complete()`.
    ///
    /// Writes are never refused either way — the replay that discharges the condition is made of
    /// them. Turn this on for a replay that also reads, accepting that those reads may be behind.
    bool allow_reads_before_recovery = false;

    /// Encryption at rest.
    ///
    /// There is always a provider and it is never null: the passthrough is pre-registered under the
    /// reserved empty id, so an unconfigured store is that provider being primary. Files record the
    /// id they were written under and a read routes on it, which is what lets a store hold files
    /// from several providers at once during a rotation.
    struct EncryptionOptions {
        /// id -> provider. The key is what objects record and what a read routes on. The engine
        /// adds the passthrough under `""`; registering that id yourself is refused.
        std::map<std::string, std::shared_ptr<EncryptionProvider>> providers;
        /// Which registered provider writes new objects. Empty means the passthrough.
        std::string primary_provider;

        /// Rewrite files recorded under any *other* provider, in the background, until none are
        /// left. Off by default.
        ///
        /// Changing `primary_provider` governs only what is written next: every existing file
        /// keeps the provider it was written under, and a cold file may never be compacted. This is
        /// what makes a rotation converge.
        ///
        /// A background pass rather than a compaction trigger, because re-sealing is a read and a
        /// write of one object — no merge, no block decode, no change to the shape of the LSM. Runs
        /// at the lowest priority, behind age migration; starving it costs the rotation time, not
        /// correctness.
        ///
        /// `Stats::files_pending_reencryption` reaches zero when the rotation is complete, which is
        /// the moment the retired provider may be unregistered. With nothing to rewrite the pass
        /// costs one scan of the version per maintenance pass.
        bool rewrite_to_primary = false;
    };
    EncryptionOptions encryption;

    /// Compact a file once this fraction of its entries are tombstones. Zero — the default — is off.
    ///
    /// The trigger the size ratios cannot express. A tombstone shadows older copies of its key
    /// until it reaches the bottommost level for its range, and every scan over the region pays to
    /// skip it — so a delete-heavy store staying inside its byte and file budgets never compacts.
    ///
    /// Expressed as a score against this threshold rather than as a separate trigger, so score
    /// remains the only compaction trigger.
    ///
    /// Off by default, being a workload judgement: a store deleting in bulk usually wants
    /// `truncate_below`, which reclaims without rewriting.
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
    /// The bloom filter dominates: at the default 10 bits per key it is ~1.25 MB per million-entry
    /// file. Sized generously because evicting a reader costs three reads to reopen it — footer,
    /// index, filter — which against a remote store is three round trips.
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
