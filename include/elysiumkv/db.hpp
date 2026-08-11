#ifndef ELYSIUMKV_DB_HPP
#define ELYSIUMKV_DB_HPP

#include "elysiumkv/options.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/stats.hpp"
#include "elysiumkv/status.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace elysiumkv {

class DB;

/// ARCHITECTURE.md "Absence is an answer, not an error" — a borrowed value pinning a block-cache page. Zero-copy: the keep-alive
/// is a shared_ptr to the block (or, for a memtable hit, to the arena that owns
/// the bytes), so the value stays readable however the cache behaves.
class Pinned {
public:
    Pinned() = default;
    /// `outstanding` is the engine's live-pin counter (ARCHITECTURE.md "The ABI boundary"): a leaked pin holds a
    /// block-cache entry forever, so the count is an invariant, not a
    /// diagnostic. Move transfers ownership of the count; there is exactly one
    /// decrement per increment.
    Pinned(std::shared_ptr<const void> keep_alive, Slice value,
           std::atomic<uint64_t>* outstanding = nullptr)
        : keep_alive_(std::move(keep_alive)), value_(value), outstanding_(outstanding) {
        if (outstanding_ != nullptr) outstanding_->fetch_add(1, std::memory_order_relaxed);
    }

    Pinned(Pinned&& other) noexcept
        : keep_alive_(std::move(other.keep_alive_)),
          value_(other.value_),
          outstanding_(std::exchange(other.outstanding_, nullptr)) {
        other.value_ = Slice();
    }
    Pinned& operator=(Pinned&& other) noexcept {
        if (this != &other) {
            release();
            keep_alive_ = std::move(other.keep_alive_);
            value_ = other.value_;
            outstanding_ = std::exchange(other.outstanding_, nullptr);
            other.value_ = Slice();
        }
        return *this;
    }
    Pinned(const Pinned&) = delete;
    Pinned& operator=(const Pinned&) = delete;
    ~Pinned() { release(); }

    Slice value() const { return value_; }

private:
    void release() {
        if (outstanding_ != nullptr) {
            outstanding_->fetch_sub(1, std::memory_order_relaxed);
            outstanding_ = nullptr;
        }
        keep_alive_.reset();
        value_ = Slice();
    }

    std::shared_ptr<const void> keep_alive_;
    Slice value_;
    std::atomic<uint64_t>* outstanding_ = nullptr;
};

/// ARCHITECTURE.md "Absence is an answer, not an error" — next()-only. The first `next()` positions at the first key in range;
/// `key()` and `value()` are valid only after `next()` returned true. Tombstones
/// are resolved away, so an iterator never yields a deleted key.
class Iterator {
public:
    virtual bool next() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    /// Check after next() returns false: exhaustion and failure look the same
    /// otherwise.
    virtual Status status() const = 0;
    /// Releases the pinned Version.
    virtual ~Iterator() = default;
};

/// ARCHITECTURE.md "Absence is an answer, not an error" — applied as a unit: the whole batch lands in one memtable, so a flush
/// never splits it.
class WriteBatch {
public:
    WriteBatch& put(Slice key, Slice value);
    WriteBatch& remove(Slice key);
    size_t size() const { return ops_.size(); }
    void clear() { ops_.clear(); }

    struct Op {
        bool is_delete = false;
        std::string key;
        std::string value;
    };
    const std::vector<Op>& ops() const { return ops_; }

private:
    std::vector<Op> ops_;
};

struct OpenResult {
    std::unique_ptr<DB> db;
    /// Stores whose files were dropped because the store lost data (ARCHITECTURE.md "A tier is not a level").
    /// Structurally empty for an all-durable configuration.
    ///
    /// Stores rather than levels: tier and level are independent, so a lost
    /// store's files are scattered across levels rather than forming a
    /// contiguous band — which is why whole-store is the only sane granularity
    /// here, not merely the preferred one.
    std::vector<std::string> discarded_stores;
    uint64_t discarded_files = 0;
    bool requires_recovery = false;
};

/// The read surface, and nothing else.
///
/// **A separate type rather than a runtime refusal**: a read-only handle cannot be passed where a
/// write happens, and the compiler says so. `DB` extends this, so the declarations below are not
/// duplicated — a writable handle is a readable one.
///
/// Multiple processes may hold one of these against a store another process is writing. That works
/// because objects are immutable and write-once, so a cached block can never become wrong, and
/// because a reader performs no compare-and-set and is therefore outside the ownership protocol
/// entirely. What it does require is that the writer defer deleting superseded objects — see
/// `Options::obsolete_retention`, which is the one thing standing between a compaction over there
/// and a vanished file over here.
class ReadOnlyDB {
public:
    virtual Result<Pinned> get(Slice key) = 0;  ///< zero-copy
    virtual Result<std::vector<uint8_t>> get_copy(Slice key) = 0;

    virtual std::unique_ptr<Iterator> iterator() = 0;
    virtual std::unique_ptr<Iterator> iterator(Slice lower_inclusive, Slice upper_exclusive) = 0;
    /// Scans from `lower_inclusive` to the end of the keyspace. Not sugar for a
    /// two-argument call with an empty upper bound: an empty key is a key, so
    /// that would ask for `[lower, "")` — the empty range — and quietly return
    /// nothing. Every binding wants "from here onwards", and without this it can
    /// only be spelled by inventing a maximum key, which does not exist.
    virtual std::unique_ptr<Iterator> iterator(Slice lower_inclusive) = 0;
    virtual std::unique_ptr<Iterator> prefix_iterator(Slice prefix) = 0;

    /// The same four scans, descending. `next()` still means "advance", it just advances downwards:
    /// the first call yields the largest key in range and each later one yields the next smaller.
    ///
    /// **Bounds keep their meaning, not their roles.** `lower_inclusive` and `upper_exclusive`
    /// describe the same set of keys as they do forward — only the order of delivery changes. So a
    /// reverse scan of `[a, d)` starts at the largest key below `d` and ends at `a`. Defining it any
    /// other way would make a range mean one thing forward and another backward.
    ///
    /// **A direction is chosen once.** There is no `prev()`: an iterator runs one way to
    /// exhaustion. Turning around mid-scan would cost a re-seek of every underlying source on each
    /// turn, and no caller has needed it — a descending scan is what a "last N" query wants.
    virtual std::unique_ptr<Iterator> reverse_iterator() = 0;
    virtual std::unique_ptr<Iterator> reverse_iterator(Slice lower_inclusive,
                                                       Slice upper_exclusive) = 0;
    /// Descends from the end of the keyspace down to `lower_inclusive`.
    virtual std::unique_ptr<Iterator> reverse_iterator(Slice lower_inclusive) = 0;
    virtual std::unique_ptr<Iterator> reverse_prefix_iterator(Slice prefix) = 0;

    /// Re-reads the manifest and installs the newest version.
    ///
    /// **Explicit, never automatic.** A background refresh would mean two `get`s in one logical
    /// operation could observe different versions, with nothing in the API marking where that can
    /// happen — and a reader that wants a stable view for the length of a query must be able to have
    /// one. Freshness is the embedder's requirement, not the engine's.
    ///
    /// Open iterators are unaffected: an iterator holds its version, which is what already keeps its
    /// files alive locally. So this is safe to call at any time and a long scan does not block it.
    ///
    /// On a writable instance this is a no-op returning `Status::Ok` — the writer's version is
    /// already the newest by construction.
    virtual Status refresh() = 0;

    /// The last position whose effect on the store is known to have survived, as established at
    /// **open** — see `DB::set_watermark`. Fixed for the life of the instance, including across
    /// `refresh()`: it describes the state this instance recovered, and `Stats::durable_watermark`
    /// is the live counterpart.
    virtual std::optional<uint64_t> recovered_watermark() const = 0;

    virtual Stats stats() const = 0;
    virtual void mark_recovery_complete() = 0;

    virtual ~ReadOnlyDB() = default;
};

class DB : public ReadOnlyDB {
public:
    /// Opens a store whose configuration cannot discard on open — every level
    /// `Durable`. Returns `Status::Config` if any level is `Transient`: **a
    /// check, not a documented precondition**, because a configuration change
    /// must not silently turn existing call sites into stale-data readers.
    static Result<std::unique_ptr<DB>> open(const Options&);

    /// Opens any configuration, reporting discard state.
    static Result<OpenResult> open_with_result(const Options&);

    /// Opens without taking ownership: **no manifest write of any kind**, no background threads, no
    /// reclamation, and no compare-and-set. Several may be open at once, alongside a writer.
    ///
    /// Refuses a store with no manifest rather than creating one — a reader that finds nothing has
    /// opened the wrong place or arrived first, and either way it is not its business to decide.
    /// Refuses a store whose `Transient` tier has lost files, because repairing that is a manifest
    /// write and serving a version with holes would present stale values as current.
    static Result<std::unique_ptr<ReadOnlyDB>> open_read_only(const Options&);

    virtual Status put(Slice key, Slice value) = 0;
    virtual Status remove(Slice key) = 0;
    virtual Status write(WriteBatch&) = 0;

    /// Drops every key below `key`, in one manifest edit rather than one tombstone per key.
    ///
    /// **Monotone.** A call at or below the current point is a no-op returning `Ok`, so replay is
    /// idempotent and truncated data can never come back. There is no un-truncate.
    ///
    /// Visibility changes at once; space comes back over time. A file whose every key is below the
    /// point is unlinked whole, with no rewrite — which is what makes this cheap enough to run
    /// continuously. A file straddling the point keeps its live half until compaction narrows it.
    ///
    /// **An open iterator is unaffected**, because it pins the Version it started on — the same
    /// rule that already keeps its files readable after a compaction unlinks them.
    ///
    /// **The floor is permanent: a later write below it is refused with `Status::Config`, not
    /// accepted and hidden.** Keys below the point are unreadable because the point says so, not
    /// because anything was written per key, so the engine cannot tell a key written before the
    /// truncation from one written after — positional recency is the only ordering it has. The
    /// alternative would be a `put` that returns `Ok` and cannot be read back. A caller that wants
    /// to clear a range and go on writing into it wants a range delete, which is a different
    /// operation with a different cost.
    virtual Status truncate_below(Slice key) = 0;

    /// ARCHITECTURE.md "A range delete is a record, not a rewrite" — deletes every key in
    /// `[lower, upper)`.
    ///
    /// **The counterpart to `truncate_below`, for a range that is not a prefix of the keyspace.**
    /// A floor can only ever drop the lowest-sorting band and is permanent; this deletes a band from
    /// anywhere, leaves the range writable afterwards, and costs one record rather than one per key.
    /// A tenant sitting in the middle of a keyspace is the case that needs it.
    ///
    /// Bounds keep their meaning rather than their role: `lower` is included, `upper` is not — the
    /// same convention an iterator's bounds use. An empty or inverted range deletes nothing and is
    /// not an error, matching an iterator over the same bounds, which yields nothing.
    ///
    /// **Not free the way `truncate_below` is.** A floor moves one value in the manifest; this
    /// writes a tombstone that every read in the range consults until compaction resolves it, and
    /// the space comes back only when the covered files are rewritten or dropped whole. It is
    /// cheaper than a delete per key by a wide margin, and dearer than a floor.
    virtual Status delete_range(Slice lower, Slice upper) = 0;

    /// Forces memtable -> L0 and waits for it.
    virtual Status flush() = 0;

    /// Drops whatever is unflushed instead of trying to save it at destruction.
    ///
    /// **Destruction attempts a flush by default**, because there is no write-ahead log and a
    /// memtable thrown away on a clean shutdown is lost for no reason — the process had every
    /// opportunity to write it. The attempt is best-effort and its failure is not reported: a
    /// destructor has nowhere to report to, and promising durability from one would be worse than
    /// promising nothing. `flush()` is still the only way to *know*.
    ///
    /// This turns the attempt off, which is what a crash looks like from the store's side. Two
    /// callers want it: a test that means to lose the memtable, and an embedder that has decided
    /// the writes are not worth the shutdown latency.
    virtual void abandon_unflushed() = 0;

    /// Rewrites every file at `level` under current compression and placement.
    /// At the last level this rewrites in place; elsewhere it compacts into the
    /// level below. One pass over the level's files, so it terminates
    /// by construction.
    ///
    /// This is the mechanism for one-time operations: finishing a compression
    /// change (ARCHITECTURE.md "Inside an SST"), completing a placement remap (ARCHITECTURE.md "A tier is not a level"), reclaiming
    /// accumulated tombstones. Those have a completion condition and must not be
    /// driven by an age timer — levels have no age at all (ARCHITECTURE.md "Compaction").
    ///
    /// Compaction is what makes a codec change *happen* — it reaches new data at
    /// once and old data as compaction sweeps past it. This is what makes it
    /// *finish*: a key range receiving no writes is never swept, and
    /// `LevelStats::files_stale_codec` reports how much remains.
    virtual Status compact_level(int level) = 0;

    /// Records that every write completed so far is at a position at or before `position` in
    /// whatever log the embedder is replaying — a changelog offset, typically. The engine orders
    /// it, carries it with the data, and hands it back at the next open; it never invents,
    /// interpolates or interprets one.
    ///
    /// **It is a position, not a time**, and unrelated to `min_write_time_ms` or a tier's
    /// `max_age`, which are wall-clock quantities driving placement. Nothing here relates them.
    ///
    /// Cheap and non-blocking: one store under the lock the write path already takes. It forces
    /// no flush and writes no manifest, so it can be called as often as the embedder commits.
    /// The value becomes durable when the memtable holding it is flushed, which is why
    /// `flush()` promotes it immediately and why `Options::flush_interval` is what bounds the lag
    /// on a quiet store.
    ///
    /// Positions must be **non-decreasing**. A decreasing one is a caller bug and is refused with
    /// `Status::Config` rather than clamped, because clamping would hide it.
    ///
    /// **This is a resume point, not a durability improvement.** There is no write-ahead log, so
    /// an unflushed memtable is still lost on a crash; the watermark tells you where to resume,
    /// it does not reduce what you lost.
    virtual Status set_watermark(uint64_t position) = 0;

    /// The last position whose effect on the store is known to have survived, as established at
    /// **open** — a fixed property of the recovered state, not a live value. `Stats` carries the
    /// live one, under a different name, precisely so this one's meaning cannot change after the
    /// first write.
    ///
    /// The guarantee: replaying only the positions **after** the returned value onto the
    /// recovered database yields the same logical key–value state as replaying the entire log.
    /// Exclusive, not inclusive — `80` means resume at `81`, and the boundary is exactly where
    /// the proof is tight. Stated as state equivalence rather than record retention because
    /// compaction drops superseded values and dead tombstones, so no physical-retention claim
    /// would be true; state equivalence is what a changelog consumer actually needs.
    ///
    /// `nullopt` means nothing can be certified and the embedder should replay from the
    /// beginning: either no watermark was ever set, or a lost transient store held data that
    /// predates the first one. Distinct from zero, which is a valid position.
    ///
    /// Declared on `ReadOnlyDB`; repeated here only because the surrounding documentation belongs
    /// with `set_watermark`.
    std::optional<uint64_t> recovered_watermark() const override = 0;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DB_HPP
