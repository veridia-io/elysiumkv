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

    /// Deletes `[lower, upper)` as part of this batch.
    ///
    /// Order within the batch decides what survives: a `put` after this one lands on top of the
    /// range and lives; a `put` before it is covered and does not. That is the same rule the
    /// standalone call follows, for the same reason — the memtable resolves the ordering as each
    /// operation is applied — and it is what makes "evict a tenant and re-seed the space" one
    /// atomic step rather than two calls with the range visibly empty between them.
    WriteBatch& delete_range(Slice lower, Slice upper);

    size_t size() const { return ops_.size(); }
    void clear() { ops_.clear(); }

    enum class Kind : uint8_t { Put, Remove, DeleteRange };

    /// One operation. For `DeleteRange` the pair is the bounds: `key` is `lower` and `value` is
    /// `upper`. A kind rather than the `is_delete` flag it replaces, because three states do not
    /// fit in a bool and two bools would admit a fourth that means nothing.
    struct Op {
        Kind kind = Kind::Put;
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

/// The read surface, and nothing else. A separate type rather than a runtime refusal, so passing a
/// read-only handle where a write happens is a compile error. `DB` extends this.
///
/// Any number of processes may hold one against a store another process is writing: objects are
/// immutable and write-once, so a cached block can never become wrong, and a reader performs no
/// compare-and-set. It requires the writer to defer deleting superseded objects — see
/// `Options::obsolete_retention`, which is what stands between a compaction there and a vanished
/// file here.
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

    /// The same four scans, descending. `next()` still advances, towards smaller keys: the first
    /// call yields the largest key in range.
    ///
    /// Bounds keep their forward meaning, so `[a, d)` describes the same set in either direction and
    /// only the delivery order changes.
    ///
    /// A direction is chosen once; there is no `prev()`. Turning around mid-scan would re-seek every
    /// underlying source on each turn.
    virtual std::unique_ptr<Iterator> reverse_iterator() = 0;
    virtual std::unique_ptr<Iterator> reverse_iterator(Slice lower_inclusive,
                                                       Slice upper_exclusive) = 0;
    /// Descends from the end of the keyspace down to `lower_inclusive`.
    virtual std::unique_ptr<Iterator> reverse_iterator(Slice lower_inclusive) = 0;
    virtual std::unique_ptr<Iterator> reverse_prefix_iterator(Slice prefix) = 0;

    /// Re-reads the manifest and installs the newest version.
    ///
    /// Explicit rather than automatic, so a caller can hold a stable view for the length of a query:
    /// a background refresh would let two `get`s in one logical operation observe different
    /// versions.
    ///
    /// Open iterators are unaffected — an iterator holds its own version — so this is safe to call
    /// at any time and a long scan does not block it.
    ///
    /// A no-op returning `Status::Ok` on a writable instance, whose version is already newest.
    virtual Status refresh() = 0;

    /// The last position whose effect on the store is known to have survived, as established at
    /// open — see `DB::set_watermark`. Fixed for the life of the instance, including across
    /// `refresh()`: it describes the state this instance recovered, and `Stats::durable_watermark`
    /// is the live counterpart.
    virtual std::optional<uint64_t> recovered_watermark() const = 0;

    virtual Stats stats() const = 0;
    /// The embedder declaring its replay finished: discharges `requires_recovery` and removes the
    /// watermark floor a discard installed, so recovery may read its frontier from the files again.
    ///
    /// Returns a `Status` because clearing the floor is a manifest write — the floor's value is that
    /// it survives a crash, so removing it must too. A failure leaves the floor in place, costing a
    /// repeated replay and losing nothing.
    ///
    /// Precondition: the replay is complete. Calling it early asserts that a known gap has been
    /// filled, which the engine cannot verify.
    virtual Status mark_recovery_complete() = 0;

    virtual ~ReadOnlyDB() = default;

    /// Whether a `delete_range(lower, upper)` has finished travelling through the tree: no file at
    /// any level still holds data in the band. Costs no reads — every file's key range is already in
    /// the manifest.
    ///
    /// Conservative, because a recorded range is a hull and a file can overlap the band while
    /// holding no key in it. `true` means every file that could have held one is gone; `false`
    /// carries no information.
    ///
    /// Answers about files only. A write into the band after the deletion is a new write and lives
    /// in the memtable until flushed, so ask about a band nobody is writing to.
    virtual Result<bool> range_is_erased(Slice lower, Slice upper) const = 0;
};

class DB : public ReadOnlyDB {
public:
    /// Opens a store whose configuration cannot discard on open — every level
    /// `Durable`. Returns `Status::Config` if any level is `Transient` — a check rather than a
    /// documented precondition, because a configuration change
    /// must not silently turn existing call sites into stale-data readers.
    static Result<std::unique_ptr<DB>> open(const Options&);

    /// Opens any configuration, reporting discard state.
    static Result<OpenResult> open_with_result(const Options&);

    /// Opens without taking ownership: no manifest write of any kind, no background threads, no
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
    /// Monotone: a call at or below the current point is a no-op returning `Ok`, so replay is
    /// idempotent. There is no un-truncate.
    ///
    /// Visibility changes at once; space comes back over time. A file entirely below the point is
    /// unlinked whole with no rewrite; one straddling it keeps its live half until compaction
    /// narrows it. An open iterator pins the Version it started on and is unaffected.
    ///
    /// The floor is permanent, and a later write below it is refused with `Status::Config` rather
    /// than accepted and hidden: positional recency is the only ordering the engine has, so it
    /// cannot distinguish a key written before the truncation from one written after. To clear a
    /// range and keep writing into it, use `delete_range`.
    virtual Status truncate_below(Slice key) = 0;

    /// ARCHITECTURE.md "A range delete is a record, not a rewrite" — deletes every key in
    /// `[lower, upper)`.
    ///
    /// The counterpart to `truncate_below` for a range that is not a prefix of the keyspace: this
    /// deletes a band from anywhere, leaves it writable afterwards, and costs one record rather than
    /// one per key.
    ///
    /// `lower` is included and `upper` is not, the convention an iterator's bounds use. An empty or
    /// inverted range deletes nothing and is not an error.
    ///
    /// Dearer than a floor, which moves one value in the manifest: this writes a tombstone that
    /// every read in the range consults until compaction resolves it, and space returns only as the
    /// covered files are rewritten or dropped whole.
    virtual Status delete_range(Slice lower, Slice upper) = 0;


    /// Forces memtable -> L0 and waits for it.
    virtual Status flush() = 0;

    /// Drops whatever is unflushed instead of trying to save it at destruction.
    ///
    /// Destruction attempts a flush by default, since there is no write-ahead log. The attempt is
    /// best-effort and its failure is not reported — a destructor has nowhere to report to — so
    /// `flush()` remains the only way to know.
    ///
    /// This turns the attempt off, which is what a crash looks like from the store's side.
    virtual void abandon_unflushed() = 0;

    /// Rewrites every file at `level` under current compression and placement.
    /// At the last level this rewrites in place; elsewhere it compacts into the
    /// level below. One pass over the level's files, so it terminates
    /// by construction.
    ///
    /// The mechanism for one-time operations with a completion condition: finishing a compression
    /// change (ARCHITECTURE.md "Inside an SST"), completing a placement remap (ARCHITECTURE.md "A
    /// tier is not a level"), reclaiming accumulated tombstones. Levels have no age, so none of
    /// these can be driven by a timer.
    ///
    /// Ordinary compaction reaches a key range only when it sweeps past it, and a range receiving no
    /// writes is never swept. `LevelStats::files_stale_codec` reports how much remains.
    virtual Status compact_level(int level) = 0;

    /// Records that every write completed so far is at a position at or before `position` in
    /// whatever log the embedder is replaying — a changelog offset, typically. The engine orders
    /// it, carries it with the data, and hands it back at the next open; it never invents,
    /// interpolates or interprets one.
    ///
    /// A position, not a time, and unrelated to `min_write_time_ms` or a tier's `max_age`.
    ///
    /// One store under the lock the write path already takes: it forces no flush and writes no
    /// manifest, so it may be called as often as the embedder commits. The value becomes durable
    /// when the memtable holding it is flushed, which `flush()` does immediately and
    /// `Options::flush_interval` bounds on a quiet store.
    ///
    /// Positions must be non-decreasing. A decreasing one is a caller bug and is refused with
    /// `Status::Config` rather than clamped, because clamping would hide it.
    ///
    /// A resume point, not a durability improvement. There is no write-ahead log, so
    /// an unflushed memtable is still lost on a crash; the watermark tells you where to resume,
    /// it does not reduce what you lost.
    virtual Status set_watermark(uint64_t position) = 0;

    /// The last position whose effect on the store is known to have survived, as established at
    /// open — a fixed property of the recovered state, not a live value. `Stats` carries the
    /// live one, under a different name, precisely so this one's meaning cannot change after the
    /// first write.
    ///
    /// The guarantee: replaying only the positions after the returned value onto the
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
