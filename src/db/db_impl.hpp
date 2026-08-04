#ifndef ELYSIUMKV_DB_DB_IMPL_HPP
#define ELYSIUMKV_DB_DB_IMPL_HPP

#include "cache/sharded_lru.hpp"
#include "blob/tier.hpp"
#include "compact/migrator.hpp"
#include "compact/picker.hpp"
#include "db/level_config.hpp"
#include "memtable/skiplist_memtable.hpp"
#include "sst/sst_reader.hpp"
#include "sst/sst_reader_cache.hpp"
#include "elysiumkv/db.hpp"
#include "version/version_set.hpp"

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace elysiumkv {

std::string sst_object_name(uint64_t file_number);
/// Its inverse; nullopt for a name this engine did not write.
std::optional<uint64_t> sst_file_number(std::string_view name);

/// Which of the invariants failed.
///
/// ARCHITECTURE.md "Negative controls" — a bundled check that reports one status cannot be negatively
/// controlled. A single injection proves that *one* constituent still runs and
/// says nothing about the other four — and the realistic decay is not the whole
/// function collapsing, it is one check quietly ceasing to fire while the rest
/// keep the bundle green. Naming the failure is what makes one control per
/// constituent expressible.
enum class Invariant : uint8_t {
    None = 0,
    StoreMissing,   ///< a file names a store the configuration does not have
    ObjectMissing,  ///< the object is absent from the store that should hold it
    EntryCount,     ///< the file holds a different number of entries than recorded
    KeyRange,       ///< the recorded [smallest, largest] disagrees with the contents
    LevelOverlap,   ///< two files overlap at a level below L0 (ARCHITECTURE.md "Compaction")
};

/// For test messages and nothing else.
const char* invariant_name(Invariant invariant);

class DbImpl final : public DB {
public:
    static Result<OpenResult> open(const Options& options, bool require_all_durable);

    ~DbImpl() override;

    Result<Pinned> get(Slice key) override;
    Result<std::vector<uint8_t>> get_copy(Slice key) override;

    Status put(Slice key, Slice value) override;
    Status remove(Slice key) override;
    Status write(WriteBatch& batch) override;

    std::unique_ptr<Iterator> iterator() override;
    std::unique_ptr<Iterator> iterator(Slice lower_inclusive, Slice upper_exclusive) override;
    std::unique_ptr<Iterator> iterator(Slice lower_inclusive) override;
    std::unique_ptr<Iterator> prefix_iterator(Slice prefix) override;

    Status flush() override;
    Status compact_level(int level) override;

    /// ARCHITECTURE.md "Invariants and sanitizers" — the continuous invariant checks, exposed so a test can demand
    /// them at any point. Run automatically after every flush when
    /// `Options::paranoid_checks` is set.
    /// Verifies the continuous invariants. `which`, when given, names the
    /// one that failed — see `Invariant` for why that is not a nicety.
    Status check_invariants(Invariant* which = nullptr) const;

#ifdef ELYSIUMKV_PARANOID
    /// ARCHITECTURE.md "Negative controls" — constructs a state that violates exactly one invariant, so the
    /// check that looks for it can be shown to fire. These states are
    /// unreachable while the engine is correct, which is precisely why there is
    /// no other way to exercise the checks. Compiled only where the checks are.
    Status break_invariant_for_test(Invariant which);
#endif

    Stats stats() const override;

    /// Runs every compaction the picker offers, on the calling thread, and waits
    /// for none. Used by the tests and by `BackgroundMode::Inline`.
    Status compact_until_quiet();
    /// ARCHITECTURE.md "Versions are immutable snapshots" — obsolete-object GC runs after each swap *and each release*. A
    /// released iterator is a release, and nothing else would notice it.
    void collect_obsolete() { versions_->collect_obsolete(); }

    // --- internals a test may need that the public Stats deliberately omits
    uint64_t flush_count() const { return flushes_.load(); }
    uint64_t pending_deletions() const { return versions_->pending_deletions(); }
    /// ARCHITECTURE.md "Open and recovery" names the file in the failure; this carries the text until the C ABI
    /// gives it a home in `elysiumkv_last_error` (ARCHITECTURE.md "The ABI boundary").
    const std::string& last_error() const { return last_error_; }
    std::shared_ptr<const Version> current_version() const { return versions_->current(); }
    /// Drops every open reader, so the next read goes back to the store. Lets a
    /// test simulate a file vanishing under a running instance.
    void evict_readers() { readers_.clear(); }
    const ResolvedLevels& levels() const { return config_; }
    void mark_recovery_complete() override { requires_recovery_.store(false); }

private:
    DbImpl(const Options& options, ResolvedLevels config, ResolvedTiers tiers);

    Status recover();
    void start_background();

    // --- write path
    Status maybe_freeze_memtable(bool force);

    /// Whether the active memtable should be flushed: `force`, or it has reached
    /// `memtable_bytes`, or it has been open longer than `flush_interval`. Size and age are
    /// alternatives, not a conjunction. **Call with `mem_mutex_` held** — it reads `mem_`.
    bool memtable_flush_due(bool force) const;

    /// Wake-up period for the flush thread's age check. Zero-cost when no interval is set:
    /// the thread waits without a timeout in that case.
    std::chrono::milliseconds flush_poll_interval() const;
    /// ARCHITECTURE.md "The differential oracle" — **exactly one unit of flush work**, returning whether it did any.
    /// The flush thread is a loop calling this; synchronous mode calls the same
    /// function inline. The modes differ only in who calls the work, never in
    /// what the work does: a separate path "just for tests" would make the
    /// gating suite validate fiction.
    bool run_one_flush(Status& status);
    /// The synchronous driver: freeze here (the writer's decision, not
    /// background work), then run the same flush and compaction functions the
    /// threads run.
    Status freeze_and_flush_inline(bool force);
    bool inline_mode() const { return options_.background == BackgroundMode::Inline; }
    void background_flush_loop();
    Status flush_memtable(const std::shared_ptr<SkiplistMemtable>& memtable);
    /// ARCHITECTURE.md "Compaction" and "Migration between tiers" — blocks (or reports `Status::Stalled`) while a level is past its
    /// `stop_at` or its `stall_age`. The age valve is what turns the exposure
    /// bound into a guarantee, so it is not configurable off.
    Status throttle_writes();

    // --- compaction
    /// One thread drives both migration and compaction (ARCHITECTURE.md "Migration between tiers"): migration off a
    /// transient tier first, then compaction.
    void background_compaction_loop();
    void schedule_compaction();
    /// ARCHITECTURE.md "The differential oracle" — the compaction counterpart of `run_one_flush`, with the same
    /// contract: performs exactly one compaction, reports whether it did work.
    bool run_one_compaction(Status& status);
    Status run_compaction(const Compaction& compaction);
    /// ARCHITECTURE.md "Migration between tiers" — the third kind of background work, and structurally the simplest:
    /// it moves bytes without interpreting them. Same `run_one()` contract.
    bool run_one_migration(Status& status);
    Status run_migration(const Migration& migration);
    /// An L0 file cannot be migrated without reordering L0's positional recency
    /// (ARCHITECTURE.md "Positional recency"), so it leaves its tier by being compacted into L1 instead. Returns
    /// false when no L0 file needs to move.
    bool compact_l0_file_off_its_tier(Status& status);
    Status write_compaction_outputs(const Compaction& compaction,
                                    std::vector<FileMetadata>& outputs);
    /// ARCHITECTURE.md "A tier is not a level" — where a finished file belongs. Evaluated *after* the bytes exist,
    /// because `max_file_bytes` is one of its inputs.
    const Tier& tier_for(uint64_t min_write_time_ms, uint64_t file_bytes) const;
    /// The store named in a file's metadata, or nullptr if the configuration no
    /// longer has it.
    BlobStore* store_for(const std::string& store_id) const;

    // --- durability (ARCHITECTURE.md "A tier is not a level", ARCHITECTURE.md "Open and recovery")
    /// One `list` per distinct store, never a `get` per file. Missing from a
    /// `Durable` store is corruption; missing from a `Transient` store discards
    /// every file on that store; any `Io` is neither, and must not discard.
    Status verify_stores_and_discard();
    /// Objects no version references: the residue of a compaction or flush that
    /// died before its edit was durable.
    void collect_orphans(const std::map<std::string, std::vector<std::string>>& listings);
    Status fail_terminal(Status status, std::string detail);
    /// ARCHITECTURE.md "A process-wide memory budget" — sheds memory when the shared budget is exceeded, in the order the spec
    /// gives: evict the block cache, then flush memtables, then let the caller stall.
    /// Returns whether the budget is still over after shedding.
    bool shed_if_over_budget();
    std::shared_ptr<SkiplistMemtable> new_memtable();
    /// Writes a new SST, renumbering if the name is taken (ARCHITECTURE.md "Immutable named objects"). Returns the file number
    /// actually used, which the caller records in `FileMetadata`.
    Result<uint64_t> write_new_sst(BlobStore& store, Slice bytes);

    // --- read path
    Result<std::shared_ptr<SstReader>> reader_for(const FileMetadata& file);
    void forget_reader(uint64_t file_number);
    /// Returns the files it could not delete, which stay pending (ARCHITECTURE.md "Versions are immutable snapshots").
    std::vector<FileMetadata> delete_obsolete(const std::vector<FileMetadata>& files);
    /// Every source in recency order: live memtable, frozen memtable, L0 by
    /// descending file number, then each deeper level (ARCHITECTURE.md "Positional recency").
    std::unique_ptr<Iterator> make_iterator(Slice lower, Slice upper, bool has_upper);

    const ResolvedLevel& level_config(int level) const;
    uint64_t now_ms() const { return options_.clock(); }

    Options options_;
    ResolvedLevels config_;
    ResolvedTiers tiers_;
    std::shared_ptr<BlockCache> block_cache_;
    std::unique_ptr<VersionSet> versions_;

    mutable std::mutex mem_mutex_;
    std::condition_variable flush_scheduled_;
    std::condition_variable flush_finished_;
    std::shared_ptr<SkiplistMemtable> mem_;
    std::shared_ptr<SkiplistMemtable> imm_;
    Status bg_error_ = Status::Ok;
    bool shutting_down_ = false;
    std::thread flush_thread_;

    /// ARCHITECTURE.md "Compaction" has one compaction thread, but `compact_level()` (ARCHITECTURE.md "Absence is an answer, not an error") lets a caller
    /// drive compaction too — and two compactions running at once each compute
    /// their output from a version the other is mutating, which loses writes.
    /// Held across pick-and-run, so a compaction is atomic with respect to any
    /// other compaction. A concurrent *flush* is safe: it only adds an L0 file,
    /// never removes one, so a compaction's inputs stay valid.
    std::mutex compaction_work_mutex_;

    mutable std::mutex compaction_mutex_;
    std::condition_variable compaction_scheduled_;
    std::condition_variable compaction_finished_;
    bool compaction_pending_ = false;
    bool shutting_down_compaction_ = false;
    std::thread compaction_thread_;

    /// Keyed by file number, which ARCHITECTURE.md "The manifest is snapshots plus edits" guarantees is never reused — including
    /// across a migration, which renumbers rather than moving a number between
    /// stores. Bounded by bytes and reported to the shared budget; see
    /// `SstReaderCache` for why unbounded was expensive and why eviction is safe.
    SstReaderCache readers_;

    std::atomic<bool> requires_recovery_{false};
    std::atomic<uint64_t> flushes_{0};
    std::atomic<uint64_t> stalls_{0};
    std::atomic<uint64_t> budget_sheds_{0};
    std::atomic<uint64_t> stalled_total_ms_{0};
    /// Written by step 7's compactor; reported from now so the shape of Stats
    /// does not change when it lands.
    std::atomic<uint64_t> compactions_{0};
    std::atomic<uint64_t> compaction_bytes_read_{0};
    std::atomic<uint64_t> compaction_bytes_written_{0};
    std::atomic<uint64_t> migrations_{0};
    std::atomic<uint64_t> migration_bytes_{0};
    mutable std::atomic<uint64_t> pins_outstanding_{0};
    std::vector<std::string> discarded_stores_;
    uint64_t discarded_files_ = 0;
    std::string last_error_;
    /// Set when a file vanishes from under a live Version (ARCHITECTURE.md "A tier is not a level"): repair cannot
    /// run alongside live iterators, so the instance is finished.
    std::atomic<bool> unusable_{false};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DB_DB_IMPL_HPP
