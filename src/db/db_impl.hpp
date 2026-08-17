#ifndef ELYSIUMKV_DB_DB_IMPL_HPP
#define ELYSIUMKV_DB_DB_IMPL_HPP

#include "cache/sharded_lru.hpp"
#include "blob/tier.hpp"
#include "compact/migrator.hpp"
#include "compact/picker.hpp"
#include "db/level_config.hpp"
#include "db/lock_audit.hpp"
#include "memtable/skiplist_memtable.hpp"
#include "sst/sst_reader.hpp"
#include "sst/sst_reader_cache.hpp"
#include "elysiumkv/db.hpp"
#include "version/version_set.hpp"

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
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

/// ARCHITECTURE.md "Negative controls" — the constituents of "placement converges when idle", so a
/// test can drain three of them and assert the invariant still fails on the fourth.
///
/// One control that removed the whole maintenance timer would be satisfied by any *partial*
/// implementation, and a partial fix is the most likely way this regresses — the four are separate
/// code paths that happen to share a trigger. So there is one enumerator per constituent, for the
/// same reason `Invariant` names which check failed rather than reporting one bundled status.
enum class MaintenanceTask : uint32_t {
    TransientRescue = 1u << 0,      ///< move a file off a `Transient` tier — durability
    CapacityEviction = 1u << 1,     ///< a tier is over `max_bytes` — cost
    DurableAgeMigration = 1u << 2,  ///< colder placement between durable tiers — cost, starvable
    LevelZeroEscape = 1u << 3,      ///< an L0 file compacted off a tier its age has outgrown
};

class WindowedBlobStore;

class DbImpl final : public DB {
public:
    static Result<OpenResult> open(const Options& options, bool require_all_durable,
                                   bool read_only = false);

    ~DbImpl() override;

    Result<Pinned> get(Slice key) override;
    Result<std::vector<uint8_t>> get_copy(Slice key) override;

    Status put(Slice key, Slice value) override;
    Status remove(Slice key) override;
    Status write(WriteBatch& batch) override;
    Status truncate_below(Slice key) override;
    Status delete_range(Slice lower, Slice upper) override;
    Status check_below_truncation(Slice key) const;

    std::unique_ptr<Iterator> iterator() override;
    std::unique_ptr<Iterator> iterator(Slice lower_inclusive, Slice upper_exclusive) override;
    std::unique_ptr<Iterator> iterator(Slice lower_inclusive) override;
    std::unique_ptr<Iterator> prefix_iterator(Slice prefix) override;
    std::unique_ptr<Iterator> reverse_iterator() override;
    std::unique_ptr<Iterator> reverse_iterator(Slice lower_inclusive,
                                               Slice upper_exclusive) override;
    std::unique_ptr<Iterator> reverse_iterator(Slice lower_inclusive) override;
    std::unique_ptr<Iterator> reverse_prefix_iterator(Slice prefix) override;

    Status flush() override;
    void abandon_unflushed() override { abandoned_.store(true, std::memory_order_relaxed); }
    Status compact_level(int level) override;
    Status refresh() override;

    Status set_watermark(uint64_t position) override;
    std::optional<uint64_t> recovered_watermark() const override { return recovered_watermark_; }

    /// ARCHITECTURE.md "Invariants and sanitizers" — the continuous invariant checks, exposed so a test can demand
    /// them at any point. Run automatically after every flush when
    /// `Options::paranoid_checks` is set.
    /// Verifies the continuous invariants. `which`, when given, names the
    /// one that failed — see `Invariant` for why that is not a nicety.
    Status check_invariants(Invariant* which = nullptr) const;

#ifdef ELYSIUMKV_PARANOID
    /// ARCHITECTURE.md "Negative controls" — claims the deleting-task slot without releasing it, which
    /// is what a second deleting worker looks like to the guard. The next compaction, migration or
    /// eviction must then abort. Without this the assertion is one nobody has ever seen fire, and
    /// CONTRIBUTING.md is explicit that such a check has not been tested.
    void claim_deleting_task_for_test() { deleting_task_in_flight_.store(true); }

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
    /// Runs one sweep on the calling thread, so a test can drive it without waiting for the
    /// interval. The sweep is idempotent and its patience comes from the clock, not from how often
    /// it is called.
    Status sweep_orphans_for_test() { return sweep_orphans(); }
    /// One reclaim pass on the calling thread: files a truncation point, a range tombstone or the
    /// TTL has made dead, dropped by manifest edit alone.
    bool reclaim_dead_files_for_test(Status& status) { return reclaim_dead_files(status); }
    /// One reconcile pass on the calling thread, so a test can drive the coordinator's decision
    /// without waiting for a tick. `force_full` bypasses the O(1) gate, which is what the
    /// periodic bypass does every minute in the running loop.
    void reconcile_for_test(bool force_full) { reconcile(force_full); }
    /// ARCHITECTURE.md "Negative controls" — drops wake notifications while **keeping** epoch
    /// invalidation, so a test asserts the property that actually holds: a wake is an
    /// optimisation, an invalidation is not. Suppressing both would assert a guarantee this
    /// design does not make, and would leave the test ambiguous about which one it claimed.
    void suppress_maintenance_wakes_for_test(bool on) { suppress_maintenance_wakes_.store(on); }
    /// Compactions the tombstone-density trigger won, rather than a size ratio.
    ///
    /// A test hook rather than a `Stats` field: it exists so a configuration claiming to exercise
    /// the trigger can show that it did, and adding it to the stats buffer would change a
    /// compatibility contract for the benefit of a test.
    uint64_t density_compactions_for_test() const {
        return density_compactions_.load(std::memory_order_relaxed);
    }
    /// ARCHITECTURE.md "Negative controls" — freezes the epoch, which is what a predicate whose
    /// invalidating transitions were never wired up looks like from the gate's side. The periodic
    /// bypass is then the only thing that can find the work, which is the bound this exists to
    /// demonstrate.
    /// Freezes at **the epoch as it is now**, not at zero. Freezing at zero would itself be a
    /// change the gate notices, so it would open once more before settling — and that one extra
    /// opening is enough to drain the work a negative control says cannot be drained, or to let a
    /// positive control pass without the mechanism it is supposed to be testing.
    void pin_maintenance_epoch_for_test(bool on) {
        pinned_maintenance_epoch_.store(on ? static_cast<int64_t>(live_maintenance_epoch()) : -1);
    }
    /// Whether the coordinator has caught up with the current epoch, i.e. its gate is now closed on
    /// state. **A test waits on this rather than sleeping**: "long enough for a few ticks" is a
    /// guess about a machine, and it was wrong on a loaded CI runner.
    bool maintenance_gate_closed_for_test() const {
        return last_reconciled_epoch_.load() == maintenance_epoch();
    }
    /// ARCHITECTURE.md "Negative controls" — pins the published stall flag, so what the write path
    /// reads and what the clock says can be made to disagree. That disagreement is the only way to
    /// tell a write path that *reads* the flag from one that *computes* the predicate, and it works
    /// in both directions:
    ///
    /// - pinned **true** with nothing actually past `stall_age`: a reader stalls, a computer does
    ///   not;
    /// - pinned **false** with the tier far past `stall_age`: a reader proceeds, a computer stalls.
    ///
    /// Neither involves timing, which matters — arranging for the flag to become true *naturally*
    /// races the rescue that clears it again, and that race is what made an earlier version of this
    /// test fail on CI while passing locally.
    void pin_transient_stall_for_test(std::optional<bool> state) {
        pinned_transient_stall_.store(state.has_value() ? (*state ? 1 : 0) : -1);
    }
    /// ARCHITECTURE.md "Negative controls" — **the engine as it was**: the coordinator's clock plays
    /// no part, so neither a time transition nor the periodic bypass opens the gate and only an
    /// epoch change — which means a write — can cause work. That is precisely the defect this
    /// design removes, and a control that merely lengthened the interval is not equivalent, because
    /// any notification wakes the coordinator early and it then reconciles for real.
    void suppress_timed_maintenance_for_test(bool on) { suppress_timed_maintenance_.store(on); }
    /// ARCHITECTURE.md "Negative controls" — refuses the named tasks, so the convergence invariant
    /// can be shown to fail on each constituent separately. A bitwise-or of `MaintenanceTask`.
    void suppress_maintenance_tasks_for_test(uint32_t mask) { suppressed_tasks_.store(mask); }
    bool task_suppressed(MaintenanceTask task) const {
        return (suppressed_tasks_.load() & static_cast<uint32_t>(task)) != 0;
    }
    /// The coordinator's gate epoch. A test watches it to tell "the store has gone quiet" from
    /// "the store is still settling", which is not the same as "no writes are arriving": an
    /// executor that did work invalidates the epoch too.
    uint64_t maintenance_epoch_for_test() const { return maintenance_epoch(); }
    /// The published transient-tier stall state. One evaluator: the coordinator computes it, the
    /// write path reads it. See `publish_transient_stall`.
    ///
    /// **The test pin is applied here rather than written into the flag**, and that is a race fix
    /// rather than a tidying. Writing it meant `publish_transient_stall` — which reads the pin,
    /// then scans every file, then stores — could begin before the pin was set and finish after,
    /// putting the computed value back over it. The window is the length of a scan, which under a
    /// sanitizer is long enough to lose; it failed on Linux under tsan. Deciding at the point of
    /// use removes the window instead of narrowing it, and leaves exactly one place that knows the
    /// pin exists.
    bool transient_stalled() const {
        const int pinned = pinned_transient_stall_.load();
        if (pinned >= 0) return pinned != 0;
        return transient_stalled_.load();
    }
    void mark_recovery_complete() override { requires_recovery_.store(false); }

private:
    DbImpl(const Options& options, ResolvedLevels config, ResolvedTiers tiers);

    Status recover();
    /// Derives `recovered_watermark_` from what survived recovery and seeds the live memtable's
    /// lower bound with it.
    void adopt_recovered_watermark();
    void start_background();

    // --- write path
    /// Moves the live memtable to the immutable slot and stamps when writes stopped arriving.
    void seal_memtable();
    Status maybe_freeze_memtable(bool force);

    /// Whether the active memtable should be flushed: `force`, or it has reached
    /// `memtable_bytes`, or it has been open longer than `flush_interval`. Size and age are
    /// alternatives, not a conjunction. **Call with `mem_mutex_` held** — it reads `mem_`.
    bool memtable_flush_due(bool force) const;

    // --- maintenance scheduling
    //
    // **Scheduling pulls; it does not wait to be pushed.** Every background policy is a
    // predicate over current state plus the clock, evaluated by one coordinator on a fixed
    // tick — not a message a caller remembered to send. Three age-bounded behaviours in this
    // engine shipped with a write as their only trigger (the flush interval, age-driven
    // migration, the L0 tier escape), and the failure was identical each time: the only thing
    // that could have noticed was the thing that had stopped happening.
    //
    // What the restructure does *not* remove is the need to declare invalidation. The O(1)
    // gate below skips evaluation when nothing relevant has changed, so a predicate whose
    // invalidating transitions are not wired into `maintenance_epoch()` can be hidden by it.
    // Wake notifications are optional — a missed nudge costs latency. Epoch invalidation is
    // mandatory. The periodic gate bypass bounds the damage from getting that wrong to *late*
    // rather than *never*, which is why it exists.

    /// The coordinator: ticks on `Options::maintenance_interval`, evaluates the predicate table
    /// and dispatches. Performs no long-running work itself, so predicate evaluation is never
    /// blocked by work it dispatched.
    void maintenance_loop();
    /// One reconcile pass. `force_full` skips the gate.
    void reconcile(bool force_full);
    /// The epoch ignoring any test pin. Split out so pinning can capture the live value.
    uint64_t live_maintenance_epoch() const;
    /// The gate's epoch: every predicate-relevant change that is *not* the passage of time.
    /// Version installs dominate it; `maintenance_bumps_` carries the rest — memtable rotation,
    /// an executor that did work, a change in storage or recovery state.
    uint64_t maintenance_epoch() const;
    /// Bumps the epoch and nudges the coordinator. The bump is the part that matters.
    void invalidate_maintenance();
    /// The earliest wall-clock time at which a *time-driven* predicate could change, or
    /// `UINT64_MAX` when none can. Only transitions strictly in the future count: one already
    /// past has been folded into the reconcile that observed it, and returning it would hold the
    /// gate open and re-scan every tick.
    ///
    /// The memtable's flush deadline is deliberately absent — the whole flush predicate is
    /// O(1) and is evaluated *ahead* of the gate on every tick, which covers both its size and
    /// its age trigger.
    uint64_t next_time_transition(const Version& version, uint64_t now) const;
    /// Evaluates "is a transient tier past `stall_age`?" and publishes the answer. **One
    /// evaluator:** if the write path computed this too, the same predicate would exist in two
    /// places and could diverge — which is how a valve ends up engaged by one and not the other.
    void publish_transient_stall(const Version& version, uint64_t now);
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
    Result<std::shared_ptr<SstReader>> compaction_reader_for(
        const FileMetadata& file, std::vector<std::unique_ptr<WindowedBlobStore>>& windows);
    /// Nudges the maintenance executor to look now. Demoted by the coordinator from *the*
    /// trigger to an optimisation — it is what keeps a stalled writer from waiting a full tick —
    /// and it invalidates the epoch, which is the half that is not optional.
    void schedule_compaction();
    /// The dispatch alone, with no epoch bump and not suppressible. The coordinator's pull path.
    void dispatch_maintenance();
    /// Declares that predicate-relevant state changed. See its definition for why the epoch bump
    /// is the mandatory half and the wake is not.
    void note_maintenance_state_changed();
    /// ARCHITECTURE.md "The differential oracle" — the compaction counterpart of `run_one_flush`, with the same
    /// contract: performs exactly one compaction, reports whether it did work.
    /// Drops files the truncation point has emptied. Same `run_one()` contract as the others.
    bool reclaim_dead_files(Status& status);
    /// Defined below, with the rest of the diagnostics.
    class DeferredLine;
    bool run_one_compaction(Status& status);
    Status run_compaction(const Compaction& compaction, DeferredLine& line);
    /// ARCHITECTURE.md "Migration between tiers" — the third kind of background work, and structurally the simplest:
    /// it moves bytes without interpreting them. Same `run_one()` contract.
    bool run_one_migration(Status& status);
    Status run_migration(const Migration& migration, DeferredLine& line);
    /// An L0 file cannot be migrated without reordering L0's positional recency
    /// (ARCHITECTURE.md "Positional recency"), so it leaves its tier by being compacted into L1 instead. Returns
    /// false when no L0 file needs to move.
    bool compact_l0_file_off_its_tier(Status& status);
    Status write_compaction_outputs(const Compaction& compaction,
                                    std::vector<FileMetadata>& outputs);
    /// ARCHITECTURE.md "A tier is not a level" — where a file belongs, from its age alone. It no longer
    /// depends on the file's size, so it no longer has to be evaluated after the bytes exist; the
    /// call sites simply have not been moved earlier, and nothing requires them to be.
    const Tier& tier_for(uint64_t min_write_time_ms) const;
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

    /// ARCHITECTURE.md "Immutable named objects" — lists every store and deletes objects that have
    /// been **continuously unreferenced for `orphan_retention`**.
    ///
    /// A single instantaneous observation cannot tell a dead writer's residue from a live writer's
    /// just-committed file, which is why deleting on one was removed. A sustained observation can,
    /// and the manifest re-read below is what makes it sustained rather than merely repeated: an
    /// object whose edit has since committed is referenced now and drops out of the set.
    ///
    /// Skips what `pending_deletions` already holds — those have an exact unreferenced-since time
    /// and `obsolete_retention` of their own, and letting the sweep at them would undercut the
    /// reader window.
    Status sweep_orphans();
    /// Whether the writer has rolled past the generation this instance holds — the discriminator
    /// between "my version is too old" and "this object is genuinely lost".
    bool manifest_has_advanced() const;
    /// Turns a read failure into `Status::Stale` when this read-only instance is behind the
    /// writer's retention window rather than looking at damaged data.
    Status classify_read_failure(Status status) const;
    /// First time each object was seen unreferenced, per store. In memory, so a restart resets it:
    /// a writer that restarts more often than `orphan_retention` never collects. That is a leak
    /// rather than a hazard — it errs toward keeping bytes — and the alternative was growing
    /// `BlobStore` with a modification time, which every binding-supplied implementation would
    /// have to serve.
    std::map<std::string, std::map<std::string, uint64_t>> orphan_first_seen_;
    /// Written by the maintenance executor when it sweeps, read by the coordinator when it
    /// computes the next time-driven deadline. **Two threads, so atomic** — it is a deadline, not a
    /// lock, and a stale read costs at most one extra tick.
    std::atomic<uint64_t> next_sweep_ms_{0};
    /// Serialises `sweep_orphans`, which the maintenance executor and a test may both call.
    std::mutex sweep_mutex_;
    Status fail_terminal(Status status, std::string detail);
    /// ARCHITECTURE.md "A process-wide memory budget" — sheds memory when the shared budget is exceeded, in that
    /// order: evict the block cache, then flush memtables, then let the caller stall.
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
    std::unique_ptr<Iterator> make_iterator(Slice lower, Slice upper, bool has_upper,
                                            bool reverse);

    const ResolvedLevel& level_config(int level) const;
    uint64_t now_ms() const { return options_.clock(); }

    // --- diagnostics (docs/logger-spec.md)
    bool logger_enabled(LogLevel level) const {
        return options_.logger != nullptr && options_.logger->write != nullptr &&
               level >= options_.min_log_level;
    }
    /// Asserts no engine lock is held, then hands the line to the embedder's sink.
    void log_emit(LogLevel level, LogEvent event, const std::string& message) const;
    /// Reports a manifest generation roll once, whoever notices it first. The roll itself happens
    /// inside `VersionSet` on whichever edit crosses the threshold; observing it from here is what
    /// keeps the logger out of a class whose locks this file cannot audit.
    void note_generation_roll() const;
    mutable std::atomic<uint64_t> last_seen_generation_{0};
    /// Formats only when the level is enabled, so a disabled logger costs one comparison.
    template <typename... Args>
    void log_event(LogLevel level, LogEvent event, Args&&... args) const {
        if (!logger_enabled(level)) return;
        std::ostringstream message;
        (message << ... << args);
        log_emit(level, event, message.str());
    }

    /// A line composed inside a critical section and emitted once it is left. **Declare it before
    /// the lock**: destruction runs in reverse, so it outlives the guard and the sink sees no lock.
    class DeferredLine {
    public:
        explicit DeferredLine(const DbImpl* self) : self_(self) {}
        DeferredLine(const DeferredLine&) = delete;
        DeferredLine& operator=(const DeferredLine&) = delete;
        ~DeferredLine() {
            // A destructor is noexcept; a throwing sink must not take the process with it.
            try {
                if (pending_) self_->log_emit(level_, event_, message_);
            } catch (...) {
            }
        }

        template <typename... Args>
        void set(LogLevel level, LogEvent event, Args&&... args) {
            if (!self_->logger_enabled(level)) return;
            std::ostringstream message;
            (message << ... << args);
            message_ = message.str();
            level_ = level;
            event_ = event;
            pending_ = true;
        }

    private:
        const DbImpl* self_;
        bool pending_ = false;
        LogLevel level_ = LogLevel::Info;
        LogEvent event_ = LogEvent::FlushComplete;
        std::string message_;
    };

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

    /// The maintenance coordinator's own thread and tick. Separate from `mem_mutex_` and
    /// `compaction_mutex_` so a tick is never delayed by either executor.
    std::mutex maintenance_mutex_;
    std::condition_variable maintenance_tick_;
    bool shutting_down_maintenance_ = false;
    std::thread maintenance_thread_;
    /// Non-clock predicate invalidations that are not version installs.
    std::atomic<uint64_t> maintenance_bumps_{0};
    /// Written only by `reconcile`, which is single-threaded apart from the synchronous pass `open`
    /// performs before any caller can reach the instance. Atomic because a test reads it to tell
    /// "the coordinator has caught up" from "it has not looked yet", and reading a plain member
    /// across threads is a race whatever the intended ordering.
    std::atomic<uint64_t> last_reconciled_epoch_{0};
    uint64_t next_time_transition_ms_ = 0;
    uint64_t reconcile_ticks_ = 0;
    std::atomic<bool> transient_stalled_{false};
    std::atomic<bool> suppress_maintenance_wakes_{false};
    std::atomic<uint64_t> density_compactions_{0};
    /// -1 when not pinned; otherwise the frozen epoch. See `pin_maintenance_epoch_for_test`.
    std::atomic<int64_t> pinned_maintenance_epoch_{-1};
    std::atomic<bool> suppress_timed_maintenance_{false};
    /// Tri-state: -1 not pinned, 0 pinned clear, 1 pinned set. **Atomic, and an `optional<bool>`
    /// here was a data race** — the coordinator thread reads this while a test thread writes it, and
    /// "the test writes it before the writes it cares about" is an argument about *intent*, not
    /// about synchronisation. TSAN reported it intermittently, which is what an unsynchronised
    /// access looks like when the interleaving usually happens to be benign.
    std::atomic<int8_t> pinned_transient_stall_{-1};
    std::atomic<uint32_t> suppressed_tasks_{0};
#ifdef ELYSIUMKV_PARANOID
    /// ARCHITECTURE.md "Negative controls" — at most one *deleting* task may be in flight, because
    /// `VersionSet::apply` does not validate that the files an edit removes are still live. The
    /// constraint is narrower than "one version-mutating task": flush overlaps compaction
    /// legally and must keep doing so, since flush only adds. Cannot fire today — those three
    /// tasks share an executor — and fires on the first test run after someone adds a second
    /// deleting worker, which is the difference it exists for.
    std::atomic<bool> deleting_task_in_flight_{false};
#endif

    /// The last watermark the embedder established, guarded by `mem_mutex_`. One source of
    /// truth: a new memtable inherits it as its lower bound, and `set_watermark` compares
    /// against it to refuse a decrease.
    std::optional<uint64_t> established_watermark_;
    /// Fixed by recovery and never written again, so `recovered_watermark()` cannot change
    /// meaning after the first write. The live frontier is `Stats::durable_watermark`.
    std::optional<uint64_t> recovered_watermark_;

    std::atomic<bool> requires_recovery_{false};
    std::atomic<uint64_t> flushes_{0};
    std::atomic<uint64_t> stalls_{0};
    std::atomic<uint64_t> background_failures_{0};
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
    /// The inputs to the resume-position rule, accumulated while the files are still in a version.
    /// A single value rather than a bound plus a "was anything absent" flag, because the decision
    /// turns on whether *anything was discarded* and that must not be inferable from the bounds.
    RecoveryWatermark recovery_watermark_;
    std::string last_error_;
    /// Set when a file vanishes from under a live Version (ARCHITECTURE.md "A tier is not a level"): repair cannot
    /// run alongside live iterators, so the instance is finished.
    std::atomic<bool> unusable_{false};
    /// **No manifest write of any kind**, no background threads, no reclamation, no CAS.
    bool read_only_ = false;
    /// Set by `abandon_unflushed`; suppresses the destructor's best-effort flush.
    std::atomic<bool> abandoned_{false};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DB_DB_IMPL_HPP
