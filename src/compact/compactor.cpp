/* Compaction and migration execution, split out of `db_impl.cpp` — which had
 * reached 1400 lines and forty methods spanning open, the write path, the read
 * path, stats, background work and store verification. ARCHITECTURE.md "Dependencies and artifacts" names this file; it
 * simply never existed, and both compaction bugs found in this codebase hid in
 * the crowd.
 *
 * These are still `DbImpl` members. That is deliberate and worth being honest
 * about: the win here is navigability, not decoupling. Every one of them reaches
 * into `versions_`, `options_`, `config_`, `tiers_`, the reader cache and the
 * counters, so a genuine `Compactor` object would need most of `DbImpl` handed
 * to it by reference and would buy nothing today.
 */

#include "db/db_impl.hpp"

#include "compact/merging_iterator.hpp"
#include "compact/migrator.hpp"
#include "compact/picker.hpp"
#include "sst/sst_writer.hpp"

#include <algorithm>
#include <map>
#include <optional>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <set>
#include <utility>

namespace elysiumkv {

#ifdef ELYSIUMKV_PARANOID
namespace {

/// ARCHITECTURE.md "Negative controls" — asserts the constraint that makes the current design safe:
/// **at most one task performing deletions is in flight at a time.**
///
/// It is narrower than "one version-mutating task", and getting that wrong would produce an
/// assertion that fires immediately: flush and a compaction *do* overlap — flush adds an L0 file
/// while the compaction deletes at L1 — and that is legal precisely because flush deletes
/// nothing, so its edit cannot contend with a delete-set chosen from an older version snapshot.
/// Compaction, migration and capacity eviction all pick files from a snapshot and
/// `VersionSet::apply` does not check they are still live, so two of *those* running at once is
/// a double delete or a compaction reading a file a migration already unlinked.
///
/// Cannot fire today — those three share one executor. Fires on the first test run after
/// someone adds a second deleting worker, which converts "found out in production" into a failed
/// assertion. Compiled only where the continuous checks are.
class DeletingTaskGuard {
public:
    explicit DeletingTaskGuard(std::atomic<bool>& flag) : flag_(flag) {
        if (flag_.exchange(true)) {
            // **Not `assert`.** `ELYSIUMKV_PARANOID` is on in the sanitizer presets too, and those
            // build as RelWithDebInfo — which defines `NDEBUG`, so an `assert` here would be inert
            // in two of the three builds that are supposed to be carrying this check. A check that
            // silently does not run in most of the configurations it is enabled in is the failure
            // mode this whole file argues against.
            std::fprintf(stderr,
                         "elysiumkv: two deleting tasks in flight. VersionSet::apply does not "
                         "validate that a delete-set is still live, so a second deleting worker "
                         "needs file-level exclusion or optimistic validation first.\n");
            std::abort();
        }
    }
    ~DeletingTaskGuard() { flag_.store(false); }

private:
    std::atomic<bool>& flag_;
};

}  // namespace
#  define ELYSIUMKV_CLAIM_DELETING_TASK() DeletingTaskGuard elysiumkv_deleting_guard(deleting_task_in_flight_)
#else
#  define ELYSIUMKV_CLAIM_DELETING_TASK() do { } while (false)
#endif


// --- compaction (ARCHITECTURE.md "Compaction") -----------------------------------------------------------

/// Hands the maintenance executor work to look for. Called by the coordinator when its gate
/// opens, which is the pull path, and by the write path as an optimisation.
void DbImpl::dispatch_maintenance() {
    if (inline_mode()) return;
    {
        std::lock_guard<std::mutex> lock(compaction_mutex_);
        compaction_pending_ = true;
    }
    compaction_scheduled_.notify_one();
}

/// The write path's nudge, and what keeps the coordinator's gate honest.
///
/// **The two halves are not equally important.** The dispatch is an optimisation — it is what
/// keeps a stalled writer from waiting a full tick — and a test may suppress it to prove the
/// coordinator alone is sufficient. The **epoch bump is mandatory**: the gate skips evaluation
/// when the epoch is unchanged, so a transition that never bumps it stays hidden until the
/// periodic bypass. Every new maintenance predicate must either be cheap enough to evaluate ahead
/// of the gate, or declare every transition that invalidates it.
void DbImpl::schedule_compaction() {
    invalidate_maintenance();
    if (suppress_maintenance_wakes_.load()) return;
    dispatch_maintenance();
}

void DbImpl::note_maintenance_state_changed() { invalidate_maintenance(); }

/// Unlinks every file whose keys all sit below the truncation point. No rewrite, no read: the
/// files leave the version in one edit and reach the object store through the ordinary
/// obsolete-object path, which is what keeps an open reader safe from them.
bool DbImpl::reclaim_truncated_files(Status& status) {
    auto version = versions_->current();
    std::vector<FileMetadata> dead = version->files_entirely_truncated();
    // Files a newer range tombstone covers whole go the same way and for the same reason: the bytes
    // are already unreadable, so rewriting them in a compaction would be work done to produce
    // nothing. One edit, and at most one block read to authorise it.
    //
    // **The manifest shortlists; the tombstones decide.** What it records per file is the *hull* of
    // that file's ranges, which is the range itself when there is one and a bounding interval with
    // unknown gaps when there are more — so a hull can show a file is not covered but never that it
    // is. Rather than give up on the second case, which is what a file whose tombstones have been
    // compacted together looks like, the candidates the hull admits are settled by reading the
    // cover's range block. Reads are per cover rather than per candidate, and only for covers the
    // hull already made plausible.
    std::map<uint64_t, std::vector<RangeTombstone>> read_covers;
    for (const Version::RangeDropCandidate& candidate : version->range_drop_candidates()) {
        if (candidate.exact) {
            dead.push_back(candidate.file);
            continue;
        }
        auto found = read_covers.find(candidate.cover.file_number);
        if (found == read_covers.end()) {
            auto reader = reader_for(candidate.cover);
            if (!reader) continue;   // unreadable is not evidence; leave the file alone
            auto ranges = (*reader)->range_tombstones();
            if (!ranges) continue;
            found = read_covers.emplace(candidate.cover.file_number, std::move(*ranges)).first;
        }
        // Disjoint and non-adjacent after merging, so a union of several can never span a gap: one
        // range containing the file's whole span is both necessary and sufficient.
        const Slice smallest = Slice::from(candidate.file.smallest_key);
        const Slice largest = Slice::from(candidate.file.largest_key);
        for (const RangeTombstone& range : found->second) {
            if (Slice::from(range.lower) <= smallest && largest < Slice::from(range.upper)) {
                dead.push_back(candidate.file);
                break;
            }
        }
    }
    if (dead.empty()) return false;

    VersionEdit edit;
    for (const FileMetadata& file : dead) {
        edit.deleted.push_back(FileRef{file.level, file.file_number});
    }
    status = versions_->apply(std::move(edit));
    return status == Status::Ok;
}

void DbImpl::background_compaction_loop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(compaction_mutex_);
            // Untimed on purpose, and it is not the untimed wait the maintenance design forbids:
            // this thread waits to be *dispatched*, and the coordinator's bounded tick is what
            // guarantees a dispatch arrives. Putting a second timer here would be the second
            // implementation of the same policy.
            while (!shutting_down_compaction_ && !compaction_pending_) {
                compaction_scheduled_.wait(lock);
            }
            if (shutting_down_compaction_) return;
            compaction_pending_ = false;
        }

        Status status = Status::Ok;
        const size_t pending_before = versions_->pending_deletions_hint();
        versions_->collect_obsolete();
        bool did_work = versions_->pending_deletions_hint() < pending_before;
        // Files the truncation point has made unreadable, before anything else: they are removed
        // by a manifest edit alone, so dropping them first spares a compaction the work of
        // rewriting bytes that are already dead. It applies a version edit, which makes it a
        // *deleting task* — sound only because it shares this one executor with compaction,
        // migration and eviction, the same reason they are sound.
        if (reclaim_truncated_files(status)) did_work = true;

        // ARCHITECTURE.md "Migration between tiers" — migrations off a Transient tier preempt everything, including
        // compaction. Draining them first is that rule.
        while (run_one_migration(status)) {
            did_work = true;
        }
        while (status == Status::Ok && run_one_compaction(status)) {
            did_work = true;
            while (run_one_migration(status)) {
            }
        }

        // The orphan sweep, when its interval has elapsed. On the maintenance executor rather than
        // the coordinator because it is a paginated list per store — long-running work, which the
        // coordinator does none of. It deletes objects but applies no version edit, so it is not a
        // *deleting task* in the single-deleter sense and needs no guard.
        if (options_.orphan_sweep_interval.has_value()) {
            const uint64_t now = now_ms();
            if (now >= next_sweep_ms_.load()) {
                next_sweep_ms_.store(
                    now + static_cast<uint64_t>(options_.orphan_sweep_interval->count()));
                const Status swept = sweep_orphans();
                if (swept != Status::Ok && !is_retryable(swept)) {
                    std::lock_guard<std::mutex> lock(mem_mutex_);
                    if (bg_error_ == Status::Ok || is_retryable(bg_error_)) bg_error_ = swept;
                }
            }
        }

        // **Only on work done.** The completion of a task can make the next one due, so the
        // coordinator has to be told — but telling it unconditionally would be a busy loop:
        // every pass would open the gate, which dispatches another pass, which finds nothing and
        // opens it again. A pass that changed nothing has nothing to invalidate.
        //
        // A *failure* deliberately does not bump either. A retryable `Io` against a store that
        // is down would otherwise be retried on every tick for the duration of the outage; the
        // periodic gate bypass is what retries it, which is late rather than never and is the
        // right cadence for something that is failing.
        if (did_work) note_maintenance_state_changed();
        if (status != Status::Ok && !is_retryable(status)) {
            std::lock_guard<std::mutex> lock(mem_mutex_);
            // **The cause, not the consequence.** This thread reports `Unusable` for
            // any already-terminal instance, so writing unconditionally would replace
            // the first real failure — a fence, a corruption — with a derived one, and
            // whoever calls `flush()` next would hear the wrong thing. That is exactly
            // how a fence came back as `Unusable`, intermittently, depending on which
            // thread got to `bg_error_` second.
            if (bg_error_ == Status::Ok || is_retryable(bg_error_)) bg_error_ = status;
        }
        // Whatever happened, a waiting writer must re-evaluate: either the level
        // drained or the failure is what it needs to hear about.
        compaction_finished_.notify_all();
        flush_finished_.notify_all();
    }
}

Status DbImpl::compact_level(int level) {
    if (read_only_) return Status::Config;
    if (unusable_.load()) return Status::Unusable;
    if (versions_->fenced()) return Status::Fenced;
    if (level < 0 || level > config_.last()) return Status::Config;

    // One pass over a *snapshot* of the level's files. Files created by this
    // pass are not revisited, which is what makes it terminate by construction —
    // and at the bottommost level, where the rewrite lands back in the same
    // level, that is the only thing that would.
    std::vector<FileMetadata> pass = versions_->current()->files_at(level);

    // **Oldest first at L0**, and this is a correctness requirement rather than a
    // preference. ARCHITECTURE.md "Positional recency" makes recency at L0 positional — higher file number is
    // newer — and `files_at(0)` hands them back newest-first. Rewriting one file
    // per compaction means each output lands in the level below before the next
    // file is considered, so a newest-first pass would merge an *older* L0 file
    // against the newer data it just wrote down there. `Compaction::all_inputs()`
    // orders the source level ahead of the output level because the source is
    // normally the newer of the two; going oldest-first is what keeps that true
    // at every step. Otherwise an older value — or an older tombstone — wins, and
    // a committed write is reverted or dropped.
    if (level == 0) std::reverse(pass.begin(), pass.end());
    const bool bottom = level == config_.last();
    const int output_level = bottom ? level : level + 1;

    const ResolvedLevel& target = level_config(output_level);

    for (const FileMetadata& file : pass) {
        // At the last level the rewrite lands back here, so a file already
        // written under the current compression, already on the tier its age and
        // size call for, and with nothing to reclaim is done — which is what
        // lets a second call be a no-op. Elsewhere the file leaves the level
        // regardless, and the level being empty is the completion condition.
        if (bottom && file.compression == target.compression && file.num_tombstones == 0 &&
            tier_for(file.min_write_time_ms).store->id() == file.store_id) {
            continue;
        }

        Compaction compaction;
        compaction.level = level;
        compaction.output_level = output_level;
        compaction.inputs = {file};
        // A rewrite, never a move: moving is exactly what fails to migrate.
        compaction.trivial_move = false;

        std::lock_guard<std::mutex> work(compaction_work_mutex_);
        {
            // Scoped, so this version is released before the compaction runs:
            // holding it would make every file it references look like a live
            // read and defer the collection of what we just replaced (ARCHITECTURE.md "Versions are immutable snapshots").
            auto version = versions_->current();

            // Skip anything ordinary compaction already carried away underneath us.
            const auto& present = version->files_at(level);
            if (std::find_if(present.begin(), present.end(), [&](const FileMetadata& candidate) {
                    return candidate.file_number == file.file_number;
                }) == present.end()) {
                continue;
            }

            // The *effective* span, so a file carrying range tombstones is merged against what it
            // shadows rather than moved past it. See `FileMetadata::effective_smallest`.
            const std::string span_low = file.effective_smallest();
            const std::string span_high = file.effective_largest();
            if (!bottom) {
                compaction.overlaps =
                    version->overlapping_inclusive(output_level, Slice::from(span_low),
                                                   Slice::from(span_high));
                if (output_level + 1 <= config_.last()) {
                    compaction.grandparents = version->overlapping_inclusive(
                        output_level + 1, Slice::from(span_low), Slice::from(span_high));
                }
            }
            compaction.output_is_bottommost =
                is_bottommost_for_range(*version, output_level, config_.last(),
                                        Slice::from(span_low), Slice::from(span_high));
        }

        if (Status status = run_compaction(compaction); status != Status::Ok) return status;
    }

    // The last compaction's superseded files are only collectable now that this
    // call no longer holds a version referencing them.
    versions_->collect_obsolete();
    return Status::Ok;
}

// --- migration (ARCHITECTURE.md "Migration between tiers") ----------------------------------------------------------

bool DbImpl::compact_l0_file_off_its_tier(Status& status) {
    if (task_suppressed(MaintenanceTask::LevelZeroEscape)) return false;

    Compaction compaction;
    {
        auto version = versions_->current();
        if (config_.last() < 1) return false;

        // The L0 file whose tier no longer matches its placement. ARCHITECTURE.md "Positional recency" resolves L0
        // recency by file number, so this file cannot simply be copied — it has to be
        // rewritten into L1, where order is by key.
        //
        // **Chosen by file number, and only when nothing older overlaps it.** This used
        // to choose the smallest `min_write_time_ms`, which is a different order and a
        // wrong one. `min_write_time_ms` is a memtable's creation time, so with a small
        // memtable several flushes share a clock tick — and among ties the loop kept the
        // *first* it saw, which is the **newest** file, because `files_at(0)` runs
        // newest-first. Pushing a newer L0 file into L1 leaves older L0 files above it,
        // and L0 always shadows L1: a committed write reverts to its previous value, and
        // survives a reopen because the manifest records it. The differential oracle
        // found it as "a scan came back one row short" 900 operations later.
        //
        // Waiting is the right response when something older overlaps: ARCHITECTURE.md "Migration between tiers" makes
        // age-driven migration between durable tiers the lowest priority and starvable
        // without harm, and the older file leaves under ordinary L0 pressure anyway.
        const FileMetadata* seed = nullptr;
        for (const FileMetadata& file : version->files_at(0)) {
            const int tier = tiers_.tier_of_store(file.store_id);
            if (tier < 0) continue;
            if (placement(tiers_, file.min_write_time_ms, now_ms()) <= tier) {
                continue;
            }
            if (seed == nullptr || file.file_number < seed->file_number) seed = &file;
        }
        if (seed == nullptr) return false;

        for (const FileMetadata& other : version->overlapping_inclusive(
                 0, Slice::from(seed->effective_smallest()),
                 Slice::from(seed->effective_largest()))) {
            // An older L0 file covering any of this range would shadow the data once it
            // moves below. Leave it for now.
            if (other.file_number < seed->file_number) return false;
        }

        compaction.level = 0;
        compaction.output_level = 1;
        compaction.inputs = {*seed};
        compaction.trivial_move = false;  // a move would leave it on the same tier
        compaction.overlaps = version->overlapping_inclusive(
            1, Slice::from(seed->effective_smallest()), Slice::from(seed->effective_largest()));
        if (config_.last() >= 2) {
            compaction.grandparents = version->overlapping_inclusive(
                2, Slice::from(seed->effective_smallest()),
                                                           Slice::from(seed->effective_largest()));
        }
        compaction.output_is_bottommost =
            is_bottommost_for_range(*version, 1, config_.last(),
                                    Slice::from(seed->effective_smallest()),
                                    Slice::from(seed->effective_largest()));
    }

    status = run_compaction(compaction);
    return status == Status::Ok;
}

bool DbImpl::run_one_migration(Status& status) {
    if (unusable_.load()) {
        status = Status::Unusable;
        return false;
    }
    if (versions_->fenced()) {
        status = Status::Fenced;
        return false;
    }

    // A migration and a compaction both mutate the version; they must not race.
    std::lock_guard<std::mutex> work(compaction_work_mutex_);

    std::optional<Migration> migration;
    {
        // Same reason as run_one_compaction: do not hold a version across the
        // work, or the object this migration supersedes cannot be collected.
        auto version = versions_->current();
        migration = pick_migration(*version, tiers_, now_ms());
    }
    // ARCHITECTURE.md "Negative controls" — a test may refuse one constituent of convergence, so
    // that draining the other three can be shown not to be enough.
    if (migration.has_value()) {
        const MaintenanceTask task =
            migration->capacity_eviction
                ? MaintenanceTask::CapacityEviction
                : (migration->leaves_transient ? MaintenanceTask::TransientRescue
                                               : MaintenanceTask::DurableAgeMigration);
        if (task_suppressed(task)) return false;
    }
    if (!migration.has_value()) {
        // The migrator skips L0; an L0 file over its tier's age leaves by being
        // compacted down instead.
        return compact_l0_file_off_its_tier(status);
    }

    status = run_migration(*migration);
    compaction_finished_.notify_all();
    return status == Status::Ok;
}

Status DbImpl::run_migration(const Migration& migration) {
    ELYSIUMKV_CLAIM_DELETING_TASK();
    BlobStore* source = store_for(migration.file.store_id);
    if (source == nullptr) return Status::Corrupt;
    const Tier& target = tiers_.tiers[static_cast<size_t>(migration.to_tier)];
    if (target.store->id() == migration.file.store_id) return Status::Ok;

    // ARCHITECTURE.md "The manifest is snapshots plus edits" — **a file number is never reused, including across tier migration.**
    // The copy gets a fresh number and the edit carries `added` and `deleted`,
    // structurally identical to a compaction — not an in-place mutation of
    // `store_id`. Keeping the number would make object identity the pair
    // (store_id, file_number), and every component keyed on the number alone
    // becomes subtly wrong; it also leaves a kill between the copy and the edit
    // ambiguous, with the same name on two stores.
    const std::string source_name = sst_object_name(migration.file.file_number);

    // The copy is byte-for-byte. Compression is a level property (ARCHITECTURE.md "Inside an SST") and
    // migration does not change a file's level, so nothing is decoded: a
    // migration costs exactly the bytes moved.
    auto bytes = source->bulk_view().get(source_name, 0, BlobStore::kReadToEnd).get();
    if (!bytes) return bytes.error();
    if (bytes->size() != migration.file.file_bytes) return Status::Corrupt;

    auto file_number = write_new_sst(*target.store, Slice::from(*bytes));
    if (!file_number) return file_number.error();

    // Durable edit first, delete second (ARCHITECTURE.md "Open and recovery"). A crash in between leaves the
    // copy as an unambiguous orphan, which open collects; the reverse order
    // would leave a version referencing an object that no longer exists.
    FileMetadata moved = migration.file;
    moved.file_number = *file_number;
    moved.store_id = target.store->id();
    // Carried over unchanged, so placement stays monotone across the renumber
    // (ARCHITECTURE.md "A tier is not a level"): the file is exactly as old as it was.
    moved.min_write_time_ms = migration.file.min_write_time_ms;
    // The watermark interval likewise: a migration is a byte copy, so the file holds exactly the
    // writes it held before, and both bounds are still the bounds. `moved` is a copy of the
    // source metadata, so this is already true — stated because it is the kind of thing a later
    // change to this function would silently drop.
    moved.watermark = migration.file.watermark;

    VersionEdit edit;
    edit.deleted.push_back({migration.file.level, migration.file.file_number});
    edit.added.push_back(std::move(moved));
    if (Status status = versions_->apply(std::move(edit)); status != Status::Ok) return status;

    migrations_.fetch_add(1, std::memory_order_relaxed);
    migration_bytes_.fetch_add(migration.file.file_bytes, std::memory_order_relaxed);
    if (options_.paranoid_checks) return check_invariants();
    return Status::Ok;
}

bool DbImpl::run_one_compaction(Status& status) {
    if (unusable_.load()) {
        status = Status::Unusable;
        return false;
    }
    if (versions_->fenced()) {
        status = Status::Fenced;
        return false;
    }

    // Picking and running are one unit: the inputs are chosen from a version,
    // and another compaction finishing in between would leave this one merging
    // files that no longer describe the store.
    std::lock_guard<std::mutex> work(compaction_work_mutex_);

    std::optional<Compaction> compaction;
    {
        // Scoped: the Compaction owns copies of everything it needs, and holding
        // the version across the run would make every file it references look
        // like a live read, deferring the collection of what this compaction
        // replaces (ARCHITECTURE.md "Versions are immutable snapshots").
        auto version = versions_->current();
        compaction = pick_compaction(*version, config_, options_.max_compaction_bytes,
                                     {options_.tombstone_density_trigger,
                                      options_.tombstone_density_min_entries});
    }
    if (!compaction.has_value()) return false;
    if (compaction->triggered_by_density) {
        density_compactions_.fetch_add(1, std::memory_order_relaxed);
    }

    status = run_compaction(*compaction);
    compaction_finished_.notify_all();
    return status == Status::Ok;
}

Status DbImpl::compact_until_quiet() {
    // A version released since the last edit — an iterator going out of scope —
    // may have been the last holder of a file waiting to be unlinked.
    versions_->collect_obsolete();

    // Bounded so a picker that cannot make progress fails loudly instead of
    // spinning; in practice each round removes at least one file.
    constexpr int kMaxRounds = 10000;
    Status status = Status::Ok;
    for (int round = 0; round < kMaxRounds; ++round) {
        // Migration first: it preempts compaction, and in Inline mode the
        // caller is the only thread that will ever run either.
        bool worked = false;
        // Same order as the threaded executor, for the same reason: files the truncation point
        // emptied go by manifest edit alone, so dropping them first spares a compaction the work
        // of rewriting dead bytes. Inline mode has to run it too, or reclamation would be a
        // property only the threaded build had.
        if (reclaim_truncated_files(status)) worked = true;
        if (status != Status::Ok) return status;
        while (run_one_migration(status)) worked = true;
        if (status != Status::Ok) return status;
        if (run_one_compaction(status)) worked = true;
        if (status != Status::Ok) return status;
        if (!worked) break;
    }
    return status;
}

Status DbImpl::run_compaction(const Compaction& compaction) {
    ELYSIUMKV_CLAIM_DELETING_TASK();
    VersionEdit edit;
    edit.compaction_pointers.emplace_back(compaction.level, compaction.largest_key());

    if (compaction.trivial_move) {
        // Within one store this is a pure manifest operation: the bytes do not
        // move, only the level they are recorded at (ARCHITECTURE.md "Compaction").
        FileMetadata moved = compaction.inputs.front();
        edit.deleted.push_back({compaction.level, moved.file_number});
        moved.level = compaction.output_level;
        edit.added.push_back(std::move(moved));
    } else {
        std::vector<FileMetadata> outputs;
        if (Status status = write_compaction_outputs(compaction, outputs); status != Status::Ok) {
            // The objects written so far are orphans: no version references
            // them, and open collects them. Nothing has been committed.
            return status;
        }
        for (const FileMetadata& file : compaction.inputs) {
            edit.deleted.push_back({compaction.level, file.file_number});
        }
        for (const FileMetadata& file : compaction.overlaps) {
            edit.deleted.push_back({compaction.output_level, file.file_number});
        }
        for (FileMetadata& file : outputs) edit.added.push_back(std::move(file));
    }

    if (Status status = versions_->apply(std::move(edit)); status != Status::Ok) return status;

    compactions_.fetch_add(1, std::memory_order_relaxed);
    // A trivial move reads nothing — it is a manifest edit, the file is not
    // opened. Counting its inputs as bytes read inflates the only number an
    // operator has for write amplification, and in the common case where a level
    // drains entirely by trivial move it reports bytes read against zero bytes
    // written, which reads as an infinitely expensive compaction that never
    // happened.
    if (!compaction.trivial_move) {
        compaction_bytes_read_.fetch_add(compaction.input_bytes(), std::memory_order_relaxed);
    }
    if (options_.paranoid_checks) return check_invariants();
    return Status::Ok;
}

Status DbImpl::write_compaction_outputs(const Compaction& compaction,
                                        std::vector<FileMetadata>& outputs) {
    const ResolvedLevel& target = level_config(compaction.output_level);

    // Sources in recency order: the source level first (L0 already sorted by
    // descending file number), then the output level. Recency is positional (ARCHITECTURE.md "Positional recency").
    std::vector<std::shared_ptr<SstReader>> readers;
    std::vector<std::unique_ptr<InternalIterator>> children;
    std::vector<std::vector<RangeTombstone>> child_ranges;
    std::vector<RangeTombstone> all_ranges;
    for (const FileMetadata& file : compaction.all_inputs()) {
        auto reader = reader_for(file);
        if (!reader) return reader.error();
        readers.push_back(*reader);
        children.push_back((*reader)->iterator());
        auto ranges = (*reader)->range_tombstones();
        if (!ranges) return ranges.error();
        all_ranges.insert(all_ranges.end(), ranges->begin(), ranges->end());
        child_ranges.push_back(std::move(*ranges));
    }
    // **Handed to the merge, so the covered entries never reach the output.** Without this the
    // compaction would copy them forward into the same file as the tombstone that covers them — and
    // a tombstone shadows nothing in its own file, so every key the range deleted would come back.
    auto merged = make_merging_iterator(std::move(children), std::move(child_ranges));

    // ARCHITECTURE.md "Compaction" — tombstones are dropped only when the output lands in the bottommost
    // level that could contain the key. There is nothing else to consider
    // without snapshots.
    // ARCHITECTURE.md "Compaction" — dropped when, and only when, the output is bottommost for its key
    // range — no deeper level holds an overlapping file. Dynamic, not the last
    // configured level.
    const bool drop_tombstones = compaction.output_is_bottommost;

    // **The range tombstones have to be carried down too, and for the same reason point tombstones
    // are.** Files older than this compaction were not read and are still there; a tombstone that
    // stopped here would stop shadowing them and the keys would come back at the next read. At the
    // bottommost level there is nothing older left, which is exactly when it can be let go.
    //
    // Each output carries the part of them that falls in **its own** slice of the keyspace, clipped
    // at the cut points. Files at one level are ordered by key rather than by recency, so an output
    // whose tombstone reached into a sibling's range would shadow that sibling — and the sibling
    // holds entries this compaction just decided to keep. Clipping tiles the range: no overlap
    // between outputs, and no gap between them either, which a clip to actual keys would leave.
    const std::vector<RangeTombstone> carried =
        drop_tombstones ? std::vector<RangeTombstone>{} : merge_ranges(std::move(all_ranges));
    // The key that opens the current output, and the one that will open the next. Null means
    // unbounded, which is right for the first output's lower edge and the last one's upper.
    std::optional<std::string> output_lower;
    std::optional<std::string> output_upper;
    bool cut_pending = false;
    // Truncated keys are dropped wherever the compaction lands, not only at the bottom. A tombstone
    // has to survive to the bottommost level because a deeper file may still hold the key it
    // shadows; a truncation point shadows every level at once, so there is nothing left to shadow
    // and nothing to carry down.
    //
    // **This reclaims space; it does not change an answer** — the read clamp already hides these
    // keys. It is what narrows a file straddling the point, the whole ones below it never reaching
    // a compaction at all.
    const std::string truncation_point = versions_->current()->truncation_point();
    const uint64_t min_write_time = compaction.min_write_time_ms();
    const WatermarkInterval watermark = compaction.watermark();
    const size_t grandparent_limit = max_grandparent_overlap_bytes(target);

    SstOptions sst_options;
    sst_options.block_bytes = options_.block_bytes;
    sst_options.restart_interval = options_.restart_interval;
    sst_options.bloom_bits_per_key = options_.bloom_bits_per_key;
    sst_options.compression = target.compression;

    std::unique_ptr<SstWriter> writer;
    size_t grandparent_index = 0;
    uint64_t grandparent_bytes = 0;

    auto finish_output = [&]() -> Status {
        if (writer == nullptr) return Status::Ok;
        for (const RangeTombstone& range :
             clip_ranges(carried, output_lower ? &*output_lower : nullptr,
                         output_upper ? &*output_upper : nullptr)) {
            writer->add_range_tombstone(Slice::from(range.lower), Slice::from(range.upper));
        }
        auto built = writer->finish();
        writer.reset();
        if (!built) return built.error();
        if (built->num_entries == 0 && built->num_range_tombstones == 0) return Status::Ok;

        // Output made from old data *is* old, so it lands directly on a cold
        // tier rather than being written hot and migrated straight back out.
        const Tier& tier = tier_for(min_write_time);

        auto file_number = write_new_sst(*tier.store, Slice::from(built->bytes));
        if (!file_number) return file_number.error();
        compaction_bytes_written_.fetch_add(built->bytes.size(), std::memory_order_relaxed);

        FileMetadata file;
        file.level = compaction.output_level;
        file.file_number = *file_number;
        file.store_id = tier.store->id();
        file.smallest_key = built->smallest_key;
        file.largest_key = built->largest_key;
        file.file_bytes = built->bytes.size();
        file.num_entries = built->num_entries;
        file.num_tombstones = built->num_tombstones;
        file.num_range_tombstones = built->num_range_tombstones;
        file.smallest_range_key = built->smallest_range_key;
        file.largest_range_key = built->largest_range_key;
        file.compression = target.compression;
        // A compaction output takes the min() over its inputs (ARCHITECTURE.md "The manifest is snapshots plus edits"): the file
        // still holds those writes, so its exposure is unchanged by the move.
        file.min_write_time_ms = min_write_time;
        // `min` of the lows, `max` of the highs — computed once for the whole compaction, since
        // every output holds a slice of the same input set.
        file.watermark = watermark;
        outputs.push_back(std::move(file));
        return Status::Ok;
    };

    for (merged->seek_to_first(); merged->valid(); merged->next()) {
        if (drop_tombstones && merged->type() == ValueType::Delete) continue;
        if (!truncation_point.empty() && merged->key() < Slice::from(truncation_point)) continue;
        // The cut is taken here rather than at the moment the output filled, because the boundary
        // is this key: everything below it belongs to the output just closed and everything from it
        // upwards to the next one. Deciding earlier would mean guessing where the next key lands.
        if (cut_pending) {
            output_upper = merged->key().to_string();
            if (Status status = finish_output(); status != Status::Ok) return status;
            output_lower = std::move(output_upper);
            output_upper.reset();
            cut_pending = false;
        }
        if (writer == nullptr) writer = std::make_unique<SstWriter>(sst_options);
        writer->add(merged->key(), merged->type(), merged->value());

        // Grandparent accounting: how much of Ln+2 this output would eventually
        // have to be merged with. Bounding it here bounds the cost of the *next*
        // compaction (ARCHITECTURE.md "Compaction").
        while (grandparent_index < compaction.grandparents.size() &&
               Slice::from(compaction.grandparents[grandparent_index].largest_key) <
                   merged->key()) {
            grandparent_bytes += compaction.grandparents[grandparent_index].file_bytes;
            ++grandparent_index;
        }

        const bool full = writer->estimated_bytes() >= target.target_file_bytes;
        const bool grandparent_heavy = grandparent_bytes > grandparent_limit;
        if (full || grandparent_heavy) {
            cut_pending = true;
            grandparent_bytes = 0;
        }
    }
    if (merged->status() != Status::Ok) return merged->status();
    // Everything the compaction held may have been covered, leaving the tombstones with no entry to
    // ride along with. They still have older files to shadow, so they get a file of their own.
    if (!carried.empty() && writer == nullptr) writer = std::make_unique<SstWriter>(sst_options);
    return finish_output();
}

}  // namespace elysiumkv
