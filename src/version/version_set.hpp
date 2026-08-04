#ifndef ELYSIUMKV_VERSION_VERSION_SET_HPP
#define ELYSIUMKV_VERSION_VERSION_SET_HPP

#include "elysiumkv/manifest_catalog.hpp"
#include "version/version.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Versions are immutable snapshots" — the component that matters. Everything before it is a sorted-file
/// library.
///
/// A file is unlinked only when no live `Version` references it and no iterator
/// holds it — **not** when compaction finishes. The failure this prevents is an
/// iterator reading a file that compaction unlinked mid-scan: silent,
/// load-dependent, and not reproducible without deliberate effort.
class VersionSet {
public:
    /// Deletes obsolete objects and evicts their blocks, and **returns the ones it
    /// could not delete** — those stay pending and are retried on the next pass.
    /// Supplied by the engine, which owns the stores.
    ///
    /// A batch rather than one call per file: a compaction can obsolete dozens of
    /// objects at once, and against a remote store one call per file is one HTTP
    /// round trip per file. The grouping by store lives on the engine side, which
    /// is the only side that knows which store a `store_id` names.
    using DeleteObjects =
        std::function<std::vector<FileMetadata>(const std::vector<FileMetadata>&)>;

    VersionSet(ManifestCatalog& catalog, int edits_per_generation, DeleteObjects deleter);

    /// Replays the live generation. `Status::NotFound` means the store has no
    /// pointer yet — an empty store, not a damaged one.
    Status recover();
    /// Installs generation 1 holding an empty version.
    Status create();

    std::shared_ptr<const Version> current() const;

    /// Persists the edit, *then* swaps in the new version. A failure to persist
    /// leaves the current version untouched.
    Status apply(VersionEdit edit);

    uint64_t allocate_file_number();

    /// Raises the counter so it never hands out `number` or anything below it.
    ///
    /// **This is what lets open stop deleting.** A crash between an SST `put` and the edit
    /// recording it leaves an object at a number recovery hands straight back out, because
    /// the edit carrying the advanced counter never landed. Stepping over what the stores
    /// already hold makes that residue harmless; the alternative — deleting every
    /// unreferenced object at open — cannot tell a dead writer's residue from a live
    /// writer's committed file.
    void observe_file_number(uint64_t number);
    uint64_t next_file_number() const {
        return next_file_number_.load(std::memory_order_relaxed);
    }

    /// Deletes objects whose removal is recorded and durable and which no live
    /// version references. Safe to call at any time; runs after every swap.
    void collect_obsolete();

#ifdef ELYSIUMKV_PARANOID
    /// ARCHITECTURE.md "Negative controls" — swaps in a version without persisting an edit, so a test can
    /// construct a state the engine would never produce. In memory only: the
    /// manifest keeps saying what actually happened, which is what makes this
    /// safe to have compiled in alongside the checks it exists to trip.
    void install_for_test(std::shared_ptr<const Version> version) {
        install(std::move(version));
    }
#endif

    /// True once a compare-and-set was lost: another writer owns the store, this
    /// instance is fenced and must be closed and reopened (ARCHITECTURE.md "Ownership is one compare-and-set").
    bool fenced() const { return fenced_.load(std::memory_order_acquire); }

    /// Fences the instance on evidence found outside the manifest — an SST name
    /// already taken, which under the no-reuse rule and open-time orphan
    /// collection can only be another writer's object. Without this the engine
    /// would keep writing objects it can never install.
    void mark_fenced() { fenced_.store(true, std::memory_order_release); }

    uint64_t generation() const;
    /// Files awaiting collection — a live iterator is the usual reason.
    size_t pending_deletions() const;

private:
    Status write_snapshot_and_install(uint64_t generation,
                                      const std::shared_ptr<const Version>& version);
    Status maybe_roll_generation(const std::shared_ptr<const Version>& version);
    /// Caller holds `mutex_`.
    void collect_obsolete_locked();
    void install(std::shared_ptr<const Version> version);

    ManifestCatalog& catalog_;
    int edits_per_generation_;
    DeleteObjects deleter_;

    mutable std::mutex mutex_;  // guards manifest state, not reads of `current()`

    /// ARCHITECTURE.md "Versions are immutable snapshots" asks for `std::atomic<std::shared_ptr<const Version>>`; libc++ has not
    /// implemented it (P0718), so the swap is guarded by its own mutex instead.
    /// The property that matters is preserved: taking a version never waits on
    /// the manifest mutex that the flush and compaction threads hold while
    /// writing. A hazard-pointer or seqlock scheme remains the permitted
    /// optimisation if profiling shows this is hot.
    mutable std::mutex current_mutex_;
    std::shared_ptr<const Version> current_;
    std::optional<ManifestCatalog::Entry> entry_;
    uint64_t next_seq_ = 1;
    std::vector<std::weak_ptr<const Version>> live_versions_;
    std::vector<FileMetadata> pending_deletions_;

    std::atomic<uint64_t> next_file_number_{1};
    std::atomic<bool> fenced_{false};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_VERSION_SET_HPP
