#ifndef ELYSIUMKV_VERSION_VERSION_SET_HPP
#define ELYSIUMKV_VERSION_VERSION_SET_HPP

#include "crypt/provider_registry.hpp"
#include "elysiumkv/manifest_catalog.hpp"
#include "version/version.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Versions are immutable snapshots" — the component that matters. Everything before it is a sorted-file
/// library.
///
/// A file is unlinked only when no live `Version` references it and no iterator
/// holds it — not when compaction finishes. The failure this prevents is an
/// iterator reading a file that compaction unlinked mid-scan: silent,
/// load-dependent, and not reproducible without deliberate effort.
class VersionSet {
public:
    /// Deletes obsolete objects and evicts their blocks, and returns the ones it
    /// could not delete — those stay pending and are retried on the next pass.
    /// Supplied by the engine, which owns the stores.
    ///
    /// A batch rather than one call per file: a compaction can obsolete dozens of
    /// objects at once, and against a remote store one call per file is one HTTP
    /// round trip per file. The grouping by store lives on the engine side, which
    /// is the only side that knows which store a `store_id` names.
    using DeleteObjects =
        std::function<std::vector<FileMetadata>(const std::vector<FileMetadata>&)>;

    /// `clock` and `obsolete_retention` govern how long a superseded object is kept after nothing
    /// local references it. The retention exists for readers in other processes, which this
    /// collector cannot see: `live_versions_` is a process-local list of weak pointers, so without
    /// a delay a compaction here deletes objects a reader elsewhere is still reading. Zero
    /// retention is today's behaviour and is correct when there are no readers.
    /// `encryption` frames every manifest payload this instance writes and routes every one it
    /// reads. The same registry the SST path uses, so a file and the edit recording it can
    /// never disagree about what a provider id means.
    VersionSet(ManifestCatalog& catalog, int edits_per_generation, DeleteObjects deleter,
               const ProviderRegistry& encryption,
               std::function<uint64_t()> clock = nullptr, Duration obsolete_retention = Duration(0));

    /// Set when a recovery failure has something an operator can act on — an encryption provider
    /// the manifest names and this process has not registered, most of all. Empty otherwise.
    const std::string& last_error() const { return last_error_; }

    /// Every file number the current version references. The orphan sweep diffs a store listing
    /// against this, and against `pending_deletions()`, to decide what is unreferenced.
    std::set<uint64_t> referenced_file_numbers() const;
    /// File numbers already queued for deletion, so the sweep does not double-count them as
    /// orphans — they have an exact unreferenced-since time and a window of their own.
    std::set<uint64_t> pending_file_numbers() const;

    /// Whether the manifest has moved on since this instance read it — a rolled generation, or a
    /// newer edit within the current one.
    ///
    /// The pointer alone is not enough, and that is easy to get wrong: the pointer moves only on
    /// a generation roll, while a writer obsoletes and collects files on every edit. So this checks
    /// the edit sequence too. Used by a read-only instance to tell "my version is older than the
    /// writer's retention window" from "this object is genuinely lost", which are the same symptom
    /// and opposite diagnoses.
    ///
    /// A read failure answers *no*. Failure to look is not evidence, and answering *yes* on a
    /// failed read would relabel real corruption as staleness — the one direction that must never
    /// happen.
    bool manifest_advanced() const;

    /// Replays the live generation. `Status::NotFound` means the store has no
    /// pointer yet — an empty store, not a damaged one.
    Status recover();
    /// Installs generation 1 holding an empty version.
    Status create();

    /// The current version, by value.
    ///
    /// Hold the returned pointer for as long as anything reads into the Version. A compaction
    /// or a flush installs a new version at any moment, and the old one is destroyed as soon as its
    /// last owner goes — so `current()->truncation_point()` yields a reference that is dangling by
    /// the next statement, since the temporary owner dies at the semicolon and binding a reference
    /// to a member of its pointee extends nothing. Copying out of the expression is fine; keeping a
    /// reference into it is not. This shipped once, in `delete_range`, and tsan found it.
    std::shared_ptr<const Version> current() const;

    /// Levels whose file count is published as its own atomic by `install`.
    static constexpr int kPublishedLevels = 16;

    /// The installed version's file count at `level`, without taking the version.
    ///
    /// For the write path's valve, which asks on every write and needs nothing else from the
    /// Version. Taking the version there costs a lock and a refcount — measurably, at roughly a
    /// twentieth of a `put` — to read three integers that never change between installs. Published
    /// by `install` itself, so there is no second place that has to remember to update it.
    ///
    /// Levels at or beyond `kPublishedLevels` are not published and read as zero; a caller with
    /// more levels than that must use the Version. `DbImpl` decides once, at open.
    uint32_t published_file_count(int level) const {
        if (level < 0 || level >= kPublishedLevels) return 0;
        return file_counts_[static_cast<size_t>(level)].load(std::memory_order_relaxed);
    }

    /// Persists the edit, *then* swaps in the new version. A failure to persist
    /// leaves the current version untouched.
    Status apply(VersionEdit edit);

    uint64_t allocate_file_number();

    /// Raises the counter so it never hands out `number` or anything below it.
    ///
    /// This is what lets open stop deleting. A crash between an SST `put` and the edit
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
    /// How many version slots are being tracked, expired ones included. Only a test cares:
    /// the interesting property is that this stays bounded on a store that never deletes.
    size_t tracked_versions() const;

    /// The same count, readable without taking `mutex_` — which matters because the
    /// maintenance coordinator asks every tick and that mutex is held across manifest writes
    /// to remote storage. A hint rather than a value: it may be a moment stale, which is
    /// harmless for a predicate that only decides whether to look.
    size_t pending_deletions_hint() const {
        return pending_deletions_hint_.load(std::memory_order_relaxed);
    }

    /// How many versions have been installed. The maintenance coordinator folds this into its
    /// epoch: a new version is the single largest source of predicate-relevant change, and
    /// counting installs is cheaper than asking each predicate whether anything moved.
    uint64_t installs() const { return installs_.load(std::memory_order_relaxed); }

    uint64_t manifest_payloads_pending_reencryption() const {
        return manifest_payloads_pending_reencryption_.load(std::memory_order_relaxed);
    }

    /// Rolls to a new generation now, whatever the edit count is.
    ///
    /// For finishing an encryption rotation. Manifest payloads are sealed under whichever
    /// provider was primary when they were written, so a store whose *files* have all been
    /// rewritten still cannot open without the retired provider until a fresh snapshot exists
    /// under the new one. Rolling is what writes that snapshot; there is nothing else that would.
    ///
    /// A no-op on an empty generation, so calling it when nothing has changed costs nothing.
    Status roll_generation_now();

private:
    Status write_snapshot_and_install(uint64_t generation,
                                      const std::shared_ptr<const Version>& version);
    Status maybe_roll_generation(const std::shared_ptr<const Version>& version);
    /// Caller holds `mutex_`.
    void collect_obsolete_locked();
    void install(std::shared_ptr<const Version> version);

    std::array<std::atomic<uint32_t>, kPublishedLevels> file_counts_{};

    ManifestCatalog& catalog_;
    const ProviderRegistry& encryption_;
    std::string last_error_;
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
    /// A file waiting to be deleted, and when it stopped being referenced. The timestamp is what
    /// `obsolete_retention` is measured from.
    struct PendingDeletion {
        FileMetadata file;
        uint64_t unreferenced_since_ms = 0;
    };
    std::vector<PendingDeletion> pending_deletions_;
    std::function<uint64_t()> clock_;
    Duration obsolete_retention_{0};

    std::atomic<uint64_t> next_file_number_{1};
    std::atomic<bool> fenced_{false};
    std::atomic<size_t> pending_deletions_hint_{0};
    std::atomic<uint64_t> installs_{0};
    std::atomic<uint64_t> manifest_payloads_pending_reencryption_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_VERSION_SET_HPP
