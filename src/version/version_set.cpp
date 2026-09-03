#include "version/version_set.hpp"

#include "version/manifest_payload.hpp"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <limits>
#include <set>
#include <utility>
#include <vector>

namespace elysiumkv {

VersionSet::VersionSet(ManifestCatalog& catalog, int edits_per_generation, DeleteObjects deleter,
                       const ProviderRegistry& encryption, std::function<uint64_t()> clock,
                       Duration obsolete_retention)
    : catalog_(catalog),
      encryption_(encryption),
      edits_per_generation_(std::max(1, edits_per_generation)),
      deleter_(std::move(deleter)),
      clock_(std::move(clock)),
      obsolete_retention_(obsolete_retention) {
    std::lock_guard<std::mutex> lock(current_mutex_);
    current_ = std::make_shared<const Version>();
}

std::shared_ptr<const Version> VersionSet::current() const {
    std::lock_guard<std::mutex> lock(current_mutex_);
    return current_;
}

uint64_t VersionSet::generation() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entry_.has_value() ? entry_->generation : 0;
}

size_t VersionSet::pending_deletions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_deletions_.size();
}

uint64_t VersionSet::allocate_file_number() {
    return next_file_number_.fetch_add(1, std::memory_order_relaxed);
}

void VersionSet::observe_file_number(uint64_t number) {
    // No `fetch_max` on an atomic before C++26, so compare-exchange. Relaxed is enough: the
    // counter's only invariant is that it never hands the same number out twice, and every
    // reader of it is a subsequent `allocate_file_number` on some thread.
    uint64_t current = next_file_number_.load(std::memory_order_relaxed);
    while (current <= number) {
        if (next_file_number_.compare_exchange_weak(current, number + 1,
                                                    std::memory_order_relaxed)) {
            return;
        }
    }
}

void VersionSet::install(std::shared_ptr<const Version> version) {
    installs_.fetch_add(1, std::memory_order_relaxed);
    live_versions_.push_back(version);

    // Published before `current_`, so a reader that sees the new version never reads counts from
    // the old one. The reverse order would let the write path throttle on a level the installed
    // version has already emptied.
    for (int level = 0; level < kPublishedLevels; ++level) {
        const uint32_t count = level < static_cast<int>(version->num_levels())
                                   ? static_cast<uint32_t>(version->file_count(level))
                                   : 0;
        file_counts_[static_cast<size_t>(level)].store(count, std::memory_order_relaxed);
    }

    std::lock_guard<std::mutex> lock(current_mutex_);
    current_ = std::move(version);
}

Status VersionSet::create() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto version = std::make_shared<const Version>();
    if (Status status = write_snapshot_and_install(1, version); status != Status::Ok) {
        return status;
    }
    next_seq_ = 1;
    install(version);
    return Status::Ok;
}

Status VersionSet::write_snapshot_and_install(uint64_t generation,
                                              const std::shared_ptr<const Version>& version) {
    VersionSnapshot snapshot;
    snapshot.next_file_number = next_file_number_.load(std::memory_order_relaxed);
    snapshot.files = version->all_files();
    for (const auto& [level, key] : version->compaction_pointers()) {
        snapshot.compaction_pointers.emplace_back(level, key);
    }
    snapshot.truncation_point = version->truncation_point();
    snapshot.watermark_floor = version->watermark_floor();

    const std::string bytes = encode_version_snapshot(snapshot);
    constexpr int kSnapshotAttempts = 16;
    for (int attempt = 0; attempt < kSnapshotAttempts; ++attempt) {
        auto framed = ManifestPayload::seal(encryption_, generation,
                                            ManifestPayload::snapshot_address(generation),
                                            Slice::from(bytes));
        if (!framed) return framed.error();
        const Status status = catalog_.put_snapshot(generation, Slice::from(*framed)).get();
        if (status == Status::Ok) break;
        if (status != Status::Config) return status;

        // The pointer alone decides ownership. An occupied immutable address with CURRENT still
        // where we last saw it is crash residue or another writer's uncommitted work, so move to a
        // fresh generation and let the CAS below arbitrate between us.
        auto pointer = catalog_.read();
        if (!pointer) return pointer.error();
        const bool unchanged = (!entry_.has_value() && !pointer->has_value()) ||
                               (entry_.has_value() && pointer->has_value() &&
                                entry_->generation == (*pointer)->generation &&
                                entry_->token == (*pointer)->token);
        if (!unchanged) {
            fenced_.store(true, std::memory_order_release);
            return Status::Fenced;
        }

        auto generations = catalog_.list_generations().get();
        if (generations) {
            for (const uint64_t existing : *generations) {
                generation = std::max(generation, existing);
            }
        } else if (generations.error() != Status::Unsupported) {
            return generations.error();
        }
        if (generation == std::numeric_limits<uint64_t>::max()) return Status::Unusable;
        ++generation;
        if (attempt + 1 == kSnapshotAttempts) return Status::Unusable;
    }

    // The pointer install is the commit point: a partially written generation
    // that never gets installed is an orphan, collected later.
    auto installed = catalog_.compare_and_set(entry_, generation);
    if (!installed) {
        if (installed.error() != Status::Io) return installed.error();
        auto observed = catalog_.read();
        if (!observed) return observed.error();
        if (observed->has_value() && (*observed)->generation == generation) {
            installed = *observed;
        } else {
            const bool unchanged = (!entry_.has_value() && !observed->has_value()) ||
                                   (entry_.has_value() && observed->has_value() &&
                                    entry_->generation == (*observed)->generation &&
                                    entry_->token == (*observed)->token);
            if (unchanged) return Status::Io;
            fenced_.store(true, std::memory_order_release);
            return Status::Fenced;
        }
    }
    if (!installed->has_value()) {
        // Another writer got there first. Its version is not ours to merge into.
        fenced_.store(true, std::memory_order_release);
        return Status::Fenced;
    }

    const std::optional<ManifestCatalog::Entry> previous = entry_;
    entry_ = *installed;
    if (previous.has_value()) {
        (void)catalog_.delete_generation(previous->generation).get();
    }
    return Status::Ok;
}

/// How many times `recover` re-reads a pointer that moved under it before giving up. Installing
/// takes four round trips against a manifest another process rolls with one compare-and-set, so
/// losing once is ordinary; losing four times running means the writer is rolling far faster than
/// this reader can read, which is a `Stale` answer rather than a failure.
constexpr int kRecoverAttempts = 4;

/// Loading is four round trips against a manifest that moves: read the pointer, fetch the snapshot,
/// list the edits, fetch each one. A writer may roll a generation and delete the previous one
/// between any two of those steps, and the manifest has no retention window of its own.
///
/// Re-reading the pointer is what distinguishes a rolled generation from a damaged store. Two
/// interleavings depend on it:
///
/// - The snapshot the pointer named is gone. `NotFound` there means the generation was collected,
///   not that the store is corrupt.
/// - The snapshot was fetched but `list_edits` ran after the delete. A missing generation lists as
///   no edits rather than as an error, so the snapshot would install with none of its edits
///   replayed and the view would move backwards.
Status VersionSet::recover() {
    std::lock_guard<std::mutex> lock(mutex_);

    for (int attempt = 0; attempt < kRecoverAttempts; ++attempt) {
        auto pointer = catalog_.read();
        if (!pointer) return pointer.error();
        if (!pointer->has_value()) return Status::NotFound;  // empty store

        const ManifestCatalog::Entry entry = **pointer;
        const uint64_t generation = entry.generation;

        auto snapshot_bytes = catalog_.get_snapshot(generation).get();
        if (!snapshot_bytes) {
            if (snapshot_bytes.error() != Status::NotFound) return snapshot_bytes.error();
            // A snapshot the pointer names and that is not there is either a damaged store or a
            // generation deleted under us. The pointer is what tells the two apart.
            auto moved = catalog_.read();
            if (!moved) return moved.error();
            if (moved->has_value() && (*moved)->generation != generation) continue;
            return Status::Corrupt;
        }
        // Every failure here is hard, unlike an edit's. A snapshot is read before any edit, so
        // a wrong or missing key surfaces at this one call rather than as a replay that quietly
        // stops and opens on a truncated history.
        std::string why;
        auto snapshot_plain = ManifestPayload::open(encryption_, generation,
                                                    ManifestPayload::snapshot_address(generation),
                                                    Slice::from(*snapshot_bytes), why);
        if (!snapshot_plain) {
            if (!why.empty()) last_error_ = why;
            return snapshot_plain.error();
        }
        auto snapshot = decode_version_snapshot(Slice::from(*snapshot_plain));
        if (!snapshot) return snapshot.error();
        if (!encryption_.primary.empty() && !encryption_.accept_plaintext &&
            std::ranges::any_of(snapshot->files, [](const FileMetadata& file) {
                return file.encryption_provider.empty();
            })) {
            last_error_ = "plaintext SST metadata refused while encryption is required";
            return Status::Config;
        }

        std::map<int, std::string> pointers;
        for (const auto& [level, key] : snapshot->compaction_pointers) pointers[level] = key;

        // Held locally until this attempt commits: a discarded attempt must not leave the
        // file-number counter moved, and it only ever rises.
        uint64_t file_number = snapshot->next_file_number;

        VersionEdit initial;
        initial.added = std::move(snapshot->files);

        auto version = Version::apply(Version({}, snapshot->next_file_number, std::move(pointers),
                                              snapshot->truncation_point,
                                              snapshot->watermark_floor),
                                      initial);

        auto seqs = catalog_.list_edits(generation).get();
        if (!seqs) return seqs.error();

        std::sort(seqs->begin(), seqs->end());
        uint64_t expected_seq = 1;
        bool unreadable = false;
        std::string replay_corruption;
        for (uint64_t seq : *seqs) {
            if (seq != expected_seq) break;
            const bool tail = seq == seqs->back();
            auto bytes = catalog_.get_edit(generation, seq).get();
            if (!bytes) {
                if (bytes.error() == Status::NotFound && tail) break;
                if (bytes.error() == Status::NotFound) {
                    replay_corruption = "manifest generation " + std::to_string(generation) +
                                        " edit " + std::to_string(seq) +
                                        " is missing below the listed tail";
                    break;
                }
                unreadable = true;
                break;
            }
            // A payload this process cannot route is not a torn write, and must not be mistaken
            // for one: breaking here would drop every committed edit from this point on and open
            // as though they had never been written.
            std::string edit_why;
            auto plain = ManifestPayload::open(encryption_, generation,
                                               ManifestPayload::edit_address(generation, seq),
                                               Slice::from(*bytes), edit_why);
            if (!plain && (plain.error() == Status::Config ||
                           plain.error() == Status::Unsupported)) {
                if (!edit_why.empty()) last_error_ = edit_why;
                return plain.error();
            }
            if (!plain) {
                if (tail && plain.error() == Status::Corrupt) break;
                last_error_ = "manifest generation " + std::to_string(generation) + " edit " +
                              std::to_string(seq) + " is corrupt below the listed tail";
                return plain.error() == Status::Unsupported ? Status::Unsupported : Status::Corrupt;
            }

            auto edit = decode_version_edit(Slice::from(*plain));
            if (!edit) {
                if (tail && edit.error() == Status::Corrupt) break;
                last_error_ = "manifest generation " + std::to_string(generation) + " edit " +
                              std::to_string(seq) + " does not decode below the listed tail";
                return edit.error() == Status::Unsupported ? Status::Unsupported : Status::Corrupt;
            }
            if (!encryption_.primary.empty() && !encryption_.accept_plaintext &&
                std::ranges::any_of(edit->added, [](const FileMetadata& file) {
                    return file.encryption_provider.empty();
                })) {
                last_error_ = "plaintext SST metadata refused while encryption is required";
                return Status::Config;
            }

            version = Version::apply(*version, *edit);
            if (edit->next_file_number > file_number) file_number = edit->next_file_number;
            ++expected_seq;
        }
        if (unreadable) {
            // Same question as a missing snapshot: rolled under us, or genuinely unreadable.
            auto moved = catalog_.read();
            if (moved->has_value() && (*moved)->generation != generation) continue;
            return Status::Io;
        }
        if (!replay_corruption.empty()) {
            auto moved = catalog_.read();
            if (!moved) return moved.error();
            if (moved && moved->has_value() && (*moved)->generation != generation) continue;
            last_error_ = std::move(replay_corruption);
            return Status::Corrupt;
        }

        // Re-read the pointer before installing anything. Everything above was read across
        // four separate round trips; if the generation moved during them, what was assembled is a
        // mixture rather than a snapshot, and the empty-edit-list case above makes that mixture
        // look perfectly well formed.
        auto after = catalog_.read();
        if (!after) return after.error();
        if (!after->has_value()) return Status::NotFound;
        if ((*after)->generation != generation) continue;

        // Monotone or nothing. Generations only advance and edits only accumulate, so this cannot
        // fire while the store is well formed — it is here because "the reader went backwards" is
        // the failure the case above produced silently, and a checked property beats a hoped-for
        // one.
        if (entry_.has_value() && (generation < entry_->generation ||
                                   (generation == entry_->generation && expected_seq < next_seq_))) {
            return Status::Corrupt;
        }

        entry_ = entry;
        next_seq_ = expected_seq;
        next_file_number_.store(file_number, std::memory_order_relaxed);
        install(version);
        return Status::Ok;
    }

    // Lost every attempt. Nothing is wrong with the store and nothing is wrong with this reader;
    // it is simply behind, which is what `Stale` says.
    return Status::Stale;
}

Status VersionSet::apply(VersionEdit edit) {
    if (fenced()) return Status::Fenced;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!entry_.has_value()) return Status::Unusable;

    edit.next_file_number = next_file_number_.load(std::memory_order_relaxed);

    const std::string bytes = encode_version_edit(edit);
    const std::string address = ManifestPayload::edit_address(entry_->generation, next_seq_);
    auto framed =
        ManifestPayload::seal(encryption_, entry_->generation, address, Slice::from(bytes));
    if (!framed) return framed.error();
    if (Status status =
            catalog_.put_edit(entry_->generation, next_seq_, Slice::from(*framed)).get();
        status != Status::Ok) {
        // Nothing is swapped: the current version still describes what is on disk.
        //
        // An occupied edit address means another writer owns this store.
        // `next_seq_` is engine-owned and monotonic, so nothing this instance has
        // done can have put an object where it is about to write. Reporting `Config` would leave
        // `fenced_` clear, so the writer would carry on with a stale view until its next generation
        // roll.
        //
        // Misread case: a put that succeeded remotely but reported a timeout, retried by the
        // caller, collides with its own edit and is fenced when it was not. That costs a reopen,
        // which re-reads the true state — cheaper than leaving a second live writer undetected.
        if (status == Status::Config) {
            fenced_.store(true, std::memory_order_release);
            return Status::Fenced;
        }
        return status;
    }
    ++next_seq_;

    auto base = current();

    // This does not validate that `edit.deleted` is still live, and a second concurrent
    // deleting task would be unsound because of it: two tasks picking inputs from the same
    // version snapshot can both commit, producing a double delete or a compaction reading a
    // file a migration has already moved and unlinked. Today it holds because compaction,
    // migration and capacity eviction share one executor and run one at a time — asserted
    // under `ELYSIUMKV_PARANOID` in `DbImpl`, so adding a second deleting worker fails on the
    // first test run instead of in production. Flush is exempt: it only adds.
    //
    // A second deleting worker therefore needs either a set of files claimed by a running
    // task, or optimistic validation here, before the worker — not after.

    // Capture the metadata of what is being removed *before* the swap: the
    // store_id says where the object physically lives, and after the swap it is
    // no longer in any version.
    //
    // A trivial move deletes and re-adds the *same* file number at a new level
    // (ARCHITECTURE.md "Compaction"): the object does not go anywhere, so it is not an obsolete object.
    // Queueing it would leave it pending forever, since the new version still
    // references it.
    //
    // A migration looks nothing like this, because ARCHITECTURE.md "The manifest is snapshots plus edits" forbids reusing a file
    // number: it adds a new number and deletes the old one, so the old object is
    // queued exactly as any compaction input would be.
    std::set<uint64_t> re_added;
    for (const FileMetadata& file : edit.added) re_added.insert(file.file_number);

    for (const FileRef& ref : edit.deleted) {
        if (re_added.count(ref.file_number) != 0) continue;
        for (const FileMetadata& file : base->files_at(ref.level)) {
            if (file.file_number == ref.file_number) {
                pending_deletions_.push_back(
                    {file, clock_ ? clock_() : 0});
                pending_deletions_hint_.store(pending_deletions_.size(),
                                              std::memory_order_relaxed);
                break;
            }
        }
    }

    auto next = Version::apply(*base, edit);
    install(next);
    // Drop our own reference to the superseded version before sweeping, or it
    // counts as a live reader of the files this edit just removed.
    base.reset();

    if (Status status = maybe_roll_generation(next); status != Status::Ok) {
        last_error_ = "manifest edit committed, but generation roll failed (" +
                      std::string(status_name(status)) + ")";
    }

    collect_obsolete_locked();
    return Status::Ok;
}

Status VersionSet::roll_generation_now() {
    if (fenced()) return Status::Fenced;

    std::lock_guard<std::mutex> lock(mutex_);
    if (!entry_.has_value()) return Status::Unusable;
    // Nothing has been written into this generation, so its snapshot is already the newest thing
    // there is and rolling would produce an identical one under a new number.
    if (next_seq_ == 1) return Status::Ok;

    const uint64_t next_generation = entry_->generation + 1;
    if (Status status = write_snapshot_and_install(next_generation, current());
        status != Status::Ok) {
        return status;
    }
    next_seq_ = 1;
    return Status::Ok;
}

Status VersionSet::maybe_roll_generation(const std::shared_ptr<const Version>& version) {
    if (next_seq_ <= static_cast<uint64_t>(edits_per_generation_)) return Status::Ok;

    const uint64_t next_generation = entry_->generation + 1;
    if (Status status = write_snapshot_and_install(next_generation, version);
        status != Status::Ok) {
        return status;
    }
    next_seq_ = 1;
    return Status::Ok;
}

void VersionSet::collect_obsolete() {
    std::lock_guard<std::mutex> lock(mutex_);
    collect_obsolete_locked();
}

size_t VersionSet::tracked_versions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return live_versions_.size();
}

void VersionSet::collect_obsolete_locked() {
    // Prune before the early return, not after it. This is the only place expired entries
    // leave `live_versions_`, and a flush-only edit deletes nothing — so a store that never
    // compacts (a single configured level makes the picker return nothing at all) grew the vector
    // by one entry per flush forever, each retained `weak_ptr` also holding a control block alive.
    std::erase_if(live_versions_, [](const std::weak_ptr<const Version>& weak) {
        return weak.expired();
    });

    if (deleter_ == nullptr || pending_deletions_.empty()) return;

    // Every file number any live version still references. An iterator holding a
    // Version is exactly what keeps a file alive past its removal.
    std::set<uint64_t> referenced;
    std::vector<std::weak_ptr<const Version>> alive;
    alive.reserve(live_versions_.size());
    for (const auto& weak : live_versions_) {
        if (auto version = weak.lock()) {
            for (const FileMetadata& file : version->all_files()) {
                referenced.insert(file.file_number);
            }
            alive.push_back(weak);
        }
    }
    live_versions_ = std::move(alive);

    // The retention window, and it is checked before anything else about the object. A reader
    // in another process is invisible here — that is the whole reason this delay exists — so the
    // only thing standing between a compaction and a reader's vanished file is the clock.
    const uint64_t now = clock_ ? clock_() : 0;
    const auto retention_ms = static_cast<uint64_t>(obsolete_retention_.count());

    std::vector<PendingDeletion> still_pending;
    std::vector<FileMetadata> collectable;
    for (const PendingDeletion& pending : pending_deletions_) {
        const FileMetadata& file = pending.file;
        if (referenced.count(file.file_number) != 0) {
            still_pending.push_back(pending);  // an iterator is still reading it
            continue;
        }
        if (retention_ms > 0 && now >= pending.unreferenced_since_ms &&
            now - pending.unreferenced_since_ms < retention_ms) {
            still_pending.push_back(pending);  // not yet out of the reader window
            continue;
        }
        // The edit recording this removal is already durable — the ordering
        // rule is unconditional, and this is the second half of it.
        collectable.push_back(file);
    }

    // One call for the whole set, not one per file. A compaction routinely
    // obsoletes dozens of objects, and against a remote store the per-file shape
    // was one HTTP round trip each.
    if (!collectable.empty()) {
        // A file the deleter could not remove goes back on the queue with its window already
        // spent, so the next pass retries it rather than restarting its retention.
        for (FileMetadata& file : deleter_(collectable)) {
            still_pending.push_back({std::move(file), 0});
        }
    }
    pending_deletions_ = std::move(still_pending);
    pending_deletions_hint_.store(pending_deletions_.size(), std::memory_order_relaxed);
}

bool VersionSet::manifest_advanced() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!entry_.has_value()) return false;

    auto pointer = catalog_.read();
    if (!pointer || !pointer->has_value()) return false;
    if ((*pointer)->generation != entry_->generation) return true;   // rolled

    auto seqs = catalog_.list_edits(entry_->generation).get();
    if (!seqs) return false;
    for (uint64_t seq : *seqs) {
        if (seq >= next_seq_) return true;   // an edit this instance has not replayed
    }
    return false;
}

std::set<uint64_t> VersionSet::referenced_file_numbers() const {
    std::set<uint64_t> numbers;
    for (const FileMetadata& file : current()->all_files()) numbers.insert(file.file_number);
    return numbers;
}

std::set<uint64_t> VersionSet::pending_file_numbers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::set<uint64_t> numbers;
    for (const PendingDeletion& pending : pending_deletions_) {
        numbers.insert(pending.file.file_number);
    }
    return numbers;
}

}  // namespace elysiumkv
