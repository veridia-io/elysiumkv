#include "version/version.hpp"

#include <algorithm>
#include <memory>

namespace elysiumkv {
namespace {

const std::vector<FileMetadata> kNoFiles;

/// L0 files may overlap, so recency decides: higher file number first (ARCHITECTURE.md "Compaction").
/// Deeper levels are non-overlapping and sort by key.
void sort_level(int level, std::vector<FileMetadata>& files) {
    if (level == 0) {
        std::sort(files.begin(), files.end(), [](const FileMetadata& a, const FileMetadata& b) {
            return a.file_number > b.file_number;
        });
    } else {
        std::sort(files.begin(), files.end(), [](const FileMetadata& a, const FileMetadata& b) {
            if (a.smallest_key != b.smallest_key) return a.smallest_key < b.smallest_key;
            return a.file_number < b.file_number;
        });
    }
}

}  // namespace

const std::vector<FileMetadata>& Version::files_at(int level) const {
    if (level < 0 || static_cast<size_t>(level) >= levels_.size()) return kNoFiles;
    return levels_[static_cast<size_t>(level)];
}

uint64_t Version::total_bytes(int level) const {
    uint64_t total = 0;
    for (const FileMetadata& file : files_at(level)) total += file.file_bytes;
    return total;
}

size_t Version::file_count(int level) const { return files_at(level).size(); }

uint64_t Version::oldest_write_time_ms(int level) const {
    uint64_t oldest = 0;
    for (const FileMetadata& file : files_at(level)) {
        if (oldest == 0 || file.min_write_time_ms < oldest) oldest = file.min_write_time_ms;
    }
    return oldest;
}

std::vector<FileMetadata> Version::overlapping_half_open(int level, Slice lower,
                                                          Slice upper) const {
    std::vector<FileMetadata> result;
    for (const FileMetadata& file : files_at(level)) {
        // A file earns its place if either span overlaps. **The tombstone span is checked
        // separately because it is not bounded by the data span**: a file can delete a range it
        // holds no keys in, and pruning on the data span alone would drop the very file that
        // answers the scan — silently, by returning keys the range delete removed.
        const bool data_overlaps =
            !(Slice::from(file.largest_key) < lower) &&
            !(!upper.empty() && upper <= Slice::from(file.smallest_key));
        const bool ranges_overlap =
            file.num_range_tombstones != 0 &&
            !(Slice::from(file.largest_range_key) <= lower) &&
            !(!upper.empty() && upper <= Slice::from(file.smallest_range_key));
        if (data_overlaps || ranges_overlap) result.push_back(file);
    }
    return result;
}

bool Version::any_file_holds(Slice lower, Slice upper) const {
    if (!upper.empty() && !(lower < upper)) return false;   // empty band holds nothing
    for (const auto& level : levels_) {
        for (const FileMetadata& file : level) {
            if (file.num_entries == 0) continue;   // carries tombstones and no keys
            if (Slice::from(file.largest_key) < lower) continue;
            if (!upper.empty() && !(Slice::from(file.smallest_key) < upper)) continue;
            return true;
        }
    }
    return false;
}

std::vector<FileMetadata> Version::overlapping_inclusive(int level, Slice first, Slice last) const {
    std::vector<FileMetadata> result;
    for (const FileMetadata& file : files_at(level)) {
        // **The candidate's effective span, not its data span** — the same reason the caller passes
        // an effective span in. A file carrying only range tombstones has no data span at all, so
        // matching on data alone leaves it behind: the L0 file next to it compacts down without it,
        // and the tombstone is then at a shallower level than data written *after* it. Positional
        // recency then says the tombstone is newer, and it deletes writes that came later.
        const std::string file_first = file.effective_smallest();
        const std::string file_last = file.effective_largest();
        if (Slice::from(file_last) < first) continue;
        if (last < Slice::from(file_first)) continue;
        result.push_back(file);
    }
    return result;
}

std::vector<FileMetadata> Version::all_files() const {
    std::vector<FileMetadata> result;
    for (const auto& level : levels_) result.insert(result.end(), level.begin(), level.end());
    return result;
}

std::vector<FileMetadata> Version::files_entirely_truncated() const {
    std::vector<FileMetadata> dead;
    if (truncation_point_.empty()) return dead;
    const Slice point = Slice::from(truncation_point_);
    for (const auto& files : levels_) {
        for (const FileMetadata& file : files) {
            // The bound is the file's own largest *effective* key, so a file is only dead when
            // everything it has to say is below the point. A file straddling the point keeps its
            // live half and is narrowed by compaction instead.
            //
            // **Effective, because a file with no entries has an empty largest key** — and the
            // empty key sorts below every truncation point, so a file carrying nothing but a range
            // tombstone read as entirely truncated and was unlinked. The tombstone went with it and
            // every key it covered came back. The differential suite found this; nothing about a
            // file that deletes without holding anything is visible from the data span alone.
            if (Slice::from(file.effective_largest()) < point) dead.push_back(file);
        }
    }
    return dead;
}

std::vector<Version::RangeDropCandidate> Version::range_drop_candidates() const {
    std::vector<RangeDropCandidate> candidates;

    // Covers first: any file carrying range tombstones at all. The span the manifest records is the
    // hull of them — exactly the tombstone when there is one, a bounding interval with unknown gaps
    // when there are more. Used here only to *reject*, which a hull can always do soundly.
    std::vector<const FileMetadata*> covers;
    for (const auto& files : levels_) {
        for (const FileMetadata& file : files) {
            if (file.num_range_tombstones != 0) covers.push_back(&file);
        }
    }
    if (covers.empty()) return candidates;

    for (const auto& files : levels_) {
        for (const FileMetadata& file : files) {
            // A file carrying tombstones of its own is left alone: dropping it would drop those
            // too, and they shadow files this cover says nothing about.
            if (file.num_range_tombstones != 0) continue;
            if (file.num_entries == 0) continue;
            for (const FileMetadata* cover : covers) {
                // Strictly newer, in the one order this engine has: a lower level, or the same
                // level and a higher file number. Equal is not newer — a tombstone shadows nothing
                // in the file that carries it, and here that file *is* the candidate.
                const bool newer = cover->level < file.level ||
                                   (cover->level == file.level &&
                                    cover->file_number > file.file_number);
                if (!newer) continue;
                // Every key in the file, inclusive of its largest, must fall inside the half-open
                // range — so the upper bound has to sit strictly above the largest key.
                if (Slice::from(cover->smallest_range_key) <= Slice::from(file.smallest_key) &&
                    Slice::from(file.largest_key) < Slice::from(cover->largest_range_key)) {
                    candidates.push_back(RangeDropCandidate{
                        file, *cover, cover->num_range_tombstones == 1});
                    break;
                }
            }
        }
    }
    return candidates;
}

bool Version::older_file_overlaps(const FileMetadata& file) const {
    for (const auto& others : levels_) {
        for (const FileMetadata& other : others) {
            const bool older = other.level > file.level ||
                               (other.level == file.level && other.file_number < file.file_number);
            if (!older) continue;
            if (Slice::from(other.largest_key) < Slice::from(file.smallest_key)) continue;
            if (Slice::from(file.largest_key) < Slice::from(other.smallest_key)) continue;
            return true;
        }
    }
    return false;
}

std::vector<FileMetadata> Version::files_expired_before(uint64_t cutoff) const {
    std::vector<FileMetadata> dead;
    for (const auto& files : levels_) {
        for (const FileMetadata& file : files) {
            // Unknown never expires. Nothing should reach here with a zero — every write path sets
            // it — but guessing "very old" from an absent value would delete a whole file.
            if (file.max_write_time_ms == 0) continue;
            // The *newest* write, so the file goes only once everything in it has outlived the
            // limit. Keyed on the oldest, a file would be dropped while still holding fresh data.
            if (file.max_write_time_ms > cutoff) continue;
            // Its range tombstones would go with it, and they shadow files this says nothing about.
            if (file.num_range_tombstones != 0) continue;
            if (older_file_overlaps(file)) continue;
            dead.push_back(file);
        }
    }
    return dead;
}

std::shared_ptr<const Version> Version::apply(const Version& base, const VersionEdit& edit) {
    std::vector<std::vector<FileMetadata>> levels = base.levels_;

    for (const FileRef& ref : edit.deleted) {
        if (ref.level < 0 || static_cast<size_t>(ref.level) >= levels.size()) continue;
        auto& files = levels[static_cast<size_t>(ref.level)];
        std::erase_if(files, [&](const FileMetadata& file) {
            return file.file_number == ref.file_number;
        });
    }

    for (const FileMetadata& file : edit.added) {
        if (file.level < 0) continue;
        if (static_cast<size_t>(file.level) >= levels.size()) {
            levels.resize(static_cast<size_t>(file.level) + 1);
        }
        levels[static_cast<size_t>(file.level)].push_back(file);
    }

    for (size_t level = 0; level < levels.size(); ++level) {
        sort_level(static_cast<int>(level), levels[level]);
    }

    std::map<int, std::string> pointers = base.compaction_pointers_;
    for (const auto& [level, key] : edit.compaction_pointers) pointers[level] = key;

    const uint64_t next_file_number =
        std::max(base.next_file_number_, edit.next_file_number);

    // Monotone: an edit can only move the point forward. Replaying the manifest is therefore
    // idempotent, and an edit that arrives after a later one cannot bring truncated keys back.
    std::string truncation_point = base.truncation_point_;
    if (edit.truncation_point > truncation_point) truncation_point = edit.truncation_point;

    return std::make_shared<const Version>(std::move(levels), next_file_number,
                                           std::move(pointers), std::move(truncation_point));
}

}  // namespace elysiumkv
