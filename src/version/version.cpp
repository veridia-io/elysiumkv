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
        if (Slice::from(file.largest_key) < lower) continue;
        if (!upper.empty() && upper <= Slice::from(file.smallest_key)) continue;
        result.push_back(file);
    }
    return result;
}

std::vector<FileMetadata> Version::overlapping_inclusive(int level, Slice first, Slice last) const {
    std::vector<FileMetadata> result;
    for (const FileMetadata& file : files_at(level)) {
        if (Slice::from(file.largest_key) < first) continue;
        if (last < Slice::from(file.smallest_key)) continue;
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
            // The bound is the file's own largest key, so a file is only dead when *every* key in
            // it is below the point. A file straddling the point keeps its live half and is
            // narrowed by compaction instead.
            if (Slice::from(file.largest_key) < point) dead.push_back(file);
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
