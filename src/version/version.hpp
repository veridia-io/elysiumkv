#ifndef ELYSIUMKV_VERSION_VERSION_HPP
#define ELYSIUMKV_VERSION_VERSION_HPP

#include "version/version_edit.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Versions are immutable snapshots" — immutable after construction. Every mutation produces a *new* Version;
/// nothing is ever edited in place, which is what lets an iterator hold one for
/// its whole lifetime without a lock.
class Version {
public:
    Version() = default;
    Version(std::vector<std::vector<FileMetadata>> levels, uint64_t next_file_number,
            std::map<int, std::string> compaction_pointers, std::string truncation_point = {})
        : levels_(std::move(levels)),
          next_file_number_(next_file_number),
          compaction_pointers_(std::move(compaction_pointers)),
          truncation_point_(std::move(truncation_point)) {}

    const std::vector<std::vector<FileMetadata>>& levels() const { return levels_; }
    size_t num_levels() const { return levels_.size(); }
    const std::vector<FileMetadata>& files_at(int level) const;

    uint64_t next_file_number() const { return next_file_number_; }
    const std::map<int, std::string>& compaction_pointers() const { return compaction_pointers_; }

    /// Keys below this are gone. Carried on the Version rather than on the DB so that an iterator,
    /// which already pins a Version for its lifetime, keeps reading the world as it was when it
    /// started — the same rule that keeps its files alive.
    const std::string& truncation_point() const { return truncation_point_; }
    bool truncated(Slice key) const {
        return !truncation_point_.empty() && key < Slice::from(truncation_point_);
    }
    /// Files no surviving key can be read from: reclaimable whole, with no rewrite.
    std::vector<FileMetadata> files_entirely_truncated() const;

    /// A file that a **newer** file's range tombstones may cover completely, paired with the file
    /// whose tombstones might do it.
    struct RangeDropCandidate {
        FileMetadata file;
        FileMetadata cover;
        /// The cover carries exactly one range, so the span the manifest records **is** that range
        /// and no further check is needed. With two or more that span is a hull with gaps in it,
        /// and a hull can show a file is *not* covered but never that it is — so the tombstones
        /// themselves have to be read, which a `Version` cannot do.
        bool exact = false;
    };

    /// Files that could be unlinked whole because a newer file's range deletes cover them —
    /// nothing read, nothing rewritten, one edit. This is what makes evicting a tenant cheap rather
    /// than merely expressible, and it is `files_entirely_truncated` generalised from a floor to a
    /// range.
    ///
    /// **A shortlist, not an answer.** Every candidate here is soundly rejected-or-admitted by the
    /// manifest alone, so a caller that reads nothing may act on the `exact` ones and drop the rest;
    /// a caller with store access settles the rest by reading one block per cover.
    std::vector<RangeDropCandidate> range_drop_candidates() const;

    /// Files whose **newest** write is at or before `cutoff` and which can therefore be unlinked
    /// whole. See `Options::ttl` for what this does and does not promise.
    std::vector<FileMetadata> files_expired_before(uint64_t cutoff) const;

private:
    /// Whether any file older than `file` — a deeper level, or the same level and a lower number —
    /// overlaps its key range. **The soundness condition for dropping a file by age**: remove one
    /// that shadows an older version of the same key and the older version comes back, which is a
    /// resurrection rather than an expiry.
    bool older_file_overlaps(const FileMetadata& file) const;

public:

    uint64_t total_bytes(int level) const;
    size_t file_count(int level) const;
    /// Oldest write held anywhere at this level, or 0 when the level is empty.
    uint64_t oldest_write_time_ms(int level) const;

    /// Files whose `[smallest, largest]` intersects the **half-open** interval
    /// `[lower, upper)`; an empty `upper` means "to the end of the keyspace".
    /// This is the *read* shape — ARCHITECTURE.md "Absence is an answer, not an error" makes iterator bounds half-open.
    std::vector<FileMetadata> overlapping_half_open(int level, Slice lower, Slice upper) const;

    /// Files whose `[smallest, largest]` intersects the **closed** interval
    /// `[first, last]`. This is the *compaction* shape, and the distinction is
    /// load-bearing rather than pedantic: a file's `smallest_key..largest_key`
    /// includes both ends, so asking the half-open question about it silently
    /// misses an output-level file that begins exactly where the input ends.
    /// The compaction then writes its output beside the file it should have
    /// merged, leaving two files covering that key at a level required to be
    /// non-overlapping — and a committed write reverts to its previous value.
    /// There is no "unbounded" sentinel here: an empty key is a key, not a flag.
    std::vector<FileMetadata> overlapping_inclusive(int level, Slice first, Slice last) const;

    /// Every file in the version, in level order.
    std::vector<FileMetadata> all_files() const;

    /// Applies an edit and returns the resulting version. L1+ files are kept
    /// sorted by smallest key (they are non-overlapping); L0 by descending file
    /// number, which is the recency order the merging iterator relies on (ARCHITECTURE.md "Positional recency").
    static std::shared_ptr<const Version> apply(const Version& base, const VersionEdit& edit);

private:
    std::vector<std::vector<FileMetadata>> levels_;
    uint64_t next_file_number_ = 1;
    std::map<int, std::string> compaction_pointers_;
    std::string truncation_point_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_VERSION_HPP
