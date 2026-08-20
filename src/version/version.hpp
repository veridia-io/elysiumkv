#ifndef ELYSIUMKV_VERSION_VERSION_HPP
#define ELYSIUMKV_VERSION_VERSION_HPP

#include "version/version_edit.hpp"

#include <map>
#include <optional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Versions are immutable snapshots" — immutable after construction. Every mutation produces a *new* Version;
/// nothing is ever edited in place, which is what lets an iterator hold one for
/// its whole lifetime without a lock.
class Version {
public:
    Version() = default;
    Version(std::vector<std::vector<FileMetadata>> levels, uint64_t next_file_number,
            std::map<int, std::string> compaction_pointers, std::string truncation_point = {},
            std::optional<WatermarkFloor> watermark_floor = std::nullopt)
        : levels_(std::move(levels)),
          next_file_number_(next_file_number),
          compaction_pointers_(std::move(compaction_pointers)),
          truncation_point_(std::move(truncation_point)),
          watermark_floor_(watermark_floor) {
        carries_ranges_.resize(levels_.size(), false);
        for (size_t level = 0; level < levels_.size(); ++level) {
            for (const FileMetadata& file : levels_[level]) {
                if (file.num_range_tombstones != 0) {
                    carries_ranges_[level] = true;
                    break;
                }
            }
        }
    }

    const std::vector<std::vector<FileMetadata>>& levels() const { return levels_; }

    /// Whether any file at this level carries range tombstones.
    ///
    /// What it licenses is a binary search on the read path. Below L0 the *data* spans are
    /// disjoint and sorted, so at most one file can hold a key — but a range tombstone span is
    /// neither bounded by its file's data span nor disjoint from its neighbours', so a file whose
    /// keys are elsewhere can still be the one that answers. When no file here carries any, that
    /// second reason to open a file cannot arise and the level is searchable rather than scannable.
    /// Computed once, at construction, because a Version is immutable.
    ///
    /// The negative control is `DeleteRange.ATombstoneSurvivesACompactionThatDoesNotReachWhatItShadows`:
    /// force this to `false` and that test fails, because the search then walks past the file
    /// whose tombstone answers the lookup.
    bool carries_ranges(int level) const {
        return level >= 0 && static_cast<size_t>(level) < carries_ranges_.size() &&
               carries_ranges_[static_cast<size_t>(level)];
    }
    size_t num_levels() const { return levels_.size(); }
    const std::vector<FileMetadata>& files_at(int level) const;

    uint64_t next_file_number() const { return next_file_number_; }
    const std::map<int, std::string>& compaction_pointers() const { return compaction_pointers_; }

    /// Keys below this are gone. Carried on the Version rather than on the DB so that an iterator,
    /// which already pins a Version for its lifetime, keeps reading the world as it was when it
    /// started — the same rule that keeps its files alive.
    const std::string& truncation_point() const { return truncation_point_; }

    /// How far this store can still certify its embedder's log, when the files alone would
    /// over-report it.
    ///
    /// Recovery normally answers that from the files: with nothing discarded, `max(high)` over
    /// them is the newest established watermark and every write below it is still in some file.
    /// That argument has a premise — *no state was lost* — and a discard falsifies it permanently,
    /// while `anything_discarded` only ever describes the recovery that observed it. The discard
    /// is itself a manifest edit that removes the lost files, so by the *next* open there is no
    /// lost set left to reason from and `max(high)` is taken again, reporting a frontier the store
    /// can no longer support. Writes above the true floor lived only in the discarded files.
    ///
    /// So the one number the loss produced is written down in the same edit that removes them.
    /// Monotone, and raised by each file flushed afterwards to that file's `high`: raising rather
    /// than clearing is what makes a *partial* replay safe, since a crash after re-materialising
    /// only part of the gap must not restore the old, higher answer.
    std::optional<WatermarkFloor> watermark_floor() const { return watermark_floor_; }
    bool truncated(Slice key) const {
        return !truncation_point_.empty() && key < Slice::from(truncation_point_);
    }
    /// Files no surviving key can be read from: reclaimable whole, with no rewrite.
    std::vector<FileMetadata> files_entirely_truncated() const;

    /// A file that a newer file's range tombstones may cover completely, paired with the file
    /// whose tombstones might do it.
    struct RangeDropCandidate {
        FileMetadata file;
        FileMetadata cover;
        /// The cover carries exactly one range, so the span the manifest records is that range
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
    /// A shortlist, not an answer. Every candidate here is soundly rejected-or-admitted by the
    /// manifest alone, so a caller that reads nothing may act on the `exact` ones and drop the rest;
    /// a caller with store access settles the rest by reading one block per cover.
    std::vector<RangeDropCandidate> range_drop_candidates() const;

    /// Files whose newest write is at or before `cutoff` and which can therefore be unlinked
    /// whole. See `Options::ttl` for what this does and does not promise.
    std::vector<FileMetadata> files_expired_before(uint64_t cutoff) const;

private:
    /// Whether any file older than `file` — a deeper level, or the same level and a lower number —
    /// overlaps its key range. The soundness condition for dropping a file by age: remove one
    /// that shadows an older version of the same key and the older version comes back, which is a
    /// resurrection rather than an expiry.
    bool older_file_overlaps(const FileMetadata& file) const;

public:

    uint64_t total_bytes(int level) const;
    size_t file_count(int level) const;
    /// Oldest write held anywhere at this level, or 0 when the level is empty.
    uint64_t oldest_write_time_ms(int level) const;

    /// Files whose `[smallest, largest]` intersects the half-open interval
    /// `[lower, upper)`; an empty `upper` means "to the end of the keyspace".
    /// This is the *read* shape — ARCHITECTURE.md "Absence is an answer, not an error" makes iterator bounds half-open.
    std::vector<FileMetadata> overlapping_half_open(int level, Slice lower, Slice upper) const;

    /// The same set as `overlapping_half_open`, as a `[begin, end)` index range into
    /// `files_at(level)` — for a level whose files are sorted and disjoint and which carries no
    /// range tombstones, where the answer is necessarily contiguous. Two binary searches instead
    /// of a scan, and no copy: an unbounded scan of a 697-file level copied every entry, two
    /// strings each, to build a list it then walked one file at a time.
    ///
    /// Undefined for L0 and for a level with range tombstones; the caller checks `carries_ranges`.
    std::pair<size_t, size_t> overlapping_index_range(int level, Slice lower, Slice upper) const;

    /// `[begin, end)` into `files_at(level)` whose data span intersects the closed interval
    /// `[first, last]`.
    ///
    /// Distinct from `overlapping_index_range` in two ways that both matter: the interval is
    /// closed, matching a file's own `smallest..largest`; and it consults the data span only, so
    /// unlike that one it is valid for a level carrying range tombstones — a level's data spans
    /// stay sorted and disjoint whatever its tombstone spans do. Undefined for L0, whose files
    /// overlap by construction.
    std::pair<size_t, size_t> data_span_index_range(int level, Slice first, Slice last) const;

    /// Files whose `[smallest, largest]` intersects the closed interval
    /// `[first, last]`. This is the *compaction* shape, and the distinction is
    /// load-bearing rather than pedantic: a file's `smallest_key..largest_key`
    /// includes both ends, so asking the half-open question about it silently
    /// misses an output-level file that begins exactly where the input ends.
    /// The compaction then writes its output beside the file it should have
    /// merged, leaving two files covering that key at a level required to be
    /// non-overlapping — and a committed write reverts to its previous value.
    /// There is no "unbounded" sentinel here: an empty key is a key, not a flag.
    std::vector<FileMetadata> overlapping_inclusive(int level, Slice first, Slice last) const;

    /// Whether any file at any level still holds data in `[lower, upper)`.
    ///
    /// Data spans only, deliberately. A file carrying a range tombstone over the band overlaps
    /// it by its *effective* span while holding no keys there at all — matching on that would mean
    /// the answer could never become "nothing left", because the tombstone is itself the thing
    /// keeping the band deleted.
    ///
    /// Conservative in the safe direction: a span is a hull, so a file can overlap without holding
    /// a single key inside the band. The answer is therefore "there might still be data here", and
    /// `false` is the one that carries information.
    bool any_file_holds(Slice lower, Slice upper) const;

    /// Every file in the version, in level order.
    std::vector<FileMetadata> all_files() const;

    /// Applies an edit and returns the resulting version. L1+ files are kept
    /// sorted by smallest key (they are non-overlapping); L0 by descending file
    /// number, which is the recency order the merging iterator relies on (ARCHITECTURE.md "Positional recency").
    static std::shared_ptr<const Version> apply(const Version& base, const VersionEdit& edit);

private:
    std::vector<std::vector<FileMetadata>> levels_;
    std::vector<bool> carries_ranges_;
    uint64_t next_file_number_ = 1;
    std::map<int, std::string> compaction_pointers_;
    std::string truncation_point_;
    std::optional<WatermarkFloor> watermark_floor_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_VERSION_HPP
