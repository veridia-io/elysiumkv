#ifndef ELYSIUMKV_COMPACT_PICKER_HPP
#define ELYSIUMKV_COMPACT_PICKER_HPP

#include "db/level_config.hpp"
#include "version/version.hpp"

#include <optional>
#include <string>
#include <vector>

namespace elysiumkv {

/// One chosen compaction: inputs at `level`, merged with the overlapping files
/// already at `output_level`. ARCHITECTURE.md "Compaction" — no file is ever "promoted" individually.
struct Compaction {
    int level = 0;
    int output_level = 0;
    std::vector<FileMetadata> inputs;    ///< files at `level`
    std::vector<FileMetadata> overlaps;  ///< files at `output_level`

    /// Files at output_level + 1. Not inputs — output cutting consults them to
    /// bound the cost of the *next* compaction.
    std::vector<FileMetadata> grandparents;

    /// ARCHITECTURE.md "Compaction" — **bottommost for this compaction's key range**: no level deeper than
    /// `output_level` holds a file overlapping it. Dynamic, evaluated per
    /// compaction, and the sole condition for dropping tombstones.
    bool output_is_bottommost = false;

    /// The seed overlaps nothing at the output level, so the file can be
    /// re-pointed instead of rewritten. A move changes no bytes and no tier.
    bool trivial_move = false;
    double score = 0.0;

    /// Every input and overlap, oldest-source-last: the order the merging
    /// iterator needs, since recency is positional (ARCHITECTURE.md "Positional recency").
    std::vector<FileMetadata> all_inputs() const;
    /// min() over inputs — a compaction output inherits the oldest write it
    /// contains (ARCHITECTURE.md "The manifest is snapshots plus edits"), which is also what places it on a tier (ARCHITECTURE.md "A tier is not a level").
    uint64_t min_write_time_ms() const;
    uint64_t input_bytes() const;
    /// Largest key covered; becomes the level's persisted compaction pointer.
    std::string largest_key() const;
};

/// ARCHITECTURE.md "Compaction" — **score is the only trigger.** Per level, the maximum of whichever
/// ratios are configured, `file_count / max_files` and `total_bytes / max_bytes`;
/// highest above 1.0 wins. The last level never triggers — it absorbs everything
/// and has nowhere to spill to.
///
/// There is no age trigger: age governs tier migration (ARCHITECTURE.md "Migration between tiers"), not compaction.
/// One-time rewrites are `compact_level()` (ARCHITECTURE.md "Absence is an answer, not an error"), which terminates.
std::optional<Compaction> pick_compaction(const Version& version, const ResolvedLevels& config,
                                          size_t max_compaction_bytes);

/// ARCHITECTURE.md "Compaction" — no level deeper than `output_level` holds a file overlapping
/// `[lower, upper]`. One range-overlap check, not a per-key test.
///
/// This must not be the *last* level statically: a store that never grows past
/// L1 leaves the deeper levels empty forever, so a static rule would never drop
/// a tombstone and deletions would accumulate without bound.
bool is_bottommost_for_range(const Version& version, int output_level, int last_level, Slice lower,
                             Slice upper);

/// The cut threshold for grandparent overlap: 10 × the output level's target
/// file size (ARCHITECTURE.md "Compaction").
size_t max_grandparent_overlap_bytes(const ResolvedLevel& output_level);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_COMPACT_PICKER_HPP
