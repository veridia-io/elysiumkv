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

    /// ARCHITECTURE.md "Compaction" — bottommost for this compaction's key range: no level deeper than
    /// `output_level` holds a file overlapping it. Dynamic, evaluated per
    /// compaction, and the sole condition for dropping tombstones.
    bool output_is_bottommost = false;

    /// The seed overlaps nothing at the output level, so the file can be
    /// re-pointed instead of rewritten. A move changes no bytes and no tier.
    bool trivial_move = false;
    double score = 0.0;
    /// Whether the winning score came from tombstone density rather than from a size ratio.
    ///
    /// Carried because the two are indistinguishable afterwards — both are just a number above
    /// one — and a configuration claiming to exercise the density trigger has no other way to
    /// show that it did. Without it a suite can hold a case that never arms the trigger and looks
    /// like coverage.
    bool triggered_by_density = false;

    /// Whether `max_compaction_bytes` cut this compaction's input set down from the closure.
    ///
    /// Carried for the same reason as the flag above: afterwards a trimmed set is indistinguishable
    /// from a closure that happened to be small, so a configuration claiming to exercise the trim
    /// has no other way to show that it did.
    bool inputs_trimmed = false;

    /// Every input and overlap, oldest-source-last: the order the merging
    /// iterator needs, since recency is positional (ARCHITECTURE.md "Positional recency").
    std::vector<FileMetadata> all_inputs() const;
    /// min() over inputs — a compaction output inherits the oldest write it
    /// contains (ARCHITECTURE.md "The manifest is snapshots plus edits"), which is also what places it on a tier (ARCHITECTURE.md "A tier is not a level").
    uint64_t min_write_time_ms() const;
    /// The newest write across the inputs, which the output still holds. See `FileMetadata`.
    uint64_t max_write_time_ms() const;
    /// The output's watermark interval: `min` of the inputs' lows, `max` of their highs. The
    /// two halves come from different inputs and that is the point — the output holds all of
    /// their data, so it inherits the weakest lower bound and the strongest upper one. Taking
    /// both from the same input, which is the natural implementation slip, would break the
    /// recovery proof: the `min` is the half it rests on.
    WatermarkInterval watermark() const;
    uint64_t input_bytes() const;
    /// Largest key covered; becomes the level's persisted compaction pointer.
    std::string largest_key() const;
};

/// ARCHITECTURE.md "Compaction" — score is the only trigger. Per level, the maximum of whichever
/// ratios are configured — `file_count / max_files`, `total_bytes / max_bytes`, and the tombstone
/// density of the level's densest file against `tombstone_density_trigger`; highest above 1.0 wins.
/// Density is a score rather than a trigger of its own precisely so this sentence stays true. The last level never triggers — it absorbs everything
/// and has nowhere to spill to.
///
/// There is no age trigger: age governs tier migration (ARCHITECTURE.md "Migration between tiers"), not compaction.
/// One-time rewrites are `compact_level()` (ARCHITECTURE.md "Absence is an answer, not an error"), which terminates.
struct TombstoneDensity {
    /// Fraction of entries that must be tombstones before a file scores; zero disables it.
    double trigger = 0.0;
    /// Entries a file needs before its density counts at all.
    uint64_t min_entries = 1024;
};

std::optional<Compaction> pick_compaction(const Version& version, const ResolvedLevels& config,
                                          size_t max_compaction_bytes,
                                          TombstoneDensity density = {});

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
