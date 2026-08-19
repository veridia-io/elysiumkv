#include "compact/picker.hpp"

#include <algorithm>
#include <set>
#include <utility>

namespace elysiumkv {
namespace {

/// ARCHITECTURE.md "Compaction" — L0 files may overlap, and so may any level configured with `max_files`.
bool is_overlapping_level(int level, const ResolvedLevel& config) {
    return level == 0 || config.max_files.has_value();
}

void widen(const std::vector<FileMetadata>& files, std::string& lower, std::string& upper) {
    for (const FileMetadata& file : files) {
        // Effective, not data: a file's range tombstones widen what it must be merged against.
        const std::string file_low = file.effective_smallest();
        const std::string file_high = file.effective_largest();
        if (lower.empty() || file_low < lower) lower = file_low;
        if (upper.empty() || file_high > upper) upper = file_high;
    }
}

uint64_t total_bytes(const std::vector<FileMetadata>& files) {
    uint64_t bytes = 0;
    for (const FileMetadata& file : files) bytes += file.file_bytes;
    return bytes;
}

/// Every file at `level` whose range overlaps [lower, upper], transitively: an
/// added file widens the range, which may pull in more. In practice at L0 this
/// usually ends up being all of them.
std::vector<FileMetadata> transitive_overlap(const Version& version, int level,
                                             std::string lower, std::string upper) {
    std::vector<FileMetadata> selected;
    while (true) {
        std::vector<FileMetadata> found =
            version.overlapping_inclusive(level, Slice::from(lower), Slice::from(upper));
        if (found.size() == selected.size()) return found;

        selected = std::move(found);
        std::string widened_lower = lower;
        std::string widened_upper = upper;
        widen(selected, widened_lower, widened_upper);
        if (widened_lower == lower && widened_upper == upper) return selected;
        lower = std::move(widened_lower);
        upper = std::move(widened_upper);
    }
}

/// Trims an overlapping level's input set to `budget`, **oldest first**. True when it cut
/// anything, which is what `Stats::compactions_trimmed` counts.
///
/// `max_compaction_bytes` was consulted only when deciding whether to *expand* a compaction back
/// into its source level, so the primary set was unbounded — and at L0 the transitive closure is
/// "usually all of them". ARCHITECTURE.md turns that budget into the third term of the exposure
/// window, so leaving it unenforced made the arithmetic there a statement about a limit that did
/// not exist.
///
/// **The direction is forced by positional recency.** Recency is `(level, file_number)`, so a file
/// left behind at an overlapping level is newer than the output if and only if its number is
/// larger. Keeping the oldest and dropping the newest leaves every remaining file *above* the
/// output, which is the order reads already expect; keeping the newest would strand an older file
/// at L0 shadowing an output built from newer data, and it would return stale values.
///
/// So the set stays downward-closed in age, and at least the oldest file always goes — a budget
/// smaller than one file must still make progress rather than stall the level for ever.
bool trim_to_budget(std::vector<FileMetadata>& inputs, size_t budget) {
    if (budget == 0 || total_bytes(inputs) <= budget) return false;

    // **Which files to keep is decided by age; the order they are left in is not touched.** The
    // vector is handed to the merge as its child list, and `write_compaction_outputs` resolves a
    // tie by lowest child index — which is the recency rule only while the children arrive in the
    // level's own order, newest first at L0. Sorting this vector by file number to choose from it
    // therefore reversed exactly the thing it was protecting: the merge took the *oldest* value for
    // every duplicated key, and a key not held by a newer file left behind at L0 read back as the
    // value it had been overwritten from.
    std::vector<uint64_t> by_age;
    by_age.reserve(inputs.size());
    for (const FileMetadata& file : inputs) by_age.push_back(file.file_number);
    std::sort(by_age.begin(), by_age.end());

    std::set<uint64_t> keep;
    uint64_t bytes = 0;
    for (const uint64_t number : by_age) {
        const auto found = std::find_if(inputs.begin(), inputs.end(),
                                        [number](const FileMetadata& file) {
                                            return file.file_number == number;
                                        });
        // At least the oldest always goes: a budget smaller than one file must still make progress
        // rather than stall the level for ever.
        if (!keep.empty() && bytes + found->file_bytes > budget) break;
        bytes += found->file_bytes;
        keep.insert(number);
    }
    std::erase_if(inputs, [&keep](const FileMetadata& file) {
        return keep.count(file.file_number) == 0;
    });
    return true;
}

/// ARCHITECTURE.md "Compaction" — the seed for a non-overlapping level comes from the persisted compaction
/// pointer — the largest key of the previous compaction there — wrapping at the
/// end of the keyspace, so the sweep covers the keyspace evenly instead of
/// rewriting one hot region.
const FileMetadata* seed_after_pointer(const std::vector<FileMetadata>& files,
                                       const std::string& pointer) {
    for (const FileMetadata& file : files) {
        if (pointer.empty() || file.largest_key > pointer) return &file;
    }
    return files.empty() ? nullptr : &files.front();  // wrap
}

/// The densest file in the level, scored against the trigger.
///
/// Per file rather than per level, because the cost is per file: one table that is nine-tenths
/// tombstones makes every scan crossing it expensive, and averaging it against a level of clean
/// tables hides exactly the case worth acting on. TidesDB reaches the same conclusion — one dense
/// table is enough to fire.
double tombstone_density_score(const Version& version, const ResolvedLevel& config,
                               const TombstoneDensity& density) {
    if (density.trigger <= 0.0) return 0.0;

    double worst = 0.0;
    for (const FileMetadata& file : version.files_at(config.level)) {
        if (file.num_entries < density.min_entries) continue;
        if (file.num_tombstones == 0) continue;
        const double ratio =
            static_cast<double>(file.num_tombstones) / static_cast<double>(file.num_entries);
        worst = std::max(worst, ratio / density.trigger);
    }
    return worst;
}

double level_score(const Version& version, const ResolvedLevel& config) {
    double score = 0.0;
    if (config.max_files.has_value() && *config.max_files > 0) {
        score = std::max(score, static_cast<double>(version.file_count(config.level)) /
                                    static_cast<double>(*config.max_files));
    }
    if (config.max_bytes.has_value() && *config.max_bytes > 0) {
        score = std::max(score, static_cast<double>(version.total_bytes(config.level)) /
                                    static_cast<double>(*config.max_bytes));
    }
    return score;
}

}  // namespace

size_t max_grandparent_overlap_bytes(const ResolvedLevel& output_level) {
    return output_level.target_file_bytes * 10;
}

std::vector<FileMetadata> Compaction::all_inputs() const {
    std::vector<FileMetadata> all = inputs;
    all.insert(all.end(), overlaps.begin(), overlaps.end());
    return all;
}

uint64_t Compaction::min_write_time_ms() const {
    uint64_t oldest = 0;
    for (const FileMetadata& file : all_inputs()) {
        if (oldest == 0 || file.min_write_time_ms < oldest) oldest = file.min_write_time_ms;
    }
    return oldest;
}

uint64_t Compaction::max_write_time_ms() const {
    uint64_t newest = 0;
    for (const FileMetadata& file : all_inputs()) {
        if (file.max_write_time_ms > newest) newest = file.max_write_time_ms;
    }
    return newest;
}

WatermarkInterval Compaction::watermark() const {
    const std::vector<FileMetadata> files = all_inputs();
    WatermarkInterval merged;
    bool first = true;
    for (const FileMetadata& file : files) {
        if (first) {
            merged = file.watermark;
            first = false;
        } else {
            merged.merge(file.watermark);
        }
    }
    return merged;
}

uint64_t Compaction::input_bytes() const { return total_bytes(all_inputs()); }

std::string Compaction::largest_key() const {
    std::string largest;
    for (const FileMetadata& file : all_inputs()) {
        if (largest.empty() || file.largest_key > largest) largest = file.largest_key;
    }
    return largest;
}

bool is_bottommost_for_range(const Version& version, int output_level, int last_level,
                             Slice lower, Slice upper) {
    for (int level = output_level + 1; level <= last_level; ++level) {
        if (!version.overlapping_inclusive(level, lower, upper).empty()) return false;
    }
    return true;
}

std::optional<Compaction> pick_compaction(const Version& version, const ResolvedLevels& config,
                                          size_t max_compaction_bytes,
                                          TombstoneDensity density) {
    const int last = config.last();
    if (last < 1) return std::nullopt;  // a single level has nowhere to compact to

    int chosen = -1;
    double chosen_score = 0.0;
    bool chosen_by_density = false;

    // Score, and nothing else. Age governs tier migration (ARCHITECTURE.md "Migration between tiers"), not compaction.
    for (int level = 0; level < last; ++level) {
        const ResolvedLevel& level_config = config.levels[static_cast<size_t>(level)];
        if (version.file_count(level) == 0) continue;

        const double by_size = level_score(version, level_config);
        const double by_density = tombstone_density_score(version, level_config, density);
        const double score = std::max(by_size, by_density);
        if (score <= 1.0) continue;
        if (chosen < 0 || score > chosen_score) {
            chosen = level;
            chosen_score = score;
            // Ties go to size, which is the trigger that was there first: density is only credited
            // when it is strictly the reason this level was picked.
            chosen_by_density = by_density > by_size;
        }
    }

    if (chosen < 0) return std::nullopt;

    Compaction compaction;
    compaction.level = chosen;
    compaction.output_level = chosen + 1;
    compaction.score = chosen_score;
    compaction.triggered_by_density = chosen_by_density;

    const ResolvedLevel& source = config.levels[static_cast<size_t>(chosen)];
    const ResolvedLevel& target = config.levels[static_cast<size_t>(compaction.output_level)];
    const std::vector<FileMetadata>& files = version.files_at(chosen);

    const FileMetadata* seed = nullptr;
    if (is_overlapping_level(chosen, source)) {
        // The oldest file, not the newest: L0 is stored newest-first for the
        // merging iterator's benefit, but draining it oldest-first is what keeps
        // the level turning over evenly.
        seed = &files.front();
        for (const FileMetadata& candidate : files) {
            if (candidate.file_number < seed->file_number) seed = &candidate;
        }
    } else {
        auto pointer = version.compaction_pointers().find(chosen);
        seed = seed_after_pointer(files, pointer == version.compaction_pointers().end()
                                             ? std::string()
                                             : pointer->second);
    }
    if (seed == nullptr) return std::nullopt;

    // The seed's *effective* span, for the same reason `widen` uses it below. A file carrying only
    // range tombstones has no data span at all, and starting from an empty one leaves the overlap
    // search matching nothing — not even the seed itself, whose effective span sits above the empty
    // upper bound. The compaction then has no inputs, and the code below reads `inputs.front()`.
    std::string lower = seed->effective_smallest();
    std::string upper = seed->effective_largest();

    if (is_overlapping_level(chosen, source)) {
        compaction.inputs = transitive_overlap(version, chosen, lower, upper);
        compaction.inputs_trimmed = trim_to_budget(compaction.inputs, max_compaction_bytes);
        // Trimming narrows the span, so the bounds are rebuilt from what survived rather than
        // carried over from the closure.
        lower = compaction.inputs.front().effective_smallest();
        upper = compaction.inputs.front().effective_largest();
    } else {
        compaction.inputs = {*seed};
    }
    widen(compaction.inputs, lower, upper);

    compaction.overlaps =
        version.overlapping_inclusive(compaction.output_level, Slice::from(lower), Slice::from(upper));

    // Optionally expand back into the source level with files falling inside the
    // resulting output range, while total input bytes stay under the budget.
    if (!compaction.overlaps.empty()) {
        std::string expanded_lower = lower;
        std::string expanded_upper = upper;
        widen(compaction.overlaps, expanded_lower, expanded_upper);

        std::vector<FileMetadata> expanded =
            version.overlapping_inclusive(chosen, Slice::from(expanded_lower), Slice::from(expanded_upper));
        if (expanded.size() > compaction.inputs.size() &&
            total_bytes(expanded) + total_bytes(compaction.overlaps) <= max_compaction_bytes) {
            std::string check_lower = expanded_lower;
            std::string check_upper = expanded_upper;
            widen(expanded, check_lower, check_upper);
            auto reoverlap = version.overlapping_inclusive(
                compaction.output_level, Slice::from(check_lower), Slice::from(check_upper));
            if (reoverlap.size() == compaction.overlaps.size()) {
                compaction.inputs = std::move(expanded);
                lower = std::move(check_lower);
                upper = std::move(check_upper);
            }
        }
    }

    compaction.grandparents = compaction.output_level + 1 <= last
                                  ? version.overlapping_inclusive(compaction.output_level + 1,
                                                                   Slice::from(lower),
                                                                   Slice::from(upper))
                                  : std::vector<FileMetadata>{};

    compaction.output_is_bottommost = is_bottommost_for_range(
        version, compaction.output_level, last, Slice::from(lower), Slice::from(upper));

    // Trivial move: nothing to merge with, and the next compaction of this file
    // will not be enormous. Tier is independent of level (ARCHITECTURE.md "A tier is not a level"), so a move never
    // changes a file's store and there is no store boundary to consider.
    //
    // But a move does not rewrite, so moving a tombstone-bearing file to where it
    // is bottommost would carry those tombstones past the only point that
    // reclaims them, and nothing subsequently forces a rewrite. Fall back to a
    // normal rewrite in that case.
    // Range tombstones count here exactly as point tombstones do: a move does not rewrite, so
    // carrying either past the level that reclaims them leaves them with nothing to force a rewrite
    // later. A file can carry range tombstones and no point tombstones at all — a flush whose
    // memtable saw only a `delete_range` is precisely that — so testing one and not the other lets
    // the commonest shape through.
    const bool drops_nothing = !compaction.output_is_bottommost ||
                               (compaction.inputs.front().num_tombstones == 0 &&
                                compaction.inputs.front().num_range_tombstones == 0);

    compaction.trivial_move = compaction.inputs.size() == 1 && compaction.overlaps.empty() &&
                              drops_nothing &&
                              total_bytes(compaction.grandparents) <
                                  max_grandparent_overlap_bytes(target);

    return compaction;
}

}  // namespace elysiumkv
