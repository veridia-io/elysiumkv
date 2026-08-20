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

/// Every file at `level` overlapping [lower, upper], to a fixpoint: each added file widens the
/// range, which may pull in more. At L0 this is usually the whole level.
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

/// Trims an overlapping level's input set to `budget`. Returns whether anything was cut.
///
/// The kept set is downward-closed in age, so every file left at the level outranks the output
/// under positional recency. At least the oldest is always kept, so a budget below one file size
/// still makes progress.
bool trim_to_budget(std::vector<FileMetadata>& inputs, size_t budget) {
    if (budget == 0 || total_bytes(inputs) <= budget) return false;

    // Selection is by age; `inputs` order must survive it. The vector becomes the merge's child
    // list, where ties resolve to the lowest index, so it has to stay in the level's own order.
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
        // The oldest is kept unconditionally, so a budget below one file size still progresses.
        if (!keep.empty() && bytes + found->file_bytes > budget) break;
        bytes += found->file_bytes;
        keep.insert(number);
    }
    std::erase_if(inputs, [&keep](const FileMetadata& file) {
        return keep.count(file.file_number) == 0;
    });
    return true;
}

/// The first file above the persisted compaction pointer, wrapping at the end of the keyspace so
/// the sweep covers it evenly rather than rewriting one region (ARCHITECTURE.md "Compaction").
const FileMetadata* seed_after_pointer(const std::vector<FileMetadata>& files,
                                       const std::string& pointer) {
    for (const FileMetadata& file : files) {
        if (pointer.empty() || file.largest_key > pointer) return &file;
    }
    return files.empty() ? nullptr : &files.front();  // wrap
}

/// The densest file in the level, scored against the trigger. Per file rather than per level: the
/// scan cost is per file, and a level average hides a single dense table.
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

    // Score only. Age governs tier migration, not compaction.
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
            // Ties go to size; density is credited only when it is strictly the deciding term.
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
        // Oldest first: `files_at(0)` is newest-first for the merge's benefit, but draining in
        // that order would starve the oldest files.
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

    // Effective span, not data span: a file carrying only range tombstones has no data span, and
    // an empty bound matches nothing — leaving a compaction whose `inputs.front()` does not exist.
    std::string lower = seed->effective_smallest();
    std::string upper = seed->effective_largest();

    if (is_overlapping_level(chosen, source)) {
        compaction.inputs = transitive_overlap(version, chosen, lower, upper);
        compaction.inputs_trimmed = trim_to_budget(compaction.inputs, max_compaction_bytes);
        // Trimming narrows the span, so the bounds come from what survived.
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

    // A move re-points a file without rewriting it, so a bottommost output must carry no
    // tombstones of either kind: nothing later would force the rewrite that reclaims them. A file
    // can hold range tombstones and no point tombstones, so both counts are checked.
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
