#include "compact/picker.hpp"

#include <algorithm>
#include <utility>

namespace elysiumkv {
namespace {

/// ARCHITECTURE.md "Compaction" — L0 files may overlap, and so may any level configured with `max_files`.
bool is_overlapping_level(int level, const ResolvedLevel& config) {
    return level == 0 || config.max_files.has_value();
}

void widen(const std::vector<FileMetadata>& files, std::string& lower, std::string& upper) {
    for (const FileMetadata& file : files) {
        if (lower.empty() || file.smallest_key < lower) lower = file.smallest_key;
        if (upper.empty() || file.largest_key > upper) upper = file.largest_key;
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
                                          size_t max_compaction_bytes) {
    const int last = config.last();
    if (last < 1) return std::nullopt;  // a single level has nowhere to compact to

    int chosen = -1;
    double chosen_score = 0.0;

    // Score, and nothing else. Age governs tier migration (ARCHITECTURE.md "Migration between tiers"), not compaction.
    for (int level = 0; level < last; ++level) {
        const ResolvedLevel& level_config = config.levels[static_cast<size_t>(level)];
        if (version.file_count(level) == 0) continue;

        const double score = level_score(version, level_config);
        if (score <= 1.0) continue;
        if (chosen < 0 || score > chosen_score) {
            chosen = level;
            chosen_score = score;
        }
    }

    if (chosen < 0) return std::nullopt;

    Compaction compaction;
    compaction.level = chosen;
    compaction.output_level = chosen + 1;
    compaction.score = chosen_score;

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

    std::string lower = seed->smallest_key;
    std::string upper = seed->largest_key;

    if (is_overlapping_level(chosen, source)) {
        compaction.inputs = transitive_overlap(version, chosen, lower, upper);
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
    const bool drops_nothing =
        !compaction.output_is_bottommost || compaction.inputs.front().num_tombstones == 0;

    compaction.trivial_move = compaction.inputs.size() == 1 && compaction.overlaps.empty() &&
                              drops_nothing &&
                              total_bytes(compaction.grandparents) <
                                  max_grandparent_overlap_bytes(target);

    return compaction;
}

}  // namespace elysiumkv
