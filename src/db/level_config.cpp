#include "db/level_config.hpp"

namespace elysiumkv {

std::map<int, LevelOptions> LevelOptions::geometric(size_t base, int multiplier, int count) {
    std::map<int, LevelOptions> levels;
    LevelOptions l0;
    l0.max_files = 4;
    l0.slowdown_at = 8;
    l0.stop_at = 12;
    levels[0] = l0;

    size_t capacity = base;
    for (int level = 1; level < count; ++level) {
        LevelOptions options;
        // The last level absorbs everything, so it carries no capacity.
        if (level != count - 1) {
            options.max_bytes = capacity;
            capacity *= static_cast<size_t>(multiplier);
        }
        levels[level] = options;
    }
    return levels;
}

Result<ResolvedLevels> resolve_levels(const std::map<int, LevelOptions>& levels) {
    if (levels.empty()) return std::unexpected(Status::Config);

    const int highest = levels.rbegin()->first;
    if (highest < 0 || levels.begin()->first < 0) return std::unexpected(Status::Config);

    ResolvedLevels resolved;
    const LevelOptions* inherited = nullptr;

    for (int level = 0; level <= highest; ++level) {
        auto it = levels.find(level);
        if (it != levels.end()) inherited = &it->second;
        if (inherited == nullptr) return std::unexpected(Status::Config);  // gap before level 0

        const LevelOptions& options = *inherited;
        ResolvedLevel entry;
        entry.level = level;
        entry.compression = options.compression;
        entry.max_bytes = options.max_bytes;
        entry.max_files = options.max_files;
        entry.slowdown_at = options.slowdown_at;
        entry.stop_at = options.stop_at;
        entry.target_file_bytes = options.target_file_bytes;
        resolved.levels.push_back(entry);
    }

    if (resolved.levels[static_cast<size_t>(resolved.last())].max_bytes.has_value()) {
        return std::unexpected(Status::Config);
    }
    return resolved;
}

}  // namespace elysiumkv
