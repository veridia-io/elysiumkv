#ifndef ELYSIUMKV_DB_LEVEL_CONFIG_HPP
#define ELYSIUMKV_DB_LEVEL_CONFIG_HPP

#include "elysiumkv/options.hpp"
#include "elysiumkv/status.hpp"

#include <map>
#include <vector>

namespace elysiumkv {

/// One level after the map has been resolved: gaps filled from the nearest
/// shallower entry. Nothing downstream sees a sparse map.
///
/// Note what is *not* here: no store, no durability, no age. Where a file lives
/// is a tier decision (ARCHITECTURE.md "A tier is not a level") and independent of its level.
struct ResolvedLevel {
    int level = 0;
    Compression compression = Compression::None;
    std::optional<size_t> max_bytes;
    std::optional<int> max_files;
    std::optional<int> slowdown_at;
    std::optional<int> stop_at;
    size_t target_file_bytes = 16ull << 20;
};

struct ResolvedLevels {
    std::vector<ResolvedLevel> levels;

    /// The deepest *configured* level. Static, and governs configuration rules —
    /// not to be confused with "bottommost for a key range", which is dynamic
    /// and governs tombstone drop (ARCHITECTURE.md "Compaction").
    int last() const { return static_cast<int>(levels.size()) - 1; }
};

/// ARCHITECTURE.md "Compaction" — the only level rule left after tiers took the rest: the last level has
/// no `max_bytes`, because it absorbs everything and a capacity there would have
/// nowhere to spill to.
Result<ResolvedLevels> resolve_levels(const std::map<int, LevelOptions>& levels);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DB_LEVEL_CONFIG_HPP
