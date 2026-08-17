#include "compact/migrator.hpp"

#include <map>
#include <vector>

namespace elysiumkv {
namespace {

struct TierContents {
    std::vector<FileMetadata> files;
    uint64_t bytes = 0;
};

std::map<int, TierContents> group_by_tier(const Version& version, const ResolvedTiers& tiers) {
    std::map<int, TierContents> by_tier;
    // Iterated in place: this already copies the files it keeps, and `all_files()` copied every
    // file in the version first — including the L0 ones this drops immediately.
    for (const auto& level : version.levels()) {
        for (const FileMetadata& file : level) {
            // Level 0 is never migrated: a fresh file number would reorder its
            // positional recency (ARCHITECTURE.md "Positional recency"). Those files leave their tier by compaction.
            if (file.level == 0) continue;
            const int tier = tiers.tier_of_store(file.store_id);
            if (tier < 0) continue;  // a store no longer in the configuration
            TierContents& contents = by_tier[tier];
            contents.files.push_back(file);
            contents.bytes += file.file_bytes;
        }
    }
    return by_tier;
}

const FileMetadata* oldest(const std::vector<FileMetadata>& files) {
    const FileMetadata* found = nullptr;
    for (const FileMetadata& file : files) {
        if (found == nullptr || file.min_write_time_ms < found->min_write_time_ms) found = &file;
    }
    return found;
}

}  // namespace

std::optional<Migration> pick_migration(const Version& version, const ResolvedTiers& tiers,
                                        uint64_t now_ms) {
    if (tiers.tiers.size() < 2) return std::nullopt;  // nowhere to move anything

    const std::map<int, TierContents> by_tier = group_by_tier(version, tiers);

    // 1. Off a Transient tier, oldest first. This preempts everything.
    for (const auto& [tier_index, contents] : by_tier) {
        const Tier& tier = tiers.tiers[static_cast<size_t>(tier_index)];
        if (tier.durability != Durability::Transient) continue;

        const FileMetadata* candidate = nullptr;
        for (const FileMetadata& file : contents.files) {
            if (placement(tiers, file.file_number, file.min_write_time_ms, now_ms) <= tier_index) {
                continue;
            }
            if (candidate == nullptr || file.min_write_time_ms < candidate->min_write_time_ms) {
                candidate = &file;
            }
        }
        if (candidate != nullptr) {
            return Migration{*candidate, tier_index,
                             placement(tiers, candidate->file_number, candidate->min_write_time_ms, now_ms),
                             /*leaves_transient=*/true, /*capacity_eviction=*/false};
        }

        // A transient tier over capacity still evicts, and that is also urgent.
        if (tier.max_bytes.has_value() && contents.bytes > *tier.max_bytes) {
            const FileMetadata* victim = oldest(contents.files);
            if (victim != nullptr && tier_index < tiers.last()) {
                return Migration{*victim, tier_index, tier_index + 1, true, true};
            }
        }
    }

    // 2. Capacity eviction on durable tiers, hottest tier first, oldest file
    //    first. Terminates: the last tier is unbounded.
    for (const auto& [tier_index, contents] : by_tier) {
        const Tier& tier = tiers.tiers[static_cast<size_t>(tier_index)];
        if (!tier.max_bytes.has_value() || contents.bytes <= *tier.max_bytes) continue;
        if (tier_index >= tiers.last()) continue;

        const FileMetadata* victim = oldest(contents.files);
        if (victim != nullptr) {
            return Migration{*victim, tier_index, tier_index + 1, false, true};
        }
    }

    // 3. Age-driven migration between durable tiers. Lowest priority: this is an
    //    optimisation, and starving it costs money rather than correctness.
    for (const auto& [tier_index, contents] : by_tier) {
        for (const FileMetadata& file : contents.files) {
            const int target = placement(tiers, file.file_number, file.min_write_time_ms, now_ms);
            if (target > tier_index) {
                return Migration{file, tier_index, target, false, false};
            }
        }
    }

    return std::nullopt;
}

}  // namespace elysiumkv
