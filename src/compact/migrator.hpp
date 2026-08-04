#ifndef ELYSIUMKV_COMPACT_MIGRATOR_HPP
#define ELYSIUMKV_COMPACT_MIGRATOR_HPP

#include "blob/tier.hpp"
#include "version/version.hpp"

#include <optional>

namespace elysiumkv {

/// ARCHITECTURE.md "Migration between tiers" — one file to move, and where to.
struct Migration {
    FileMetadata file;
    int from_tier = 0;
    int to_tier = 0;
    /// True when this moves a file off a `Transient` tier. Those preempt
    /// everything, including compaction: falling behind there costs recovery
    /// window, not just cost efficiency.
    bool leaves_transient = false;
    /// True when the tier is over `max_bytes` rather than the file being too old
    /// — capacity eviction, oldest first.
    bool capacity_eviction = false;
};

/// ARCHITECTURE.md "Migration between tiers" — picks the one migration most worth doing, or nothing.
///
/// Priority, highest first:
///   1. off a `Transient` tier — preempts compaction;
///   2. capacity eviction, oldest file first, from the hottest tier over its
///      `max_bytes`;
///   3. age-driven migration between durable tiers — lowest priority, and
///      starvable without harm, since it is an optimisation.
///
/// **Level 0 is excluded.** ARCHITECTURE.md "Positional recency" resolves recency within L0 positionally, by file
/// number — and ARCHITECTURE.md "The manifest is snapshots plus edits" requires a migration to allocate a *fresh* number, which
/// would be higher than every file that stayed. An L0 file that migrated would
/// therefore read as the newest file at its level whatever it actually holds,
/// and shadow genuinely newer data with a stale value.
///
/// An L0 file that needs to leave its tier is compacted into L1 instead: the
/// rewrite gets a new number, L1 orders by key rather than by number, and the
/// output is placed by age so it lands on the right tier at once. See
/// `DbImpl::run_one_migration`.
///
/// Terminates because placement is monotone: files only move colder, and the
/// last tier is unbounded.
std::optional<Migration> pick_migration(const Version& version, const ResolvedTiers& tiers,
                                        uint64_t now_ms);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_COMPACT_MIGRATOR_HPP
