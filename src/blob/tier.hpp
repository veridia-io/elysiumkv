#ifndef ELYSIUMKV_BLOB_TIER_HPP
#define ELYSIUMKV_BLOB_TIER_HPP

#include "elysiumkv/options.hpp"
#include "elysiumkv/status.hpp"

#include <map>
#include <string>
#include <vector>

namespace elysiumkv {

/// The tier list after validation, plus the distinct stores it names.
/// (`Durability` itself lives in the public options header — `Tier` is part of
/// the configuration surface, so the enum has to be reachable by an embedder.)
struct ResolvedTiers {
    std::vector<Tier> tiers;
    /// Verification and GC are per store (ARCHITECTURE.md "Open and recovery"), so this is the unit both work
    /// in. Several tiers may name the same store.
    std::map<std::string, std::shared_ptr<BlobStore>> stores;

    int last() const { return static_cast<int>(tiers.size()) - 1; }
    bool any_transient() const;
    /// Index of the tier whose store holds this file, or -1 when the store is
    /// not in the configuration at all.
    int tier_of_store(const std::string& store_id) const;
    /// A store may be discarded only if *every* tier naming it is Transient.
    /// Nothing forbids one store from backing two durabilities, so the
    /// ambiguity is resolved toward the non-destructive reading (ARCHITECTURE.md "A tier is not a level").
    bool store_is_discardable(const std::string& store_id) const;
};

/// ARCHITECTURE.md "A tier is not a level" — validation, all `Status::Config`:
///
/// - `max_age` must be non-decreasing across tiers. Otherwise placement is not
///   monotone and files thrash.
/// - The last tier must not set it — it catches everything.
/// - The last tier must be `Durable`.
/// - `Transient` tiers must form a prefix.
/// - A `Transient` tier must set `max_age`, and `stall_age > max_age`.
/// - No tier's store may be a `CacheBlobStore` at its innermost element.
Result<ResolvedTiers> resolve_tiers(const std::vector<Tier>& tiers);

/// ARCHITECTURE.md "A tier is not a level" — the placement function, verbatim:
///
/// ```
/// tier(file) = the first tier T, hot to cold, satisfying
///                  age(file) <= T.max_age   (or unset)
/// ```
///
/// **Age is the only input, and that is the whole of it.** A per-file size bound used to be a second
/// predicate here and was removed: it gave size an independent route to a colder tier, so a large
/// file could be placed cold on the day it was written, and placement is only well behaved because
/// it is monotone. Capping a tier's footprint is `Tier::max_bytes`, which evicts oldest-first;
/// keeping large files off a fast tier is a matter of the level's `target_file_bytes`.
///
/// **Monotone**: `min_write_time_ms` propagates as `min()` over compaction
/// inputs (ARCHITECTURE.md "The manifest is snapshots plus edits"), so it never increases — a file's age only grows and its tier
/// only descends. That is what makes the design stable rather than oscillating,
/// and it is why freshly compacted output made from old data lands directly in a
/// cold tier instead of being written hot and migrated straight back out.
///
/// The last tier bounds nothing, so this always returns a valid index.
int placement(const ResolvedTiers& tiers, uint64_t min_write_time_ms, uint64_t now_ms);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_TIER_HPP
