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

    /// `Options::age_jitter`, carried here because `placement()` is handed this table and
    /// nothing else.
    double age_jitter = 0.0;

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
/// - `age_jitter` must be in `[0, 1]`.
Result<ResolvedTiers> resolve_tiers(const std::vector<Tier>& tiers, double age_jitter);

/// A file's share of `Options::age_jitter` against a bound of `span_ms`.
///
/// It only ever pulls the bound earlier. For a `Transient` tier `max_age` is a durability
/// exposure bound the engine promises, so spreading a migration past it would weaken a guarantee
/// to smooth a graph.
///
/// File number 0 means "not written yet" and takes no jitter: a flush or compaction picks a
/// tier for its output before the write settles a number, and a renumbering on a name collision
/// would then move the bound underneath it. Those files sit on the exact bound until the first
/// reconcile after they are in the manifest.
uint64_t tier_age_jitter_ms(const ResolvedTiers& tiers, uint64_t file_number,
                            uint64_t min_write_time_ms, uint64_t span_ms);

/// ARCHITECTURE.md "A tier is not a level" — the placement function, verbatim:
///
/// ```
/// tier(file) = the first tier T, hot to cold, satisfying
///                  age(file) <= T.max_age   (or unset)
/// ```
///
/// Age is the only input. A per-file size bound would give size an independent route to a colder
/// tier, placing a large file cold on the day it was written, and placement is well behaved only
/// because it is monotone in age. Cap a tier's footprint with `Tier::max_bytes`, which evicts
/// oldest-first; keep large files off a fast tier with the level's `target_file_bytes`.
///
/// Monotone: `min_write_time_ms` propagates as `min()` over compaction
/// inputs (ARCHITECTURE.md "The manifest is snapshots plus edits"), so it never increases — a file's age only grows and its tier
/// only descends. That is what makes the design stable rather than oscillating,
/// and it is why freshly compacted output made from old data lands directly in a
/// cold tier instead of being written hot and migrated straight back out.
///
/// Still monotone under jitter. The offset is fixed per file, so the bounds a given file
/// faces are fixed numbers and the first one it fits only ever moves colder as its age grows.
/// Across files the bounds no longer line up, which is the entire point.
///
/// The last tier bounds nothing, so this always returns a valid index.
int placement(const ResolvedTiers& tiers, uint64_t file_number, uint64_t min_write_time_ms,
              uint64_t now_ms);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_TIER_HPP
