#include "blob/tier.hpp"

#include "util/jitter.hpp"

namespace elysiumkv {

bool ResolvedTiers::any_transient() const {
    for (const Tier& tier : tiers) {
        if (tier.durability == Durability::Transient) return true;
    }
    return false;
}

int ResolvedTiers::tier_of_store(const std::string& store_id) const {
    for (size_t i = 0; i < tiers.size(); ++i) {
        if (tiers[i].store->id() == store_id) return static_cast<int>(i);
    }
    return -1;
}

bool ResolvedTiers::store_is_discardable(const std::string& store_id) const {
    bool named = false;
    for (const Tier& tier : tiers) {
        if (tier.store->id() != store_id) continue;
        named = true;
        if (tier.durability != Durability::Transient) return false;
    }
    return named;
}

uint64_t tier_age_jitter_ms(const ResolvedTiers& tiers, uint64_t file_number,
                            uint64_t min_write_time_ms, uint64_t span_ms) {
    if (file_number == 0) return 0;
    return jitter_offset(file_number, min_write_time_ms,
                         jitter_window_ms(span_ms, tiers.age_jitter));
}

int placement(const ResolvedTiers& tiers, uint64_t file_number, uint64_t min_write_time_ms,
              uint64_t now_ms) {
    const uint64_t age_ms = now_ms > min_write_time_ms ? now_ms - min_write_time_ms : 0;

    for (size_t i = 0; i < tiers.tiers.size(); ++i) {
        const Tier& tier = tiers.tiers[i];
        if (tier.max_age.has_value()) {
            const uint64_t span = static_cast<uint64_t>(tier.max_age->count());
            const uint64_t bound =
                    span - tier_age_jitter_ms(tiers, file_number, min_write_time_ms, span);
            if (age_ms > bound) continue;
        }
        return static_cast<int>(i);
    }
    // The last tier bounds nothing, so this is unreachable for a validated
    // configuration; returning it is the right answer regardless.
    return tiers.last();
}

Result<ResolvedTiers> resolve_tiers(const std::vector<Tier>& tiers, double age_jitter) {
    if (tiers.empty()) return std::unexpected(Status::Config);
    // Written to reject NaN as well.
    if (!(age_jitter >= 0.0) || age_jitter > 1.0) return std::unexpected(Status::Config);

    ResolvedTiers resolved;
    resolved.tiers = tiers;
    resolved.age_jitter = age_jitter;

    const size_t last = tiers.size() - 1;
    for (size_t i = 0; i < tiers.size(); ++i) {
        Tier& tier = resolved.tiers[i];
        if (tier.store == nullptr) return std::unexpected(Status::Config);

        // A cache holds only copies, so making one the only home for a file is
        // the one arrangement discard has nothing to fall back on.
        if (authoritative_store(*tier.store).as_cache() != nullptr) {
            return std::unexpected(Status::Config);
        }
        if (tier.store->id().empty()) return std::unexpected(Status::Config);

        if (tier.durability == Durability::Transient) {
            // Lag = ∞ is not permitted (ARCHITECTURE.md "A tier is not a level").
            if (!tier.max_age.has_value()) return std::unexpected(Status::Config);
            if (!tier.stall_age.has_value()) tier.stall_age = *tier.max_age * 2;
            if (*tier.stall_age <= *tier.max_age) return std::unexpected(Status::Config);
        }

        // Non-decreasing bounds, or placement is not monotone and files thrash.
        if (i > 0) {
            const Tier& previous = resolved.tiers[i - 1];
            if (previous.max_age.has_value() && tier.max_age.has_value() &&
                *tier.max_age < *previous.max_age) {
                return std::unexpected(Status::Config);
            }
            if (!previous.max_age.has_value() && tier.max_age.has_value()) {
                // An unbounded tier followed by a bounded one is decreasing.
                return std::unexpected(Status::Config);
            }
        }

        if (i == last) {
            // The last tier catches everything.
            if (tier.max_age.has_value()) {
                return std::unexpected(Status::Config);
            }
            if (tier.durability != Durability::Durable) return std::unexpected(Status::Config);
        }

        resolved.stores.emplace(tier.store->id(), tier.store);
    }

    // Transient tiers form a prefix: 0..k transient, k+1.. durable.
    bool seen_durable = false;
    for (const Tier& tier : resolved.tiers) {
        if (tier.durability == Durability::Transient && seen_durable) {
            return std::unexpected(Status::Config);
        }
        if (tier.durability == Durability::Durable) seen_durable = true;
    }

    return resolved;
}

}  // namespace elysiumkv
