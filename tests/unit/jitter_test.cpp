#include "blob/tier.hpp"
#include "support/test_db.hpp"
#include "util/jitter.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <set>

namespace elysiumkv {
namespace {

using test::TestStore;

constexpr uint64_t kMaxAge = 3'600'000;   // one hour, the shape production runs

/// Two tiers: a transient one bounding age, then the durable catch-all.
ResolvedTiers two_tiers(TestStore& store, double jitter) {
    std::vector<Tier> tiers;
    tiers.push_back(Tier{.store = store.store(0),
                         .durability = Durability::Transient,
                         .max_age = Duration(kMaxAge),
                         .max_bytes = std::nullopt,
                         .stall_age = Duration(2 * kMaxAge)});
    tiers.push_back(Tier{.store = store.store(1), .durability = Durability::Durable});
    auto resolved = resolve_tiers(tiers, jitter);
    EXPECT_TRUE(resolved.has_value());
    return *resolved;
}

/// The first age at which this file leaves tier 0, found by bisection rather than by
/// recomputing the offset — so the test cannot pass by repeating the implementation.
uint64_t crossing_age(const ResolvedTiers& tiers, uint64_t file_number, uint64_t born) {
    uint64_t low = 0, high = 2 * kMaxAge;
    while (low < high) {
        const uint64_t mid = low + (high - low) / 2;
        if (placement(tiers, file_number, born, born + mid) == 0) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

// --- the derivation ---------------------------------------------------------

TEST(JitterTest, AnOffsetNeverReachesTheWindow) {
    for (uint64_t n = 1; n <= 500; ++n) {
        EXPECT_LT(jitter_offset(n, 17 * n, 1000), 1000u);
    }
}

TEST(JitterTest, AZeroWindowIsNoOffset) {
    EXPECT_EQ(jitter_offset(7, 99, 0), 0u);
    EXPECT_EQ(jitter_window_ms(kMaxAge, 0.0), 0u);
    EXPECT_EQ(jitter_window_ms(0, 0.5), 0u);
}

/// NaN passes every comparison it is asked, so it has to be rejected by shape rather than by
/// range — otherwise it reaches the engine as a window of unpredictable width.
TEST(JitterTest, ANaNFractionIsNoWindow) {
    EXPECT_EQ(jitter_window_ms(kMaxAge, std::nan("")), 0u);
}

/// The property the whole design rests on. `placement()` and `next_time_transition()` each
/// derive the offset independently; if it were rolled they would disagree, and a reopen would
/// re-cluster the files it had just spread.
TEST(JitterTest, TheSameFileAlwaysGetsTheSameOffset) {
    for (uint64_t n = 1; n <= 50; ++n) {
        const uint64_t first = jitter_offset(n, 1'000 + n, 900'000);
        for (int again = 0; again < 5; ++again) {
            EXPECT_EQ(jitter_offset(n, 1'000 + n, 900'000), first) << "file " << n;
        }
    }
}

/// File numbers are handed out consecutively, so a burst of them arrives as a run. A plain
/// modulo would map that run onto a narrow band of offsets — 24 files landing in a 24 ms
/// band of an hour-wide window — which is the burst this exists to break up.
TEST(JitterTest, ConsecutiveFileNumbersSpreadAcrossTheWindow) {
    constexpr uint64_t kWindow = 900'000;
    constexpr int kBuckets = 10;
    std::set<int> occupied;
    for (uint64_t n = 1; n <= 200; ++n) {
        occupied.insert(static_cast<int>(jitter_offset(n, 5'000, kWindow) * kBuckets / kWindow));
    }
    EXPECT_EQ(occupied.size(), static_cast<size_t>(kBuckets))
        << "200 consecutive numbers left a tenth of the window empty";
}

/// Why the write time is a seed and not just the number. A partitioned embedder gives every
/// partition its own file-number counter, so partition 0's file 7 and partition 9's file 7 are
/// both file 7 — and would migrate together, which is the case this was built for.
TEST(JitterTest, StoresThatShareAFileNumberDoNotShareAnOffset) {
    std::set<uint64_t> offsets;
    for (uint64_t partition = 0; partition < 24; ++partition) {
        // Restore is sequential per partition, so their files are stamped seconds apart.
        offsets.insert(jitter_offset(7, 1'000'000 + partition * 1'000, 900'000));
    }
    EXPECT_EQ(offsets.size(), 24u) << "two partitions were given the same offset";
}

/// The inverse: one compaction's outputs all inherit a single `min_write_time_ms`, so the time
/// alone would give them one offset. The number separates them.
TEST(JitterTest, OneCompactionsOutputsDoNotShareAnOffset) {
    std::set<uint64_t> offsets;
    for (uint64_t n = 40; n < 50; ++n) offsets.insert(jitter_offset(n, 1'000'000, 900'000));
    EXPECT_EQ(offsets.size(), 10u);
}

// --- placement --------------------------------------------------------------

TEST(JitterTest, WithoutJitterAFileCrossesExactlyAtMaxAge) {
    TestStore store(2);
    const ResolvedTiers tiers = two_tiers(store, 0.0);
    for (uint64_t n = 1; n <= 20; ++n) {
        EXPECT_EQ(crossing_age(tiers, n, 1'000'000 + n), kMaxAge + 1) << "file " << n;
    }
}

/// Earlier only. A transient tier's `max_age` is an exposure bound the engine promises, so
/// spreading a migration *past* it would weaken a guarantee to smooth a graph.
TEST(JitterTest, JitterOnlyEverMovesTheCrossingEarlier) {
    TestStore store(2);
    const ResolvedTiers tiers = two_tiers(store, 0.25);
    const uint64_t floor_age = kMaxAge - kMaxAge / 4;

    std::set<uint64_t> crossings;
    for (uint64_t n = 1; n <= 100; ++n) {
        const uint64_t crossing = crossing_age(tiers, n, 1'000'000 + n * 37);
        EXPECT_LE(crossing, kMaxAge + 1) << "file " << n << " crossed late";
        EXPECT_GT(crossing, floor_age) << "file " << n << " crossed before the window opens";
        crossings.insert(crossing);
    }
    EXPECT_GT(crossings.size(), 90u) << "100 files produced fewer than 91 distinct crossings";
}

/// A file's tier only ever descends. The offset is fixed per file, so the bounds it faces are
/// fixed numbers — jitter moves them apart across files without making any one file oscillate.
TEST(JitterTest, PlacementStaysMonotoneInAge) {
    TestStore store(2);
    const ResolvedTiers tiers = two_tiers(store, 0.9);
    constexpr uint64_t kBorn = 1'000'000;
    for (uint64_t n = 1; n <= 20; ++n) {
        int previous = 0;
        for (uint64_t age = 0; age <= 2 * kMaxAge; age += 4'999) {
            const int at = placement(tiers, n, kBorn, kBorn + age);
            EXPECT_GE(at, previous) << "file " << n << " moved back to a hotter tier at " << age;
            previous = at;
        }
    }
}

/// A flush or compaction picks a tier before the write settles a number, and a renumbering on a
/// name collision would then move a jittered bound underneath it.
TEST(JitterTest, AnUnnumberedFileTakesNoJitter) {
    TestStore store(2);
    const ResolvedTiers tiers = two_tiers(store, 0.5);
    EXPECT_EQ(crossing_age(tiers, /*file_number=*/0, 1'000'000), kMaxAge + 1);
}

/// `stall_age` is an alarm rather than a schedule, and blurring an alarm only makes it harder
/// to read. Pinned because it sits one field away from the bound that *is* jittered.
TEST(JitterTest, TheStallAgeIsNotJittered) {
    TestStore store(2);
    const ResolvedTiers tiers = two_tiers(store, 0.5);
    EXPECT_EQ(tiers.tiers[0].stall_age->count(), static_cast<Duration::rep>(2 * kMaxAge));
}

// --- configuration ----------------------------------------------------------

TEST(JitterTest, AFractionOutsideTheUnitRangeIsRejected) {
    TestStore store(2);
    std::vector<Tier> tiers;
    tiers.push_back(Tier{.store = store.store(0),
                         .durability = Durability::Transient,
                         .max_age = Duration(kMaxAge)});
    tiers.push_back(Tier{.store = store.store(1), .durability = Durability::Durable});

    EXPECT_TRUE(resolve_tiers(tiers, 0.0).has_value());
    EXPECT_TRUE(resolve_tiers(tiers, 1.0).has_value());
    EXPECT_EQ(resolve_tiers(tiers, -0.1).error(), Status::Config);
    EXPECT_EQ(resolve_tiers(tiers, 1.1).error(), Status::Config);
    EXPECT_EQ(resolve_tiers(tiers, std::nan("")).error(), Status::Config);
}

/// The full fraction leaves the bound above zero rather than at it, so a file is never born
/// already past its own deadline.
TEST(JitterTest, TheFullFractionStillLeavesAPositiveBound) {
    TestStore store(2);
    const ResolvedTiers tiers = two_tiers(store, 1.0);
    for (uint64_t n = 1; n <= 100; ++n) {
        EXPECT_EQ(placement(tiers, n, 1'000'000, 1'000'000), 0)
            << "file " << n << " was placed cold on the moment it was written";
    }
}

}  // namespace
}  // namespace elysiumkv
