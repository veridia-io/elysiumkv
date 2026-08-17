#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Fault injection" — configuration validation, **all rejected at open**. Every one of
/// these describes a store that would be silently wrong later, so the check
/// belongs at the door.
class ConfigValidationTest : public ::testing::Test {
protected:
    Options base() { return make_options(store_); }

    static Status open_error(const Options& options) {
        auto opened = DB::open_with_result(options);
        return opened.has_value() ? Status::Ok : opened.error();
    }

    TestStore store_{3};
};

TEST_F(ConfigValidationTest, TheBaseConfigurationIsValid) {
    EXPECT_EQ(open_error(base()), Status::Ok);
}

// --- tier rules (ARCHITECTURE.md "A tier is not a level") ---------------------------------------------------------

TEST_F(ConfigValidationTest, NoTiersAtAllIsRejected) {
    Options options = base();
    options.tiers.clear();
    EXPECT_EQ(open_error(options), Status::Config);
}

// The last tier catches everything, so it can bound nothing that would send a file past it. Age is
// now the only such bound — the per-file size bound that used to be checked here is gone.
TEST_F(ConfigValidationTest, TheLastTierMayNotBoundAge) {
    Options aged = base();
    aged.tiers.back().max_age = Duration(60'000);
    EXPECT_EQ(open_error(aged), Status::Config);

    // A capacity there is fine — it just never evicts, since there is nowhere
    // below to evict to.
    Options capped = base();
    capped.tiers.back().max_bytes = 1u << 30;
    EXPECT_EQ(open_error(capped), Status::Ok);
}

// ARCHITECTURE.md "A tier is not a level" — the last tier is the floor a discard recovers against.
TEST_F(ConfigValidationTest, ATransientLastTierIsRejected) {
    Options options = base();
    options.tiers.back().durability = Durability::Transient;
    options.tiers.back().max_age = Duration(60'000);
    EXPECT_EQ(open_error(options), Status::Config);
}

// ARCHITECTURE.md "A tier is not a level" — placement must be monotone, or files thrash between stores paying a
// copy each way.
TEST_F(ConfigValidationTest, BoundsMustBeNonDecreasingAcrossTiers) {
    Options decreasing = base();
    decreasing.tiers = {
        Tier{.store = store_.store(0), .max_age = Duration(60'000)},
        Tier{.store = store_.store(1), .max_age = Duration(30'000)},  // hotter than tier 0
        Tier{.store = store_.store(2)},
    };
    EXPECT_EQ(open_error(decreasing), Status::Config);

    Options increasing = base();
    increasing.tiers = {
        Tier{.store = store_.store(0), .max_age = Duration(30'000)},
        Tier{.store = store_.store(1), .max_age = Duration(60'000)},
        Tier{.store = store_.store(2)},
    };
    EXPECT_EQ(open_error(increasing), Status::Ok);
}

// An unbounded tier followed by a bounded one is decreasing: nothing could ever
// reach the bounded tier.
TEST_F(ConfigValidationTest, AnUnboundedTierMayNotBeFollowedByABoundedOne) {
    Options options = base();
    options.tiers = {
        Tier{.store = store_.store(0)},  // catches everything
        Tier{.store = store_.store(1), .max_age = Duration(60'000)},
        Tier{.store = store_.store(2)},
    };
    EXPECT_EQ(open_error(options), Status::Config);
}

// ARCHITECTURE.md "A tier is not a level" — Transient tiers form a prefix.
TEST_F(ConfigValidationTest, ADurableTierAheadOfATransientOneIsRejected) {
    Options options = base();
    options.tiers = {
        Tier{.store = store_.store(0), .durability = Durability::Durable,
             .max_age = Duration(30'000)},
        Tier{.store = store_.store(1), .durability = Durability::Transient,
             .max_age = Duration(60'000)},
        Tier{.store = store_.store(2)},
    };
    EXPECT_EQ(open_error(options), Status::Config);
}

// ARCHITECTURE.md "A tier is not a level" — lag = ∞ is not permitted.
TEST_F(ConfigValidationTest, ATransientTierWithoutMaxAgeIsRejected) {
    Options options = base();
    options.tiers = {
        Tier{.store = store_.store(0), .durability = Durability::Transient},
        Tier{.store = store_.store(1)},
    };
    EXPECT_EQ(open_error(options), Status::Config);
}

TEST_F(ConfigValidationTest, StallAgeMustExceedMaxAge) {
    Options equal = base();
    equal.tiers = {
        Tier{.store = store_.store(0),
             .durability = Durability::Transient,
             .max_age = Duration(60'000),
             .stall_age = Duration(60'000)},
        Tier{.store = store_.store(1)},
    };
    EXPECT_EQ(open_error(equal), Status::Config);

    Options greater = equal;
    greater.tiers[0].stall_age = Duration(120'000);
    EXPECT_EQ(open_error(greater), Status::Ok);

    // Unset defaults to twice max_age.
    Options defaulted = equal;
    defaulted.tiers[0].stall_age.reset();
    EXPECT_EQ(open_error(defaulted), Status::Ok);
}

// ARCHITECTURE.md "A tier is not a level" — a cache holds only copies, so making one the only home for a file is the
// one arrangement discard has nothing to fall back on.
TEST_F(ConfigValidationTest, ACacheAsATiersInnermostStoreIsRejected) {
    class TransparentCache final : public CacheBlobStore {
    public:
        explicit TransparentCache(std::shared_ptr<BlobStore> delegate)
            : delegate_(std::move(delegate)) {}
        BlobStore& delegate() override { return *delegate_; }
        size_t max_cache_bytes() const override { return 0; }
        bool cache_on_write() const override { return false; }
        uint64_t hits() const override { return 0; }
        uint64_t misses() const override { return 0; }
        std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
            return delegate_->get(name, offset, len);
        }
        std::future<Status> put(std::string_view name, Slice bytes) override {
            return delegate_->put(name, bytes);
        }
        std::future<Status> remove(std::string_view name) override {
            return delegate_->remove(name);
        }
        std::future<ListResult> list(std::string_view prefix) override {
            return delegate_->list(prefix);
        }

    private:
        std::shared_ptr<BlobStore> delegate_;
    };

    // A cache *chain* over an authoritative store is fine: the innermost element
    // is what has to be authoritative.
    Options wrapped = base();
    wrapped.tiers.back().store = std::make_shared<TransparentCache>(store_.store(0));
    EXPECT_EQ(open_error(wrapped), Status::Ok);

    auto nested = std::make_shared<TransparentCache>(
        std::make_shared<TransparentCache>(store_.store(0)));
    EXPECT_EQ(&authoritative_store(*nested), store_.store(0).get());
}

// --- level rules (ARCHITECTURE.md "Compaction") ----------------------------------------------------------

// ARCHITECTURE.md "Compaction" — the last level absorbs everything; a capacity there would have nowhere to
// spill to. This is the only level rule left after tiers took the rest.
TEST_F(ConfigValidationTest, MaxBytesOnTheLastLevelIsRejected) {
    Options options = base();
    options.levels[2].max_bytes = 1u << 30;
    EXPECT_EQ(open_error(options), Status::Config);
}

TEST_F(ConfigValidationTest, AnEmptyLevelMapIsRejected) {
    Options options = base();
    options.levels.clear();
    EXPECT_EQ(open_error(options), Status::Config);
}

TEST_F(ConfigValidationTest, AMissingCatalogIsRejected) {
    Options options = base();
    options.manifest_catalog.reset();
    EXPECT_EQ(open_error(options), Status::Config);
}

// ARCHITECTURE.md "A tier is not a level" — gaps inherit the nearest shallower entry.
TEST_F(ConfigValidationTest, GapsInTheLevelMapInheritFromAbove) {
    Options options = base();
    LevelOptions l0;
    l0.max_files = 4;
    l0.compression = Compression::Zstd;
    LevelOptions l3;
    options.levels = {{0, l0}, {3, l3}};  // 1 and 2 inherit level 0

    auto opened = DB::open_with_result(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    EXPECT_EQ(opened->db->stats().levels.size(), 4u);
}

// --- the guarded open (ARCHITECTURE.md "A tier is not a level") ----------------------------------------------------

TEST_F(ConfigValidationTest, GuardedOpenRefusesATransientConfiguration) {
    Options options = make_transient_options(store_, Duration(60'000), Duration(120'000));

    auto guarded = DB::open(options);
    ASSERT_FALSE(guarded.has_value());
    EXPECT_EQ(guarded.error(), Status::Config);

    auto reported = DB::open_with_result(options);
    ASSERT_TRUE(reported.has_value()) << status_name(reported.error());
    EXPECT_TRUE(reported->discarded_stores.empty());
    EXPECT_EQ(reported->discarded_files, 0u);
    EXPECT_FALSE(reported->requires_recovery);
}

TEST_F(ConfigValidationTest, GuardedOpenAcceptsAnAllDurableConfiguration) {
    EXPECT_TRUE(DB::open(base()).has_value());
    EXPECT_TRUE(DB::open(make_tiered_options(store_, Duration(60'000))).has_value())
        << "several tiers are fine; it is transience the guard is about";
}

TEST_F(ConfigValidationTest, StatsReportBothAxes) {
    auto opened = DB::open(make_tiered_options(store_, Duration(60'000)));
    ASSERT_TRUE(opened.has_value());
    const Stats stats = (*opened)->stats();
    EXPECT_EQ(stats.levels.size(), 3u);
    EXPECT_EQ(stats.tiers.size(), 2u);
}

}  // namespace
}  // namespace elysiumkv::test
