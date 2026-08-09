#include "support/alloc_counter.hpp"
#include "support/sanitizers.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Benchmarks" — the differential oracle validates answers, not cost. Nothing in
/// ARCHITECTURE.md "The differential oracle" distinguishes a hot path free of allocation from one that constructs a
/// `std::string` per comparison: both are correct, both pass a million seeded
/// operations, and one is several times slower. These are hard assertions.
///
/// Skipped under sanitizers, whose runtimes allocate on their own account.
class AllocationTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (running_under_sanitizer()) {
            GTEST_SKIP() << "sanitizer runtimes allocate on their own account";
        }
        Options options = make_options(store_, Compression::None, 4u << 20);
        options.background = BackgroundMode::Inline;
        auto opened = DB::open(options);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(*opened);

        for (int i = 0; i < kKeys; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(std::string(64, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db_->flush(), Status::Ok);

        // Warm the block cache and the reader table: the assertions are about
        // the hot path, and a cold read legitimately allocates a block.
        for (int i = 0; i < kKeys; ++i) {
            auto found = db_->get(Slice::from(key_at(i)));
            ASSERT_TRUE(found.has_value());
        }
    }

    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "cluster:%04d:key:%08d", i / 100, i);
        return buf;
    }

    static constexpr int kKeys = 2000;
    TestStore store_;
    std::unique_ptr<DB> db_;
};

TEST_F(AllocationTest, PointLookupOnACacheHitAllocatesNothing) {
    // The key is built outside the measured region: what is being asserted is
    // the engine's behaviour, not the caller's.
    const std::string key = key_at(1234);

    AllocationScope scope;
    for (int i = 0; i < 100; ++i) {
        auto found = db_->get(Slice::from(key));
        ASSERT_TRUE(found.has_value());
        ASSERT_FALSE(found->value().empty());
    }
    EXPECT_EQ(scope.count(), 0u) << "a cache-hit point lookup must not allocate";
}

TEST_F(AllocationTest, IteratorNextAllocatesNothing) {
    auto it = db_->iterator();
    ASSERT_TRUE(it->next());  // construction and first positioning may allocate

    AllocationScope scope;
    int seen = 0;
    while (it->next()) {
        ASSERT_FALSE(it->key().empty());
        ++seen;
    }
    EXPECT_GT(seen, kKeys / 2);
    EXPECT_EQ(scope.count(), 0u) << "advancing an iterator must not allocate per entry";
}

TEST_F(AllocationTest, PrefixScanAllocatesOnlyWhileSettingUp) {
    const std::string prefix = "cluster:0012:";

    auto it = db_->prefix_iterator(Slice::from(prefix));
    ASSERT_TRUE(it->next());

    AllocationScope scope;
    int seen = 1;
    while (it->next()) ++seen;
    EXPECT_EQ(seen, 100);
    EXPECT_EQ(scope.count(), 0u);
}

/// Descending has to be allocation-free per entry too, and it is the direction with somewhere to
/// hide one: a backward step re-reads from the nearest restart point and rebuilds the key from a
/// shared prefix, and the memtable descends the skiplist afresh. Any of those reaching for the heap
/// would cost a scan far more than the step it saved.
TEST_F(AllocationTest, ReverseIteratorNextAllocatesNothing) {
    auto it = db_->reverse_iterator();
    ASSERT_TRUE(it->next());  // construction and first positioning may allocate

    AllocationScope scope;
    int seen = 0;
    while (it->next()) {
        ASSERT_FALSE(it->key().empty());
        ++seen;
    }
    EXPECT_GT(seen, kKeys / 2);
    EXPECT_EQ(scope.count(), 0u) << "stepping an iterator backwards must not allocate per entry";
}

TEST_F(AllocationTest, AReversePrefixScanAllocatesOnlyWhileSettingUp) {
    const std::string prefix = "cluster:0012:";

    auto it = db_->reverse_prefix_iterator(Slice::from(prefix));
    ASSERT_TRUE(it->next());

    AllocationScope scope;
    int seen = 1;
    while (it->next()) ++seen;
    EXPECT_EQ(seen, 100);
    EXPECT_EQ(scope.count(), 0u);
}

// The counter has to be able to see an allocation, or the assertions above are
// vacuous.
TEST_F(AllocationTest, TheCounterIsNotVacuous) {
    AllocationScope scope;
    auto* leaky = new std::string(1000, 'x');
    EXPECT_GT(scope.count(), 0u);
    delete leaky;
}

}  // namespace
}  // namespace elysiumkv::test
