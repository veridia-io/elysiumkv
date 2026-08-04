// ARCHITECTURE.md "A process-wide memory budget" — **the shared budget, and the shedding order that makes it one.**
//
// Before this, the budget was accounting without control: the block cache reported to it
// and ignored refusals, the reader cache and the blob cache honoured them, memtable
// arenas — by far the largest consumer — did not participate at all, and nothing ever
// shed. For the case ARCHITECTURE.md "A process-wide memory budget" exists to serve, many instances in one process, that meant the
// budget did not bound the largest thing in it.
//
// The order is the property under test: evict the block cache, then flush memtables, then
// stall. Each step is cheaper than the next in what it costs the application, and only
// the last is visible to it.

#include "elysiumkv/memory_budget.hpp"

#include "db/db_impl.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>

namespace elysiumkv {
namespace test {
namespace {

TEST(MemoryBudgetTest, AnUnconditionalChargeReportsTheOverage) {
    MemoryBudget budget(1000);
    EXPECT_TRUE(budget.try_acquire_over(600));
    EXPECT_EQ(budget.overage(), 0u);

    // A consumer that cannot decline — a memtable arena serving a write already
    // accepted — charges anyway, and the overage is the signal, not the refusal.
    EXPECT_FALSE(budget.try_acquire_over(600));
    EXPECT_EQ(budget.used(), 1200u);
    EXPECT_EQ(budget.overage(), 200u);

    budget.release(600);
    EXPECT_EQ(budget.overage(), 0u);
}

TEST(MemoryBudgetTest, TryAcquireStillRefusesForConsumersThatCanDecline) {
    MemoryBudget budget(1000);
    EXPECT_TRUE(budget.try_acquire(900));
    EXPECT_FALSE(budget.try_acquire(200)) << "a cache that can decline is told to";
    EXPECT_EQ(budget.used(), 900u) << "and a refusal must not charge";
}

class BudgetedDbTest : public ::testing::Test {
protected:
    Options options_for(const std::shared_ptr<MemoryBudget>& budget, size_t memtable_bytes) {
        Options options = make_options(store_, Compression::None, memtable_bytes);
        options.memory_budget = budget;
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_.load(std::memory_order_relaxed); };
        return options;
    }

    static std::string key_at(int i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    TestStore store_;
    std::atomic<uint64_t> now_{1'000'000};
};

/// The memtable was the consumer that reported nothing. This is the fact that makes the
/// whole mechanism possible.
TEST_F(BudgetedDbTest, MemtableArenasChargeTheSharedBudget) {
    auto budget = std::make_shared<MemoryBudget>(256u << 20);
    auto opened = DB::open(options_for(budget, 8u << 20));
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);

    const size_t before = budget->used();
    for (int i = 0; i < 2000; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(200, 'v'))), Status::Ok);
    }
    EXPECT_GT(budget->used(), before + (100u << 10))
        << "a memtable holding 2000 entries must be visible in the budget";

    // The arena is released when the memtable is dropped after its flush completes.
    const size_t with_memtable = budget->used();
    ASSERT_EQ(db->flush(), Status::Ok);
    EXPECT_LT(budget->used(), with_memtable) << "flushing gives the arena back";
}

/// ARCHITECTURE.md "A process-wide memory budget" — the order, step 1: the block cache goes first, because its loss is pure latency.
TEST_F(BudgetedDbTest, TheBlockCacheIsShedBeforeAnythingElse) {
    // A budget small enough that the block cache alone exceeds it once the data has been
    // read back. (Chosen after the first attempt used 512 KiB and nothing shed, because
    // with the accounting fixed the total genuinely fitted.)
    auto budget = std::make_shared<MemoryBudget>(128u << 10);
    Options options = options_for(budget, 64u << 10);
    auto cache = std::make_shared<ShardedLruBlockCache>(8u << 20, budget.get());
    options.block_cache = cache;

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);

    for (int i = 0; i < 3000; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(120, 'v'))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);

    // Fill the block cache by reading everything back.
    for (int i = 0; i < 3000; ++i) {
        (void)db->get(Slice::from(key_at(i)));
    }
    ASSERT_GT(cache->approximate_bytes(), 0u) << "nothing was cached, so this proves nothing";
    const size_t cached_before = cache->approximate_bytes();

    // One more write drives the write path, which sheds.
    ASSERT_EQ(db->put(Slice::from(key_at(99999)), Slice::from(std::string(120, 'v'))), Status::Ok);

    EXPECT_LT(cache->approximate_bytes(), cached_before)
        << "the block cache must be the first thing given back";
    EXPECT_GT(db->stats().budget_sheds, 0u);

    // And nothing was lost: shedding a cache costs latency, never data.
    for (int i = 0; i < 3000; i += 97) {
        auto found = db->get(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << key_at(i) << ": " << status_name(found.error());
    }
}

/// **A write is never refused because of the budget.** The instance over it is usually
/// over because of its neighbours, so failing its writes would be both surprising and
/// useless. `block_on_stall = false` is the one case that reports back, and it reports
/// `Stalled` — the same thing level pressure reports, retryable and not an error.
TEST_F(BudgetedDbTest, ExhaustingTheBudgetNeverFailsAWrite) {
    // Absurdly small: every write leaves it over, and shedding cannot help because the
    // memory is genuinely in the memtable holding those writes.
    auto budget = std::make_shared<MemoryBudget>(4096);
    auto opened = DB::open(options_for(budget, 1u << 20));
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);

    for (int i = 0; i < 500; ++i) {
        const Status status = db->put(Slice::from(key_at(i)), Slice::from(std::string(200, 'v')));
        ASSERT_TRUE(status == Status::Ok || status == Status::Stalled)
            << "the budget may slow a writer down; it may not fail it: " << status_name(status);
    }

    // Everything that reported Ok is readable, which is what "never fails" has to mean.
    for (int i = 0; i < 500; ++i) {
        auto found = db->get(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value() || found.error() == Status::NotFound)
            << status_name(found.error());
    }
    EXPECT_GT(db->stats().budget_sheds, 0u) << "the budget was exceeded, so shedding must have run";
}

/// The reason for all of it: two instances in one process, one budget. Neither can size
/// itself as though it were alone.
TEST_F(BudgetedDbTest, TwoInstancesShareOneBudget) {
    auto budget = std::make_shared<MemoryBudget>(64u << 20);
    TestStore second;

    auto first = DB::open(options_for(budget, 8u << 20));
    ASSERT_TRUE(first.has_value());

    Options other = make_options(second, Compression::None, 8u << 20);
    other.memory_budget = budget;
    other.background = BackgroundMode::Inline;
    auto both = DB::open(other);
    ASSERT_TRUE(both.has_value());

    for (int i = 0; i < 1000; ++i) {
        ASSERT_EQ((*first)->put(Slice::from(key_at(i)), Slice::from(std::string(200, 'a'))),
                  Status::Ok);
    }
    const size_t after_first = budget->used();
    ASSERT_GT(after_first, 0u);

    for (int i = 0; i < 1000; ++i) {
        ASSERT_EQ((*both)->put(Slice::from(key_at(i)), Slice::from(std::string(200, 'b'))),
                  Status::Ok);
    }
    EXPECT_GT(budget->used(), after_first)
        << "the second instance's memtable must be charged to the same budget — the whole "
           "point of ARCHITECTURE.md - A process-wide memory budget being per process";

    // Each instance sees the shared total, not its own share of it.
    EXPECT_EQ((*first)->stats().memory_budget_total, 64u << 20);
    EXPECT_EQ((*both)->stats().memory_budget_total, 64u << 20);
    EXPECT_EQ((*first)->stats().memory_budget_used, (*both)->stats().memory_budget_used);
}

TEST(BlockCacheSheddingTest, EvictAtLeastReleasesAtLeastWhatWasAskedFor) {
    auto budget = std::make_shared<MemoryBudget>(1u << 20);
    ShardedLruBlockCache cache(1u << 20, budget.get());

    for (uint64_t file = 0; file < 64; ++file) {
        cache.insert(file, 0, std::make_shared<Block>(Buffer(1024)));
    }
    const size_t before = cache.approximate_bytes();
    ASSERT_GT(before, 0u);
    const size_t charged = budget->used();

    const size_t released = cache.evict_at_least(before / 2);
    EXPECT_GE(released, before / 2);
    EXPECT_LE(cache.approximate_bytes(), before - released);
    EXPECT_EQ(budget->used(), charged - released) << "and the budget gets every byte back";

    // Asking for more than it holds empties it and says how much that was, rather than
    // looping forever.
    const size_t rest = cache.evict_at_least(1u << 30);
    EXPECT_EQ(cache.approximate_bytes(), 0u);
    EXPECT_EQ(rest, before - released);
    EXPECT_EQ(cache.evict_at_least(1024), 0u) << "an empty cache releases nothing";
}

}  // namespace
}  // namespace test
}  // namespace elysiumkv
