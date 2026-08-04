#include "cache/sharded_lru.hpp"

#include "elysiumkv/memory_budget.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

namespace elysiumkv {
namespace {

std::shared_ptr<const Block> block_of(size_t size) {
    return std::make_shared<const Block>(Buffer(size, 'x'));
}

TEST(BlockCache, RoundTripsAndMisses) {
    ShardedLruBlockCache cache(1u << 20);
    EXPECT_EQ(cache.get(1, 0), nullptr);

    cache.insert(1, 0, block_of(100));
    auto found = cache.get(1, 0);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->size(), 100u);

    EXPECT_EQ(cache.get(1, 4096), nullptr) << "a different offset is a different block";
    EXPECT_EQ(cache.get(2, 0), nullptr) << "a different file is a different block";
    EXPECT_EQ(cache.hits(), 1u);
    EXPECT_EQ(cache.misses(), 3u);
}

TEST(BlockCache, EvictsUnderCapacity) {
    // Small enough that each shard holds only a few blocks.
    ShardedLruBlockCache cache(ShardedLruBlockCache::kNumShards * 4096);
    for (uint64_t i = 0; i < 1000; ++i) cache.insert(1, i * 4096, block_of(1024));

    EXPECT_LE(cache.approximate_bytes(), ShardedLruBlockCache::kNumShards * 4096 * 2);

    int resident = 0;
    for (uint64_t i = 0; i < 1000; ++i) {
        if (cache.get(1, i * 4096) != nullptr) ++resident;
    }
    EXPECT_GT(resident, 0);
    EXPECT_LT(resident, 1000);
}

// ARCHITECTURE.md "Reads don't copy" — a reader's use outlives eviction naturally, which is what lets the
// public API hand out a borrowed value without the cache tracking pins.
TEST(BlockCache, EvictedBlocksStayAliveForWhoeverHoldsThem) {
    ShardedLruBlockCache cache(ShardedLruBlockCache::kNumShards * 2048);
    cache.insert(7, 0, block_of(1024));

    auto pinned = cache.get(7, 0);
    ASSERT_NE(pinned, nullptr);

    for (uint64_t i = 1; i < 500; ++i) cache.insert(7, i * 4096, block_of(1024));
    EXPECT_EQ(cache.get(7, 0), nullptr) << "the block should be gone from the cache";
    EXPECT_EQ(pinned->size(), 1024u) << "but still readable through the held pointer";
}

// Called when an SST is unlinked (ARCHITECTURE.md "Versions are immutable snapshots"): nothing of that file may survive, and
// nothing of any other file may be touched.
TEST(BlockCache, EvictFileRemovesExactlyThatFile) {
    ShardedLruBlockCache cache(1u << 20);
    for (uint64_t offset = 0; offset < 100; ++offset) {
        cache.insert(1, offset * 4096, block_of(64));
        cache.insert(2, offset * 4096, block_of(64));
    }

    cache.evict_file(1);
    for (uint64_t offset = 0; offset < 100; ++offset) {
        EXPECT_EQ(cache.get(1, offset * 4096), nullptr) << offset;
        EXPECT_NE(cache.get(2, offset * 4096), nullptr) << offset;
    }
    EXPECT_GT(cache.approximate_bytes(), 0u);
}

// Block offsets are multiples of the block size, so a hash that does not mix
// sends every block of a file to one shard. That costs fifteen sixteenths of the
// capacity and serialises every lookup — and it looks like nothing but a low hit
// rate, so it is worth an explicit test.
TEST(BlockCache, BlocksOfOneFileSpreadAcrossShards) {
    constexpr size_t kBlocks = ShardedLruBlockCache::kNumShards * 8;
    constexpr size_t kBlockBytes = 4096;
    // Four times the headroom needed: shard occupancy is random, so a fair hash
    // still varies. One shard holding everything would evict three quarters of
    // it, which is the failure this is looking for.
    ShardedLruBlockCache cache(kBlocks * (kBlockBytes + 256) * 4);

    for (uint64_t i = 0; i < kBlocks; ++i) cache.insert(1, i * kBlockBytes, block_of(kBlockBytes));

    size_t resident = 0;
    for (uint64_t i = 0; i < kBlocks; ++i) {
        if (cache.get(1, i * kBlockBytes) != nullptr) ++resident;
    }
    EXPECT_EQ(resident, kBlocks) << "blocks piled into one shard and evicted each other";
}

TEST(BlockCache, ReportsToTheSharedMemoryBudget) {
    MemoryBudget budget(1u << 30);
    {
        ShardedLruBlockCache cache(1u << 20, &budget);
        for (uint64_t i = 0; i < 100; ++i) cache.insert(1, i * 4096, block_of(1024));
        EXPECT_GE(budget.used(), 100u * 1024);
    }
    EXPECT_EQ(budget.used(), 0u) << "the cache releases everything it held on destruction";
}

TEST(MemoryBudget, RefusesBeyondTotalAndRecovers) {
    MemoryBudget budget(1000);
    EXPECT_TRUE(budget.try_acquire(600));
    EXPECT_FALSE(budget.try_acquire(600));
    EXPECT_EQ(budget.used(), 600u);

    budget.release(600);
    EXPECT_EQ(budget.used(), 0u);
    EXPECT_TRUE(budget.try_acquire(1000));
    EXPECT_FALSE(budget.try_acquire(1));
}

TEST(BlockCache, ConcurrentAccessIsSafe) {
    ShardedLruBlockCache cache(1u << 20);
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&cache, t] {
            for (uint64_t i = 0; i < 2000; ++i) {
                const uint64_t file = static_cast<uint64_t>(t % 2);
                cache.insert(file, i * 4096, block_of(256));
                (void)cache.get(file, (i / 2) * 4096);
                if (i % 500 == 0) cache.evict_file(file);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_LE(cache.approximate_bytes(), 2u << 20);
}

}  // namespace
}  // namespace elysiumkv
