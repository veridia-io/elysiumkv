// ARCHITECTURE.md "Caches chain" — the cache decorators, run through **the same contract suite as every other
// store**. That is the point of them being `BlobStore`s: the engine never learns they
// exist, so nothing about the seam may change when one is in the chain.
//
// The contract is also the sharpest test a cache can be given. Half its cases are
// about reads returning exactly the right bytes — ranged reads, reads past the end,
// empty objects, a 5 MiB object read at offsets — and a cache that answers any of
// them from the wrong entry fails there rather than in production six months later.

#include "elysiumkv/memory_budget.hpp"
#include "contract/blob_store_contract.hpp"
#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/disk_cache_blob_store.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/disk_blob_store.hpp"
#include "elysiumkv/memory_cache_blob_store.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

using Op = FaultInjectingBlobStore::Op;

constexpr size_t kGenerousCache = 64u << 20;

/// The temp directory outlives the store it backs, so the deleter holds it.
template <typename T>
std::shared_ptr<BlobStore> keep_alive(T* store, std::shared_ptr<TempDir> dir) {
    return std::shared_ptr<BlobStore>(store, [dir](BlobStore* p) { delete p; });
}

std::shared_ptr<BlobStore> make_memory_cache() {
    auto dir = std::make_shared<TempDir>();
    auto below = std::make_shared<DiskBlobStore>(dir->path());
    return keep_alive(new MemoryCacheBlobStore(below, std::make_shared<MemoryBudget>(kGenerousCache),
                                              kGenerousCache, /*cache_on_write=*/true),
                      dir);
}

std::shared_ptr<BlobStore> make_disk_cache() {
    auto dir = std::make_shared<TempDir>();
    std::filesystem::create_directories(dir->path() / "store");
    auto below = std::make_shared<DiskBlobStore>(dir->path() / "store");
    return keep_alive(
        new DiskCacheBlobStore(below, dir->path() / "cache", kGenerousCache, true), dir);
}

/// ARCHITECTURE.md "Caches chain" — the example chain, minus the remote store: memory over disk over local. A cache
/// is a `BlobStore`, so composing them needs nothing new — and this asserts that the
/// composition, not just each layer, still satisfies the contract.
std::shared_ptr<BlobStore> make_chain() {
    auto dir = std::make_shared<TempDir>();
    std::filesystem::create_directories(dir->path() / "store");
    auto below = std::make_shared<DiskBlobStore>(dir->path() / "store");
    auto disk = std::make_shared<DiskCacheBlobStore>(below, dir->path() / "disk", kGenerousCache,
                                                     true);
    return keep_alive(new MemoryCacheBlobStore(disk, std::make_shared<MemoryBudget>(kGenerousCache),
                                              kGenerousCache, true),
                      dir);
}

INSTANTIATE_TEST_SUITE_P(
    Caches, BlobStoreContract,
    ::testing::Values(BlobStoreFactory{"MemoryCacheBlobStore", make_memory_cache},
                      BlobStoreFactory{"DiskCacheBlobStore", make_disk_cache},
                      BlobStoreFactory{"MemoryOverDiskOverLocal", make_chain}),
    BlobStoreFactoryName());

// --- behaviour specific to being a cache --------------------------------------

/// A counting store below the cache, so "did this read reach the authoritative
/// store" is answerable rather than inferred.
class CacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directories(dir_.path() / "store");
        local_ = std::make_shared<DiskBlobStore>(dir_.path() / "store");
        below_ = std::make_shared<FaultInjectingBlobStore>(local_);
    }

    static std::string as_string(const Buffer& b) {
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }

    TempDir dir_;
    std::shared_ptr<DiskBlobStore> local_;
    std::shared_ptr<FaultInjectingBlobStore> below_;
};

TEST_F(CacheTest, AReadIsServedFromTheCacheTheSecondTime) {
    MemoryCacheBlobStore cache(below_, nullptr, kGenerousCache, /*cache_on_write=*/false);
    // This case measures reads reaching the store below, and the paranoid self-check
    // reads from the store below on every hit. Off for the duration.
    cache.set_verify_against_delegate(false);
    const std::string bytes(4096, 'x');
    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);

    const uint64_t before = below_->call_count(Op::Get);
    auto first = cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(below_->call_count(Op::Get), before + 1) << "the first read must go down";

    auto second = cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(as_string(*second), bytes);
    EXPECT_EQ(below_->call_count(Op::Get), before + 1) << "the second read must not";
    EXPECT_EQ(cache.hits(), 1u);
    EXPECT_EQ(cache.misses(), 1u);
}

/// The reason `cache_on_write` exists: a freshly written L0 file is read almost
/// immediately by the next L0→L1 compaction. Without containment lookup the
/// write-through population would be dead weight, because `put` caches the whole
/// object and every later read is a block inside it.
TEST_F(CacheTest, CacheOnWriteServesTheFirstReadIncludingSubRanges) {
    MemoryCacheBlobStore cache(below_, nullptr, kGenerousCache, /*cache_on_write=*/true);
    // This case measures reads reaching the store below, and the paranoid self-check
    // reads from the store below on every hit. Off for the duration.
    cache.set_verify_against_delegate(false);
    std::string bytes;
    for (int i = 0; i < 1000; ++i) bytes += static_cast<char>('a' + (i % 26));
    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);

    const uint64_t before = below_->call_count(Op::Get);
    auto whole = cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(as_string(*whole), bytes);

    auto middle = cache.get("000000000001.sst", 400, 100).get();
    ASSERT_TRUE(middle.has_value());
    EXPECT_EQ(as_string(*middle), bytes.substr(400, 100))
        << "a range inside a cached whole object must come from the cache, correctly";

    EXPECT_EQ(below_->call_count(Op::Get), before)
        << "cache_on_write means the object was never read from below at all";
}

/// A bounded cached range must not answer a read that extends past it. Serving a
/// short buffer would look exactly like a read past the end of the object, which the
/// contract says is legal — so this failure mode is silent truncation.
TEST_F(CacheTest, ABoundedCachedRangeDoesNotAnswerALongerRead) {
    MemoryCacheBlobStore cache(below_, nullptr, kGenerousCache, false);
    const std::string bytes(2000, 'z');
    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);

    auto head = cache.get("000000000001.sst", 0, 100).get();
    ASSERT_TRUE(head.has_value() && head->size() == 100u);

    auto longer = cache.get("000000000001.sst", 0, 500).get();
    ASSERT_TRUE(longer.has_value());
    EXPECT_EQ(longer->size(), 500u) << "a 100-byte entry must not satisfy a 500-byte read";

    auto to_end = cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(to_end.has_value());
    EXPECT_EQ(to_end->size(), bytes.size())
        << "and it must not satisfy a read-to-end either — that is the truncating case";
}

TEST_F(CacheTest, RemovingAnObjectInvalidatesIt) {
    MemoryCacheBlobStore cache(below_, nullptr, kGenerousCache, true);
    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(std::string_view("cached"))).get(),
              Status::Ok);
    ASSERT_TRUE(cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get().has_value());

    ASSERT_EQ(cache.remove("000000000001.sst").get(), Status::Ok);
    auto gone = cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(gone.has_value()) << "a cache must not serve a deleted object";
    EXPECT_EQ(gone.error(), Status::NotFound);
    EXPECT_EQ(cache.cached_bytes(), 0u);
}

/// Batching must survive the chain. A cache that looped over `remove` would put a
/// remote delegate back to one DELETE per object after every compaction — the exact
/// cost `remove_many` exists to remove.
TEST_F(CacheTest, RemoveManyIsForwardedAsABatch) {
    MemoryCacheBlobStore cache(below_, nullptr, kGenerousCache, true);
    std::vector<std::string> names;
    for (int i = 1; i <= 5; ++i) {
        char name[32];
        std::snprintf(name, sizeof(name), "%012d.sst", i);
        names.emplace_back(name);
        ASSERT_EQ(cache.put(name, Slice::from(std::string_view("x"))).get(), Status::Ok);
    }
    ASSERT_GT(cache.cached_bytes(), 0u);

    const uint64_t before = below_->call_count(Op::RemoveMany);
    ASSERT_EQ(cache.remove_many(names).get(), Status::Ok);
    EXPECT_EQ(below_->call_count(Op::RemoveMany), before + 1)
        << "the cache must forward the batch, not unroll it";
    EXPECT_EQ(cache.cached_bytes(), 0u) << "and invalidate everything in it";
}

TEST_F(CacheTest, EvictionIsLeastRecentlyUsedAndBounded) {
    // Room for two objects of 1000 bytes, not three.
    MemoryCacheBlobStore cache(below_, nullptr, 2500, /*cache_on_write=*/true);
    // This case measures reads reaching the store below, and the paranoid self-check
    // reads from the store below on every hit. Off for the duration.
    cache.set_verify_against_delegate(false);
    const std::string bytes(1000, 'q');
    for (int i = 1; i <= 3; ++i) {
        char name[32];
        std::snprintf(name, sizeof(name), "%012d.sst", i);
        ASSERT_EQ(cache.put(name, Slice::from(bytes)).get(), Status::Ok);
        if (i == 2) {
            // Touch object 1 so object 2 becomes the least recently used.
            ASSERT_TRUE(cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get().has_value());
        }
    }
    EXPECT_LE(cache.cached_bytes(), 2500u) << "the bound is a bound";

    // **The survivor is checked first, and it has to be.** Reading the evicted
    // object repopulates it, which evicts something else — so asking about object 2
    // before object 1 destroys the evidence about object 1. The first draft of this
    // test did exactly that and blamed the cache.
    const uint64_t before = below_->call_count(Op::Get);
    ASSERT_TRUE(cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get().has_value());
    EXPECT_EQ(below_->call_count(Op::Get), before)
        << "object 1 was touched between the writes and must have survived";

    ASSERT_TRUE(cache.get("000000000002.sst", 0, BlobStore::kReadToEnd).get().has_value());
    EXPECT_EQ(below_->call_count(Op::Get), before + 1)
        << "object 2 was the least recently used and must have been the one evicted";
}

/// ARCHITECTURE.md "A process-wide memory budget" — an in-memory cache reports to the shared budget, and a full budget is a
/// slow read rather than a failure.
TEST_F(CacheTest, TheSharedBudgetBoundsPopulationAndIsReleased) {
    auto budget = std::make_shared<MemoryBudget>(1500);
    {
        MemoryCacheBlobStore cache(below_, budget, kGenerousCache, /*cache_on_write=*/true);
        const std::string bytes(1000, 'b');
        ASSERT_EQ(cache.put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);
        EXPECT_EQ(budget->used(), 1000u) << "the cache charges the shared budget";

        // The second object does not fit in the budget. The put must still succeed:
        // the authoritative store took it, and caching is optional by construction.
        ASSERT_EQ(cache.put("000000000002.sst", Slice::from(bytes)).get(), Status::Ok);
        EXPECT_EQ(budget->used(), 1000u) << "a refused acquire must not be double-counted";

        auto readable = cache.get("000000000002.sst", 0, BlobStore::kReadToEnd).get();
        ASSERT_TRUE(readable.has_value()) << "an uncached object is still readable";
        EXPECT_EQ(readable->size(), bytes.size());
    }
    EXPECT_EQ(budget->used(), 0u)
        << "a closed cache must give its budget back, or a process that opens and closes "
           "instances leaks the whole budget one cache at a time";
}

/// Repopulating the same range must not charge the shared budget twice. A read that
/// missed with a small range and later missed with a larger one at the same offset
/// hits the same key, and double-charging would drift the budget upward with no bytes
/// behind it — a leak that only shows up as caches mysteriously refusing to populate.
TEST_F(CacheTest, RepopulatingARangeDoesNotChargeTheBudgetTwice) {
    auto budget = std::make_shared<MemoryBudget>(1u << 20);
    MemoryCacheBlobStore cache(below_, budget, kGenerousCache, /*cache_on_write=*/false);
    const std::string bytes(4000, 'r');
    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);

    ASSERT_TRUE(cache.get("000000000001.sst", 0, 100).get().has_value());
    const size_t after_small = budget->used();
    EXPECT_EQ(after_small, 100u);

    // Same offset, longer range: the bounded entry cannot answer it, so this misses
    // and repopulates the same key.
    ASSERT_TRUE(cache.get("000000000001.sst", 0, 1000).get().has_value());
    EXPECT_EQ(budget->used(), 1000u)
        << "the replaced entry's bytes must come back off the budget, not accumulate";
    EXPECT_EQ(cache.cached_bytes(), 1000u) << "and the core must agree with the payload";
}

/// ARCHITECTURE.md "Caches chain" — compaction reads through `bulk_view()`, which unwraps the whole chain by
/// composition. Streaming a file it will never reread must not evict what the point
/// lookup path wants.
TEST_F(CacheTest, BulkViewBypassesTheChain) {
    auto disk = std::make_shared<DiskCacheBlobStore>(below_, dir_.path() / "disk", kGenerousCache,
                                                     false);
    MemoryCacheBlobStore memory(disk, nullptr, kGenerousCache, false);

    ASSERT_EQ(memory.put("000000000001.sst", Slice::from(std::string(4096, 'c'))).get(),
              Status::Ok);
    ASSERT_EQ(&memory.bulk_view(), &authoritative_store(memory))
        << "the chain must unwrap to the authoritative store";

    ASSERT_TRUE(memory.bulk_view().get("000000000001.sst", 0, BlobStore::kReadToEnd).get()
                    .has_value());
    EXPECT_EQ(memory.cached_bytes(), 0u) << "a bulk read must not populate the memory layer";
    EXPECT_EQ(disk->cached_bytes(), 0u) << "nor the disk layer";
    EXPECT_EQ(memory.misses(), 0u) << "and must not even be seen by it";
}

TEST_F(CacheTest, ACacheIsNotALocation) {
    MemoryCacheBlobStore cache(below_, nullptr, kGenerousCache, true);
    EXPECT_EQ(cache.id(), local_->id())
        << "id() is recorded per file in the manifest; a cache layer must not change it";

    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(std::string_view("x"))).get(), Status::Ok);
    auto listed = cache.list("").get();
    ASSERT_TRUE(listed.has_value());
    EXPECT_EQ(listed->size(), 1u)
        << "list must come from the authoritative store: a half-populated cache reporting its "
           "own contents would look like a store that had lost everything else";
    EXPECT_EQ(cache.as_cache(), &cache);
    EXPECT_EQ(&authoritative_store(cache), below_.get());
}

/// A wiped cache directory is valid (ARCHITECTURE.md "Caches chain"). The read must fall through, not fail.
TEST_F(CacheTest, LosingCachedBytesCostsLatencyAndNothingElse) {
    DiskCacheBlobStore cache(below_, dir_.path() / "disk", kGenerousCache, /*cache_on_write=*/true);
    const std::string bytes(2048, 'w');
    ASSERT_EQ(cache.put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);
    ASSERT_TRUE(cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get().has_value());

    std::error_code ec;
    std::filesystem::remove_all(dir_.path() / "disk", ec);

    auto after = cache.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(after.has_value()) << "a lost cache entry is refetchable by construction";
    EXPECT_EQ(as_string(*after), bytes);
}

/// **The whole point, end to end: the engine never learns any of this exists.**
/// ARCHITECTURE.md "Caches chain" says `Version`, compaction and the manifest are unchanged by a cache in the
/// chain, and the only way to believe that is to run a database over one.
TEST_F(CacheTest, ADatabaseRunsOverACachedTierAndTheCacheIsUsed) {
    auto catalog = std::make_shared<DiskManifestCatalog>(dir_.path());
    auto disk = std::make_shared<DiskCacheBlobStore>(below_, dir_.path() / "disk", 64u << 20,
                                                    /*cache_on_write=*/true);
    auto memory = std::make_shared<MemoryCacheBlobStore>(
        disk, std::make_shared<MemoryBudget>(8u << 20), 8u << 20, /*cache_on_write=*/false);

    Options options;
    options.manifest_catalog = catalog;
    options.memtable_bytes = 32u << 10;
    options.block_bytes = 512;
    options.paranoid_checks = true;
    // No block cache, so reads reach the blob chain instead of being answered above
    // it. ARCHITECTURE.md "Caches chain" is explicit that `BlockCache` and a memory blob cache are substitutes
    // over hot data, and this test is about the blob chain.
    LevelOptions l0;
    l0.max_files = 4;
    options.levels = {{0, l0}, {1, LevelOptions{}}};
    options.tiers = {Tier{.store = memory, .durability = Durability::Durable}};

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);

    for (int i = 0; i < 400; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        ASSERT_EQ(db->put(Slice::from(std::string_view(key)),
                          Slice::from(std::string(120, 'v'))),
                  Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);

    for (int i = 0; i < 400; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        auto value = db->get(Slice::from(std::string_view(key)));
        ASSERT_TRUE(value.has_value()) << key;
        EXPECT_EQ(value->value().size(), 120u);
    }

    EXPECT_GT(memory->hits() + disk->hits(), 0u)
        << "every read went to the authoritative store: the chain is inert";

    // And it survives a reopen, which is where a cache that had become authoritative
    // for anything would show up.
    db.reset();
    auto reopened = DB::open(options);
    ASSERT_TRUE(reopened.has_value()) << status_name(reopened.error());
    for (int i = 0; i < 400; i += 37) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        auto value = (*reopened)->get(Slice::from(std::string_view(key)));
        ASSERT_TRUE(value.has_value()) << key;
    }
}

}  // namespace
}  // namespace elysiumkv::test
