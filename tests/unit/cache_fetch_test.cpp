#include "blob/range_cache.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/blob_store.hpp"
#include "elysiumkv/disk_blob_store.hpp"
#include "elysiumkv/memory_cache_blob_store.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

namespace elysiumkv::test {
namespace {

/// Counts what actually reaches the backing store. A cache is not visible in any answer — the same
/// bytes come back either way — so the only way to assert it did something is to count underneath.
class CountingStore final : public BlobStore {
public:
    explicit CountingStore(std::shared_ptr<BlobStore> inner) : inner_(std::move(inner)) {}

    std::string id() const override { return inner_->id(); }
    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
        gets_.fetch_add(1, std::memory_order_relaxed);
        return inner_->get(name, offset, len);
    }
    std::future<Status> put(std::string_view name, Slice bytes) override {
        return inner_->put(name, bytes);
    }
    std::future<Status> remove(std::string_view name) override { return inner_->remove(name); }
    std::future<Result<std::vector<std::string>>> list(std::string_view prefix) override {
        return inner_->list(prefix);
    }

    uint64_t gets() const { return gets_.load(); }
    void reset() { gets_.store(0); }

private:
    std::shared_ptr<BlobStore> inner_;
    std::atomic<uint64_t> gets_{0};
};

// --- the plan ---------------------------------------------------------------------------

TEST(FetchPlanTest, ZeroGranularityAsksForExactlyWhatWasWanted) {
    const FetchPlan plan = plan_fetch(4096, 1024, 0);
    EXPECT_EQ(plan.offset, 4096u);
    EXPECT_EQ(plan.len, 1024u);
}

TEST(FetchPlanTest, AMissIsRoundedOutToItsChunk) {
    const FetchPlan plan = plan_fetch(5000, 100, 4096);
    EXPECT_EQ(plan.offset, 4096u) << "aligned down to a boundary";
    EXPECT_EQ(plan.len, 4096u) << "and out to the end of that chunk";
}

/// A plan is always a superset. Shrinking a request would turn a complete answer into a partial
/// one, which this cache treats as wrong rather than short.
TEST(FetchPlanTest, ARequestLargerThanTheChunkIsNotShrunk) {
    const FetchPlan plan = plan_fetch(100, 10'000, 4096);
    EXPECT_LE(plan.offset, 100u);
    EXPECT_GE(plan.offset + plan.len, 100u + 10'000u);
}

TEST(FetchPlanTest, AReadToTheEndStaysAReadToTheEnd) {
    const FetchPlan plan = plan_fetch(5000, BlobStore::kReadToEnd, 4096);
    EXPECT_EQ(plan.offset, 4096u);
    EXPECT_EQ(plan.len, BlobStore::kReadToEnd);
}

// --- the cache --------------------------------------------------------------------------

struct Fixture {
    TempDir dir;
    std::shared_ptr<CountingStore> counter;
    std::unique_ptr<MemoryCacheBlobStore> cache;
    std::string object;

    explicit Fixture(size_t granularity, size_t object_bytes = 256 * 1024) {
        auto local = std::make_shared<DiskBlobStore>(dir.path(), "store-0");
        counter = std::make_shared<CountingStore>(local);
        cache = std::make_unique<MemoryCacheBlobStore>(counter, nullptr, 64ull << 20,
                                                       /*cache_on_write=*/false, granularity);
        // A paranoid build re-reads from the delegate on every *hit* to verify the cached bytes.
        // That is a good check and a useless one here: these tests count reads, and verification
        // would swamp the number they are about. Turned off explicitly rather than by skipping the
        // test, so the assertions still run in every build.
        cache->set_verify_against_delegate(false);
        object.assign(object_bytes, 'x');
        EXPECT_EQ(local->put("000000000001.sst", Slice::from(object)).get(), Status::Ok);
        counter->reset();
    }

    /// Reads the object 4 KiB at a time, as a scan over its blocks would.
    uint64_t read_in_blocks(size_t block = 4096) {
        for (uint64_t at = 0; at < object.size(); at += block) {
            auto got = cache->get("000000000001.sst", at, block).get();
            EXPECT_TRUE(got.has_value());
            EXPECT_EQ(got->size(), block);
        }
        return counter->gets();
    }
};

/// The point of the whole thing. A sequential read costs one request per chunk rather than
/// one per block, and it needs no notion of a scan to do it.
TEST(CacheFetch, ChunkingCollapsesASequentialReadIntoFarFewerRequests) {
    Fixture plain(/*granularity=*/0);
    const uint64_t unchunked = plain.read_in_blocks();

    Fixture chunked(/*granularity=*/64 * 1024);
    const uint64_t coalesced = chunked.read_in_blocks();

    EXPECT_EQ(unchunked, 64u) << "256 KiB in 4 KiB reads";
    EXPECT_EQ(coalesced, 4u) << "256 KiB in 64 KiB chunks";
}

/// The bytes have to be the ones asked for, not the chunk they arrived in.
TEST(CacheFetch, AChunkedReadReturnsExactlyTheWindowRequested) {
    TempDir dir;
    auto local = std::make_shared<DiskBlobStore>(dir.path(), "store-0");
    std::string object;
    for (int i = 0; i < 4096; ++i) object.push_back(static_cast<char>(i % 251));
    ASSERT_EQ(local->put("000000000001.sst", Slice::from(object)).get(), Status::Ok);

    MemoryCacheBlobStore cache(local, nullptr, 64ull << 20, false, /*granularity=*/1024);
    cache.set_verify_against_delegate(false);
    for (uint64_t at : {0u, 100u, 1023u, 1024u, 3000u}) {
        auto got = cache.get("000000000001.sst", at, 64).get();
        ASSERT_TRUE(got.has_value()) << "at " << at;
        ASSERT_EQ(got->size(), 64u) << "at " << at;
        EXPECT_EQ(std::string(got->begin(), got->end()), object.substr(at, 64)) << "at " << at;
    }
}

/// A read overlapping the end is short, not an error — and chunking must not change that, since the
/// chunk it rounds out to runs past the object.
TEST(CacheFetch, AReadOverlappingTheEndIsStillTruncatedRatherThanFailed) {
    TempDir dir;
    auto local = std::make_shared<DiskBlobStore>(dir.path(), "store-0");
    const std::string object(5000, 'y');
    ASSERT_EQ(local->put("000000000001.sst", Slice::from(object)).get(), Status::Ok);

    MemoryCacheBlobStore cache(local, nullptr, 64ull << 20, false, /*granularity=*/4096);
    cache.set_verify_against_delegate(false);
    auto got = cache.get("000000000001.sst", 4900, 500).get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->size(), 100u) << "only 100 bytes exist past 4900";

    auto to_end = cache.get("000000000001.sst", 4096, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(to_end.has_value());
    EXPECT_EQ(to_end->size(), 904u);
}

/// Chunking must not make a repeat read cost anything: the second pass is served entirely from what
/// the first pulled.
TEST(CacheFetch, ASecondPassCostsNothing) {
    Fixture chunked(/*granularity=*/64 * 1024);
    chunked.read_in_blocks();
    chunked.counter->reset();
    EXPECT_EQ(chunked.read_in_blocks(), 0u);
}

}  // namespace
}  // namespace elysiumkv::test
