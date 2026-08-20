// ARCHITECTURE.md "Immutable named objects", ARCHITECTURE.md "Caches chain" — the caches under injected failure, which is the one set of claims about
// them the differential oracle cannot check: that suite demands exact results, and an
// injected `Io` makes a read fail legitimately.
//
// The claim that matters most is not about performance. ARCHITECTURE.md "Immutable named objects" makes `NotFound` *positive
// evidence* that an object is absent, and the whole-store discard path acts on it — so a
// layer that turned "the store below could not answer" into "the object is not there"
// would turn an outage into apparent data loss. A cache sits directly on that path.

#include "fault/fault_injecting_blob_store.hpp"

#include "sst/sst_reader.hpp"
#include "sst/sst_writer.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/disk_cache_blob_store.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/disk_blob_store.hpp"
#include "elysiumkv/memory_budget.hpp"
#include "elysiumkv/memory_cache_blob_store.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

using Op = FaultInjectingBlobStore::Op;

/// memory over disk over a fault injector over the real store — the chain with the
/// failure point at the bottom, where an authoritative store's failures come from.
class CacheFaultTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directories(dir_.path() / "store");
        local_ = std::make_shared<DiskBlobStore>(dir_.path() / "store", "store-0");
        local_->set_sync_writes(false);
        faulty_ = std::make_shared<FaultInjectingBlobStore>(local_);
        disk_ = std::make_shared<DiskCacheBlobStore>(faulty_, dir_.path() / "disk", 8u << 20,
                                                    /*cache_on_write=*/true);
        memory_ = std::make_shared<MemoryCacheBlobStore>(disk_, nullptr, 1u << 20,
                                                         /*cache_on_write=*/true);
    }

    static std::string as_string(const Buffer& b) {
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }

    TempDir dir_;
    std::shared_ptr<DiskBlobStore> local_;
    std::shared_ptr<FaultInjectingBlobStore> faulty_;
    std::shared_ptr<DiskCacheBlobStore> disk_;
    std::shared_ptr<MemoryCacheBlobStore> memory_;
};

/// The one that would be catastrophic. A miss that reaches an unreachable store must
/// report `Io`, never `NotFound`: absence is what the discard path acts on, and a cache
/// converting one into the other would let a transient outage look like a wiped store.
TEST_F(CacheFaultTest, AMissAgainstAnUnreachableStoreIsIoNeverNotFound) {
    ASSERT_EQ(memory_->put("000000000001.sst", Slice::from(std::string(4096, 'x'))).get(),
              Status::Ok);

    // A different object, never cached, so the read has to go all the way down.
    faulty_->set_unreachable(true);
    auto missing = memory_->get("000000000002.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), Status::Io)
        << "a cache must not turn 'could not determine' into 'definitely absent'";

    // And `list`, which is the actual evidence the discard path uses.
    auto listed = memory_->list("").get();
    ASSERT_FALSE(listed.has_value());
    EXPECT_EQ(listed.error(), Status::Io);
}

/// A genuine absence still reads as absence through the chain — the other half of the
/// same rule, and the reason the case above is not simply "return Io for everything".
TEST_F(CacheFaultTest, AGenuineAbsenceIsStillNotFoundThroughTheChain) {
    auto missing = memory_->get("000000000099.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), Status::NotFound);
}

/// ARCHITECTURE.md "Caches chain" — "a missing or corrupt cache entry is always refetchable from below; cache loss
/// costs latency and nothing else". Both layers wiped mid-flight, from under a live store.
TEST_F(CacheFaultTest, LosingBothCacheLayersMidFlightCostsOnlyLatency) {
    const std::string bytes(8192, 'w');
    ASSERT_EQ(memory_->put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);
    ASSERT_TRUE(memory_->get("000000000001.sst", 0, BlobStore::kReadToEnd).get().has_value());

    // The disk layer's directory removed behind its back, which ARCHITECTURE.md "Caches chain" explicitly permits.
    std::error_code ec;
    std::filesystem::remove_all(dir_.path() / "disk", ec);

    auto after = memory_->get("000000000001.sst", 0, 100).get();
    ASSERT_TRUE(after.has_value()) << "the memory layer still holds it";
    EXPECT_EQ(as_string(*after), bytes.substr(0, 100));

    // Now go around the memory layer too: a fresh chain over the same store, with both
    // caches empty, must still read the object.
    auto fresh_disk = std::make_shared<DiskCacheBlobStore>(faulty_, dir_.path() / "disk2",
                                                           8u << 20, false);
    MemoryCacheBlobStore fresh(fresh_disk, nullptr, 1u << 20, false);
    auto refetched = fresh.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(refetched.has_value());
    EXPECT_EQ(as_string(*refetched), bytes);
}

/// A cache *can* answer while the store below cannot, and that is not a bug — it is the
/// one situation where a cache buys availability rather than latency. Worth pinning
/// because it is also what forced the paranoid self-check to treat "the delegate errored"
/// as "cannot verify" rather than "the cache is wrong".
TEST_F(CacheFaultTest, ACachedRangeIsServedWhileTheStoreBelowIsUnreachable) {
    const std::string bytes(4096, 'c');
    ASSERT_EQ(memory_->put("000000000001.sst", Slice::from(bytes)).get(), Status::Ok);
    ASSERT_TRUE(memory_->get("000000000001.sst", 0, BlobStore::kReadToEnd).get().has_value());

    faulty_->set_unreachable(true);
    auto served = memory_->get("000000000001.sst", 0, 512).get();
    ASSERT_TRUE(served.has_value()) << "a cached range does not need the store below";
    EXPECT_EQ(as_string(*served), bytes.substr(0, 512));
}

/// A failed `put` must not leave the caches holding bytes the authoritative store never
/// accepted. Write-through means the store acknowledges *first*; this is the negative
/// control for that ordering.
TEST_F(CacheFaultTest, AFailedPutCachesNothing) {
    faulty_->add_rule({.op = Op::Put, .name_contains = "000000000007", .status = Status::Io});

    const Status status = memory_->put("000000000007.sst", Slice::from(std::string(1024, 'p'))).get();
    ASSERT_NE(status, Status::Ok);
    EXPECT_EQ(memory_->cached_bytes(), 0u) << "nothing may be cached for a put that failed";
    EXPECT_EQ(disk_->cached_bytes(), 0u);

    faulty_->clear_rules();
    auto absent = memory_->get("000000000007.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(absent.has_value()) << "and the object must not appear to exist";
    EXPECT_EQ(absent.error(), Status::NotFound);
}

/// A whole database over the chain, with the store below failing and recovering. ARCHITECTURE.md "A tier is not a level" — a
/// store that cannot answer is not a store that lost data — and a cache in the way must
/// not change that verdict either way.
/// A rotted cache is not a corrupt store. Only the block layer runs the checksum, so a cached
/// copy that was truncated or flipped surfaced as `Status::Corrupt` for the *store* while the
/// authoritative object was untouched — which sends an operator to a restore for a healthy store,
/// and breaks the promise the cache design rests on: a cache may be absent, never wrong.
///
/// A test double rather than `DiskCacheBlobStore`, deliberately. That one is checked by
/// `verify_cache_hit`, which re-fetches every hit and aborts on a mismatch — so under the debug and
/// sanitizer presets it catches the rot itself and this path is never reached. It is compiled out
/// in release, which is precisely where the repair below is the only thing standing between a bad
/// chunk and a false corruption report.
TEST_F(CacheFaultTest, ACorruptCachedBlockIsRepairedFromTheAuthority) {
    /// Serves the delegate's bytes with one flipped, until it is told to forget the object.
    class RottingCache final : public CacheBlobStore {
    public:
        explicit RottingCache(std::shared_ptr<BlobStore> delegate)
            : delegate_(std::move(delegate)) {}

        BlobStore& delegate() override { return *delegate_; }
        size_t max_cache_bytes() const override { return 0; }
        bool cache_on_write() const override { return false; }
        uint64_t hits() const override { return 0; }
        uint64_t misses() const override { return 0; }

        void invalidate(std::string_view) override {
            ++invalidations;
            rotting = false;   // forgetting the bad copy is what invalidation means
        }

        std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
            auto bytes = delegate_->get(name, offset, len).get();
            if (rotting && bytes && !bytes->empty()) {
                (*bytes)[bytes->size() / 2] ^= 0xFF;
            }
            return make_ready_future(std::move(bytes));
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

        bool rotting = false;
        int invalidations = 0;

    private:
        std::shared_ptr<BlobStore> delegate_;
    };

    SstWriter writer({.bloom_bits_per_key = 10, .compression = Compression::None});
    for (int i = 0; i < 2000; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        writer.add(Slice::from(std::string_view(key)), ValueType::Put,
                   Slice::from(std::string(64, 'v')));
    }
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(local_->put("000000000001.sst", Slice::from(built->bytes)).get(), Status::Ok);

    auto cache = std::make_shared<RottingCache>(local_);
    auto reader = SstReader::open(*cache, "000000000001.sst", built->bytes.size(),
                                  {.block_bytes = 512});
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());

    // Warm, so the footer and index are resident: those are read outside `load_block` and are a
    // different path. What is under test is a data block.
    ASSERT_TRUE((*reader)->get(Slice::from(std::string("key:000000"))).has_value());

    cache->rotting = true;
    auto found = (*reader)->get(Slice::from(std::string("key:001000")));
    ASSERT_TRUE(found.has_value())
        << "a rotted cache reported the store as " << status_name(found.error());
    EXPECT_TRUE(found->has_value()) << "the authority holds this key, so the retry must find it";
    EXPECT_GT(cache->invalidations, 0) << "the bad copy must be forgotten, not merely bypassed";
}

TEST_F(CacheFaultTest, ADatabaseOverACachedChainSurvivesAnOutage) {
    auto catalog = std::make_shared<DiskManifestCatalog>(dir_.path());
    Options options;
    options.manifest_catalog = catalog;
    options.memtable_bytes = 32u << 10;
    options.block_bytes = 512;
    options.paranoid_checks = true;
    options.background = BackgroundMode::Inline;
    LevelOptions l0;
    l0.max_files = 4;
    options.levels = {{0, l0}, {1, LevelOptions{}}};
    options.tiers = {Tier{.store = memory_, .durability = Durability::Durable}};

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);

    for (int i = 0; i < 400; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        ASSERT_EQ(db->put(Slice::from(std::string_view(key)), Slice::from(std::string(100, 'v'))),
                  Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    db.reset();

    // Reopening while the store cannot answer must fail retryably: no discard, no
    // manifest write, and emphatically not an empty store.
    faulty_->set_unreachable(true);
    auto failed = DB::open(options);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), Status::Io)
        << "an unreachable store behind a cache must not look like an empty one";

    // The data was there the whole time; only the listing failed.
    faulty_->set_unreachable(false);
    auto recovered = DB::open(options);
    ASSERT_TRUE(recovered.has_value()) << status_name(recovered.error());
    for (int i = 0; i < 400; i += 13) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        auto found = (*recovered)->get(Slice::from(std::string_view(key)));
        ASSERT_TRUE(found.has_value()) << key << ": " << status_name(found.error());
    }
}

}  // namespace
}  // namespace elysiumkv::test
