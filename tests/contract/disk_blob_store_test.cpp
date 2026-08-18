#include "contract/blob_store_contract.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>

namespace elysiumkv::test {
namespace {

namespace fs = std::filesystem;

std::shared_ptr<BlobStore> make_local_store() {
    auto dir = std::make_shared<TempDir>();
    auto* store = new DiskBlobStore(dir->path());
    // The temp directory outlives the store it backs.
    return std::shared_ptr<BlobStore>(store, [dir](BlobStore* p) { delete p; });
}

INSTANTIATE_TEST_SUITE_P(Disk, BlobStoreContract,
                         ::testing::Values(BlobStoreFactory{"DiskBlobStore",
                                                            make_local_store}),
                         BlobStoreFactoryName());

// --- behaviour specific to a filesystem-backed store --------------------------

class DiskBlobStoreTest : public ::testing::Test {
protected:
    TempDir dir_;
    DiskBlobStore store_{dir_.path()};
};

// ARCHITECTURE.md "Immutable named objects" — a *missing* root is ambiguous between a fresh volume, a wrong path and a
// failed mount. It must fail loudly rather than resemble a fresh one — and it
// must never look like absence, because absence is what triggers a discard.
TEST_F(DiskBlobStoreTest, MissingRootIsIoNeverNotFound) {
    fs::remove_all(dir_.path());

    auto value = store_.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), Status::Io);

    auto names = store_.list("").get();
    ASSERT_FALSE(names.has_value());
    EXPECT_EQ(names.error(), Status::Io);

    EXPECT_EQ(store_.put("000000000001.sst", Slice::from(std::string_view("x"))).get(),
              Status::Io);
    EXPECT_EQ(store_.remove("000000000001.sst").get(), Status::Io);
}

// The descriptor cache is bounded, and the bound is small — so the common case for a store with
// more files than descriptors is reading through eviction, on every read.
TEST_F(DiskBlobStoreTest, ReadsAreCorrectAcrossDescriptorCacheEviction) {
    store_.set_max_open_files(4);

    const auto name = [](int i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%012d.sst", i);
        return std::string(buf);
    };
    for (int i = 0; i < 40; ++i) {
        const std::string bytes = "object-" + std::to_string(i);
        ASSERT_EQ(store_.put(name(i), Slice::from(bytes)).get(), Status::Ok);
    }

    // Twice through, so the second pass reads names the first pass evicted.
    for (int pass = 0; pass < 2; ++pass) {
        for (int i = 0; i < 40; ++i) {
            auto value = store_.get(name(i), 0, BlobStore::kReadToEnd).get();
            ASSERT_TRUE(value.has_value()) << name(i);
            EXPECT_EQ(std::string(reinterpret_cast<const char*>(value->data()), value->size()),
                      "object-" + std::to_string(i));
        }
    }
}

// Zero holds nothing, which is what a process against a low descriptor limit wants.
TEST_F(DiskBlobStoreTest, ADisabledDescriptorCacheStillReads) {
    store_.set_max_open_files(0);
    ASSERT_EQ(store_.put("000000000001.sst", Slice::from(std::string_view("hello"))).get(),
              Status::Ok);

    auto value = store_.get("000000000001.sst", 1, 3).get();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(value->data()), value->size()), "ell");

    ASSERT_EQ(store_.remove("000000000001.sst").get(), Status::Ok);
    EXPECT_EQ(store_.get("000000000001.sst", 0, BlobStore::kReadToEnd).get().error(),
              Status::NotFound);
}

// A replaced ephemeral volume normally presents as an *empty* directory, which
// lists successfully — and that is the only shape of evidence a discard may use.
TEST_F(DiskBlobStoreTest, EmptyExistingRootListsSuccessfullyEmpty) {
    auto names = store_.list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_TRUE(names->empty());
}

TEST_F(DiskBlobStoreTest, PartialWritesAreNeverVisible) {
    std::ofstream(dir_.path() / ".tmp.1234.0.000000000001.sst") << "half a file";

    auto names = store_.list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_TRUE(names->empty());

    auto value = store_.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), Status::NotFound);
}

// store_id is persisted in FileMetadata, so it must not change across restarts.
TEST_F(DiskBlobStoreTest, IdIsStableAcrossInstances) {
    DiskBlobStore other(dir_.path());
    EXPECT_EQ(store_.id(), other.id());

    DiskBlobStore relative(dir_.path() / "." / "");
    EXPECT_EQ(store_.id(), relative.id());

    DiskBlobStore named(dir_.path(), "express-tier");
    EXPECT_EQ(named.id(), "express-tier");
}

TEST_F(DiskBlobStoreTest, ObjectsSurviveWithoutSyncWhenSyncIsDisabled) {
    store_.set_sync_writes(false);
    ASSERT_EQ(store_.put("000000000001.sst", Slice::from(std::string_view("x"))).get(),
              Status::Ok);
    auto value = store_.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->size(), 1u);
}

TEST_F(DiskBlobStoreTest, AuthoritativeStoreOfAPlainStoreIsItself) {
    EXPECT_EQ(&authoritative_store(store_), &store_);
    EXPECT_EQ(store_.as_cache(), nullptr);
}

}  // namespace
}  // namespace elysiumkv::test
