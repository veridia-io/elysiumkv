#include "contract/blob_store_contract.hpp"
#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <gtest/gtest.h>

#include <memory>

namespace elysiumkv::test {
namespace {

std::shared_ptr<BlobStore> make_fault_store() {
    auto dir = std::make_shared<TempDir>();
    auto local = std::make_shared<DiskBlobStore>(dir->path());
    auto* store = new FaultInjectingBlobStore(local);
    return std::shared_ptr<BlobStore>(store, [dir, local](BlobStore* p) { delete p; });
}

// With no rules configured the decorator must be invisible: it runs the whole
// contract suite unchanged.
INSTANTIATE_TEST_SUITE_P(FaultInjecting, BlobStoreContract,
                         ::testing::Values(BlobStoreFactory{"FaultInjectingBlobStore",
                                                            make_fault_store}),
                         BlobStoreFactoryName());

class FaultInjectionTest : public ::testing::Test {
protected:
    std::string_view kName = "000000000001.sst";

    Status put(std::string_view name, std::string_view bytes) {
        return store_.put(name, Slice::from(bytes)).get();
    }

    TempDir dir_;
    std::shared_ptr<DiskBlobStore> local_ = std::make_shared<DiskBlobStore>(dir_.path());
    FaultInjectingBlobStore store_{local_};
};

TEST_F(FaultInjectionTest, FailsTheNthMatchingCallAndThenRecovers) {
    ASSERT_EQ(put(kName, "value"), Status::Ok);
    store_.add_rule({.op = FaultInjectingBlobStore::Op::Get,
                     .name_contains = std::string(kName),
                     .first_match = 1,
                     .match_count = 2,
                     .status = Status::Io});

    EXPECT_TRUE(store_.get(kName, 0, BlobStore::kReadToEnd).get().has_value());
    EXPECT_EQ(store_.get(kName, 0, BlobStore::kReadToEnd).get().error(), Status::Io);
    EXPECT_EQ(store_.get(kName, 0, BlobStore::kReadToEnd).get().error(), Status::Io);
    EXPECT_TRUE(store_.get(kName, 0, BlobStore::kReadToEnd).get().has_value());
    EXPECT_EQ(store_.call_count(FaultInjectingBlobStore::Op::Get), 4u);
}

TEST_F(FaultInjectionTest, RulesMatchByName) {
    store_.add_rule({.op = FaultInjectingBlobStore::Op::Put,
                     .name_contains = "0002",
                     .match_count = 0,
                     .status = Status::Io});
    EXPECT_EQ(put("000000000001.sst", "a"), Status::Ok);
    EXPECT_EQ(put("000000000002.sst", "b"), Status::Io);
    EXPECT_EQ(put("000000000003.sst", "c"), Status::Ok);
}

// A torn write leaves a prefix of the object behind. The put reports failure, so
// the engine allocates a new file number and the fragment becomes an orphan.
TEST_F(FaultInjectionTest, TornWriteLeavesAPrefixAndReportsFailure) {
    store_.add_rule({.op = FaultInjectingBlobStore::Op::Put,
                     .status = Status::Io,
                     .torn_write = true,
                     .torn_bytes = 4});
    EXPECT_EQ(put(kName, "abcdefghij"), Status::Io);

    auto value = store_.get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->size(), 4u);
}

// ARCHITECTURE.md "A tier is not a level" — whole-store loss presents as a *successful* list that omits objects the
// version references. That is the only evidence a discard may act on.
TEST_F(FaultInjectionTest, VanishedObjectsListSuccessfullyAsAbsent) {
    ASSERT_EQ(put("000000000001.sst", "a"), Status::Ok);
    ASSERT_EQ(put("000000000002.sst", "b"), Status::Ok);
    ASSERT_EQ(store_.vanish_all(), Status::Ok);

    auto names = store_.list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_TRUE(names->empty());

    auto value = store_.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), Status::NotFound);
}

// The opposite case, and the dangerous one: the store cannot answer. Every
// operation is Io, which is never evidence of loss.
TEST_F(FaultInjectionTest, UnreachableStoreReportsIoForEverything) {
    ASSERT_EQ(put("000000000001.sst", "a"), Status::Ok);
    store_.set_unreachable(true);

    EXPECT_EQ(store_.list("").get().error(), Status::Io);
    EXPECT_EQ(store_.get("000000000001.sst", 0, BlobStore::kReadToEnd).get().error(), Status::Io);
    EXPECT_EQ(put("000000000002.sst", "b"), Status::Io);
    EXPECT_EQ(store_.remove("000000000001.sst").get(), Status::Io);

    store_.set_unreachable(false);
    auto names = store_.list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_EQ(names->size(), 1u) << "an unreachable store must not have lost anything";
}

TEST_F(FaultInjectionTest, VanishRemovesOneObjectOnly) {
    ASSERT_EQ(put("000000000001.sst", "a"), Status::Ok);
    ASSERT_EQ(put("000000000002.sst", "b"), Status::Ok);
    ASSERT_EQ(store_.vanish("000000000001.sst"), Status::Ok);

    auto names = store_.list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_EQ(*names, (std::vector<std::string>{"000000000002.sst"}));
}

}  // namespace
}  // namespace elysiumkv::test
