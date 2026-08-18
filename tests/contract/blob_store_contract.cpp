#include "contract/blob_store_contract.hpp"

#include <cstdio>
#include <string>

namespace elysiumkv::test {
namespace {

TEST_P(BlobStoreContract, PutGetRoundTrip) {
    ASSERT_EQ(put("000000000001.sst", "hello world"), Status::Ok);
    auto value = get("000000000001.sst");
    ASSERT_TRUE(value.has_value()) << status_name(value.error());
    EXPECT_EQ(as_string(*value), "hello world");
}

TEST_P(BlobStoreContract, EmptyObjectRoundTrips) {
    ASSERT_EQ(put("000000000001.sst", ""), Status::Ok);
    auto value = get("000000000001.sst");
    ASSERT_TRUE(value.has_value());
    EXPECT_TRUE(value->empty());
}

TEST_P(BlobStoreContract, RangedGetReturnsExactlyTheRange) {
    ASSERT_EQ(put("000000000001.sst", "abcdefghij"), Status::Ok);

    auto middle = get("000000000001.sst", 3, 4);
    ASSERT_TRUE(middle.has_value());
    EXPECT_EQ(as_string(*middle), "defg");

    // A read overlapping the end is truncated to what exists.
    auto tail = get("000000000001.sst", 8, 100);
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(as_string(*tail), "ij");

    // At or past the end: an empty buffer, not an error.
    auto past = get("000000000001.sst", 10, 5);
    ASSERT_TRUE(past.has_value());
    EXPECT_TRUE(past->empty());
}

TEST_P(BlobStoreContract, LargeObjectRoundTripsAndRangeReadsIntoIt) {
    std::string bytes(4u << 20, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<char>(i & 0xFF);
    ASSERT_EQ(put("000000000001.sst", bytes), Status::Ok);

    auto whole = get("000000000001.sst");
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(whole->size(), bytes.size());
    EXPECT_EQ(as_string(*whole), bytes);

    auto window = get("000000000001.sst", 1u << 20, 64);
    ASSERT_TRUE(window.has_value());
    EXPECT_EQ(as_string(*window), bytes.substr(1u << 20, 64));
}

// ARCHITECTURE.md "Immutable named objects" — NotFound is positive evidence of absence. Nothing else may stand in for it.
TEST_P(BlobStoreContract, MissingObjectIsNotFound) {
    auto value = get("000000000042.sst");
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), Status::NotFound);
}

// Objects are write-once. A collision means a zombie writer reusing file
// numbers, so it is terminal — and the existing bytes are left alone.
TEST_P(BlobStoreContract, PutAtAnExistingNameNeverOverwrites) {
    ASSERT_EQ(put("000000000001.sst", "original"), Status::Ok);
    EXPECT_EQ(put("000000000001.sst", "replacement"), Status::Unusable);

    auto value = get("000000000001.sst");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(as_string(*value), "original");
}

TEST_P(BlobStoreContract, RemoveIsIdempotentAndMakesTheObjectAbsent) {
    ASSERT_EQ(put("000000000001.sst", "x"), Status::Ok);

    // **Read before removing.** A store that holds an open descriptor keeps reading an unlinked
    // inode perfectly well, so a removed object stays readable unless the remove drops it — and
    // the removal is only observable through a store that had already been asked for the object.
    ASSERT_TRUE(get("000000000001.sst").has_value());

    EXPECT_EQ(remove("000000000001.sst"), Status::Ok);

    auto value = get("000000000001.sst");
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), Status::NotFound);

    EXPECT_EQ(remove("000000000001.sst"), Status::Ok);
    EXPECT_EQ(remove("000000000099.sst"), Status::Ok);
}

// ARCHITECTURE.md "Immutable named objects" — an empty prefix in an existing store is a successful, meaningful empty
// result — never conflated with a failure to look.
TEST_P(BlobStoreContract, ListOfAnEmptyStoreSucceedsAndIsEmpty) {
    auto names = list();
    ASSERT_TRUE(names.has_value()) << status_name(names.error());
    EXPECT_TRUE(names->empty());
}

TEST_P(BlobStoreContract, ListFiltersByPrefixAndIsSorted) {
    ASSERT_EQ(put("000000000002.sst", "b"), Status::Ok);
    ASSERT_EQ(put("000000000001.sst", "a"), Status::Ok);
    ASSERT_EQ(put("other-object", "c"), Status::Ok);

    auto all = list();
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(*all, (std::vector<std::string>{"000000000001.sst", "000000000002.sst",
                                              "other-object"}));

    auto ssts = list("0000");
    ASSERT_TRUE(ssts.has_value());
    EXPECT_EQ(*ssts, (std::vector<std::string>{"000000000001.sst", "000000000002.sst"}));

    auto none = list("zzz");
    ASSERT_TRUE(none.has_value());
    EXPECT_TRUE(none->empty());
}

// ARCHITECTURE.md "Contract suites" — S3 pages at 1000 keys and a mature store has thousands of SSTs. The
// engine must never see a page boundary.
TEST_P(BlobStoreContract, ListAcrossAPaginationBoundary) {
    constexpr int kObjects = 1200;
    for (int i = 0; i < kObjects; ++i) {
        char name[32];
        std::snprintf(name, sizeof(name), "%012d.sst", i);
        ASSERT_EQ(put(name, "x"), Status::Ok) << name;
    }
    auto names = list();
    ASSERT_TRUE(names.has_value()) << status_name(names.error());
    ASSERT_EQ(names->size(), static_cast<size_t>(kObjects));
    EXPECT_EQ(names->front(), "000000000000.sst");
    EXPECT_EQ(names->back(), "000000001199.sst");
}

TEST_P(BlobStoreContract, MalformedNamesAreAConfigurationError) {
    EXPECT_EQ(put("", "x"), Status::Config);
    EXPECT_EQ(put("nested/name.sst", "x"), Status::Config);
    EXPECT_EQ(put(".hidden", "x"), Status::Config);

    auto value = get("nested/name.sst");
    ASSERT_FALSE(value.has_value());
    EXPECT_EQ(value.error(), Status::Config);
}

// ARCHITECTURE.md "Immutable named objects" — a cache layer returns its delegate's id — caches are not locations — so
// the bulk view of any store answers for the same location.
TEST_P(BlobStoreContract, BulkViewNamesTheSameLocation) {
    EXPECT_EQ(store_->bulk_view().id(), store_->id());

    ASSERT_EQ(put("000000000001.sst", "hello"), Status::Ok);
    auto value = store_->bulk_view().get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(as_string(*value), "hello");
}

// The counters are what an operator is billed on against object storage, so every
// implementation owes them — a store that silently reports zero traffic is worse than
// one that reports none at all.
TEST_P(BlobStoreContract, EveryRequestIsCounted) {
    const IoCounters before = store_->counters();

    ASSERT_EQ(put("000000000001.sst", "abcdefghij"), Status::Ok);
    ASSERT_TRUE(get("000000000001.sst", 2, 3).has_value());
    ASSERT_TRUE(list().has_value());
    ASSERT_EQ(remove("000000000001.sst"), Status::Ok);

    const IoCounters after = store_->counters();
    EXPECT_EQ(after.puts, before.puts + 1);
    EXPECT_EQ(after.gets, before.gets + 1);
    EXPECT_EQ(after.lists, before.lists + 1);
    EXPECT_EQ(after.removes, before.removes + 1);
    EXPECT_EQ(after.bytes_written, before.bytes_written + 10);
    // At least the three asked for: a cache reads a whole chunk to answer a window.
    EXPECT_GE(after.bytes_read, before.bytes_read + 3);
    EXPECT_EQ(after.errors, before.errors);
}

// A miss is an answer, and a store that serves nothing but misses is working. Counting
// it as an error would make an empty prefix look like a broken tier.
TEST_P(BlobStoreContract, NotFoundIsARequestAndNotAnError) {
    const IoCounters before = store_->counters();

    ASSERT_EQ(get("000000000042.sst").error(), Status::NotFound);

    const IoCounters after = store_->counters();
    EXPECT_EQ(after.gets, before.gets + 1);
    EXPECT_EQ(after.errors, before.errors);
}

// A collision is a real failure, and it is counted as one — the tier's error rate is the
// signal that says a store is misbehaving rather than merely empty.
TEST_P(BlobStoreContract, AFailedPutIsCountedAsAnError) {
    ASSERT_EQ(put("000000000001.sst", "original"), Status::Ok);
    const IoCounters before = store_->counters();

    ASSERT_EQ(put("000000000001.sst", "replacement"), Status::Unusable);

    const IoCounters after = store_->counters();
    EXPECT_EQ(after.puts, before.puts + 1);
    EXPECT_EQ(after.errors, before.errors + 1);
    EXPECT_EQ(after.bytes_written, before.bytes_written)
        << "a rejected put moved nothing";
}

TEST_P(BlobStoreContract, IdIsNonEmpty) { EXPECT_FALSE(store_->id().empty()); }

}  // namespace
}  // namespace elysiumkv::test
