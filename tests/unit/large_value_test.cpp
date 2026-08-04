#include "db/db_impl.hpp"
#include "sst/format.hpp"
#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <string>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Inside an SST" — a value up to `kMaxValueBytes` round-trips; anything larger is refused
/// at put().
///
/// The bug these pin down: `SstReader::max_uncompressed()` refuses a block
/// claiming more than its bound, and nothing applied the matching limit on the
/// way in. A 1 MiB value under a 1 KiB block size was accepted by put(), survived
/// flush(), and then read back as Status::Corrupt — a write that reported success
/// and was gone, reported as corruption rather than as a limit, which sends an
/// operator looking for a damaged disk.
///
/// The limit is now a constant the writer and the reader share, so they cannot
/// drift: the reader's floor is derived from the writer's ceiling.
class LargeValue : public ::testing::Test {
protected:
    std::unique_ptr<DB> open(size_t block_bytes) {
        Options options = make_options(store_, Compression::None, /*memtable_bytes=*/64u << 10);
        options.block_bytes = block_bytes;
        options.background = BackgroundMode::Inline;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        EXPECT_TRUE(opened.has_value());
        return opened.has_value() ? std::move(opened->db) : nullptr;
    }
    TestStore store_{1};
};

TEST_F(LargeValue, AMaximalValueRoundTripsThroughAnSst) {
    auto db = open(/*block_bytes=*/1024);
    ASSERT_NE(db, nullptr);

    const std::string key = "k";
    const std::string value(kMaxValueBytes, 'v');
    ASSERT_EQ(db->put(Slice::from(key), Slice::from(value)), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok) << "it has to survive becoming a block";

    auto found = db->get_copy(Slice::from(key));
    ASSERT_TRUE(found.has_value()) << "the largest permitted value must be readable";
    EXPECT_EQ(found->size(), kMaxValueBytes);
}

TEST_F(LargeValue, AnOversizedValueIsRefusedRatherThanLostLater) {
    auto db = open(/*block_bytes=*/1024);
    ASSERT_NE(db, nullptr);

    const std::string key = "k";
    const std::string too_big(kMaxValueBytes + 1, 'v');
    EXPECT_EQ(db->put(Slice::from(key), Slice::from(too_big)), Status::Config)
        << "a limit the writer does not enforce is a trap, not a limit";

    // And nothing was written: the refusal is total, not partial.
    EXPECT_EQ(db->flush(), Status::Ok);
    EXPECT_FALSE(db->get_copy(Slice::from(key)).has_value());
}

TEST_F(LargeValue, AnOversizedKeyIsRefused) {
    auto db = open(/*block_bytes=*/1024);
    ASSERT_NE(db, nullptr);
    const std::string key(kMaxKeyBytes + 1, 'k');
    EXPECT_EQ(db->put(Slice::from(key), Slice::from(std::string("v"))), Status::Config);
    EXPECT_EQ(db->remove(Slice::from(key)), Status::Config);
}

/// A batch is applied as a unit (ARCHITECTURE.md "Absence is an answer, not an error"), so one oversized entry must reject the
/// whole thing rather than leave half of it in the store.
TEST_F(LargeValue, AnOversizedEntryRejectsTheWholeBatch) {
    auto db = open(/*block_bytes=*/1024);
    ASSERT_NE(db, nullptr);

    WriteBatch batch;
    batch.put(Slice::from(std::string("fine")), Slice::from(std::string("v")));
    batch.put(Slice::from(std::string("huge")),
              Slice::from(std::string(kMaxValueBytes + 1, 'v')));
    batch.put(Slice::from(std::string("also-fine")), Slice::from(std::string("v")));

    EXPECT_EQ(db->write(batch), Status::Config);
    EXPECT_FALSE(db->get_copy(Slice::from(std::string("fine"))).has_value())
        << "the entry before the oversized one must not have landed";
    EXPECT_FALSE(db->get_copy(Slice::from(std::string("also-fine"))).has_value());
}

/// A large block size raises the reader's bound but must not lower it: the
/// maximal entry stays readable whatever block_bytes is set to.
TEST_F(LargeValue, TheLimitDoesNotDependOnBlockSize) {
    for (size_t block_bytes : {size_t{512}, size_t{4096}, size_t{1u << 20}}) {
        TestStore store(1);
        Options options = make_options(store, Compression::Zstd, 64u << 10);
        options.block_bytes = block_bytes;
        options.background = BackgroundMode::Inline;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value());
        auto db = std::move(opened->db);

        const std::string value(kMaxValueBytes, 'z');
        ASSERT_EQ(db->put(Slice::from(std::string("k")), Slice::from(value)), Status::Ok)
            << block_bytes;
        ASSERT_EQ(db->flush(), Status::Ok) << block_bytes;
        auto found = db->get_copy(Slice::from(std::string("k")));
        EXPECT_TRUE(found.has_value()) << "block_bytes=" << block_bytes;
    }
}

}  // namespace
}  // namespace elysiumkv::test
