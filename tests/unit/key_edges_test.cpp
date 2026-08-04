#include "db/db_impl.hpp"
#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::vector<std::string> scan(Iterator& it) {
    std::vector<std::string> keys;
    while (it.next()) keys.push_back(std::string(reinterpret_cast<const char*>(it.key().data()),
                                                 it.key().size()));
    return keys;
}

class KeyEdges : public ::testing::Test {
protected:
    void SetUp() override {
        Options options = make_options(store_, Compression::None, 64u << 10);
        options.background = BackgroundMode::Inline;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value());
        db_ = std::move(opened->db);
    }
    TestStore store_{1};
    std::unique_ptr<DB> db_;
};

/// A prefix of all-0xFF has no successor, so a prefix scan cannot be expressed
/// as [prefix, prefix+1) — the upper bound would wrap to the empty string and
/// select nothing.
TEST_F(KeyEdges, APrefixOfMaximalBytesStillScans) {
    const std::string high(3, '\xFF');
    ASSERT_EQ(db_->put(Slice::from(high), Slice::from(std::string("v"))), Status::Ok);
    ASSERT_EQ(db_->put(Slice::from(high + "a"), Slice::from(std::string("v"))), Status::Ok);
    ASSERT_EQ(db_->put(Slice::from(std::string("aaa")), Slice::from(std::string("v"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->prefix_iterator(Slice::from(high));
    EXPECT_EQ(scan(*it).size(), 2u) << "the 0xFF prefix and everything under it";
}

/// The empty key is a key. It sorts first and must not be confused with the
/// "unbounded" sentinel the read path uses for an absent upper bound.
TEST_F(KeyEdges, TheEmptyKeyRoundTrips) {
    ASSERT_EQ(db_->put(Slice(), Slice::from(std::string("empty-key"))), Status::Ok);
    ASSERT_EQ(db_->put(Slice::from(std::string("a")), Slice::from(std::string("a"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto found = db_->get_copy(Slice());
    ASSERT_TRUE(found.has_value()) << "the empty key was written and must read back";
    EXPECT_EQ(std::string(found->begin(), found->end()), "empty-key");

    auto it = db_->iterator();
    const std::vector<std::string> keys = scan(*it);
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "") << "the empty key sorts first";
}

/// The one-sided form added for the C ABI: [lower, end of keyspace).
TEST_F(KeyEdges, AOneSidedRangeReachesTheEnd) {
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(db_->put(Slice::from("k" + std::to_string(i)),
                           Slice::from(std::string("v"))),
                  Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto from_five = db_->iterator(Slice::from(std::string("k5")));
    EXPECT_EQ(scan(*from_five).size(), 5u);

    // And the two-argument form still means a closed-open range, including the
    // degenerate one: an empty upper bound is the empty key, not "no bound".
    auto degenerate = db_->iterator(Slice::from(std::string("k5")), Slice());
    EXPECT_EQ(scan(*degenerate).size(), 0u) << "a range ending at the empty key is empty";
}

}  // namespace
}  // namespace elysiumkv::test
