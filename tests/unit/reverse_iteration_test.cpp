#include "db/db_impl.hpp"
#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::vector<std::string> keys_of(Iterator& it) {
    std::vector<std::string> keys;
    while (it.next()) {
        keys.emplace_back(reinterpret_cast<const char*>(it.key().data()), it.key().size());
    }
    return keys;
}

std::vector<std::string> values_of(Iterator& it) {
    std::vector<std::string> values;
    while (it.next()) {
        values.emplace_back(reinterpret_cast<const char*>(it.value().data()), it.value().size());
    }
    return values;
}

std::string key_at(int i) {
    std::string out = "key";
    const std::string digits = std::to_string(i);
    out.append(4 - std::min<size_t>(4, digits.size()), '0');
    out += digits;
    return out;
}

/// Descending iteration, which has to produce *the same set* as the forward scan in the opposite
/// order — not merely a decreasing sequence. Most of the ways this breaks (a skipped entry at a
/// block boundary, a duplicate at a restart point) still yield a decreasing sequence.
class ReverseIteration : public ::testing::Test {
protected:
    void SetUp() override {
        Options options = make_options(store_, Compression::None, 64u << 10);
        options.background = BackgroundMode::Inline;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value());
        db_ = std::move(opened->db);
    }

    void put(const std::string& key, const std::string& value) {
        ASSERT_EQ(db_->put(Slice::from(key), Slice::from(value)), Status::Ok);
    }

    /// The forward scan of the same bounds, reversed. The reference every case below compares
    /// against, so a defect has to break both directions identically to go unnoticed.
    std::vector<std::string> forward_reversed() {
        auto forward = db_->iterator();
        std::vector<std::string> keys = keys_of(*forward);
        std::reverse(keys.begin(), keys.end());
        return keys;
    }

    TestStore store_{1};
    std::unique_ptr<DB> db_;
};

TEST_F(ReverseIteration, TheMemtableAloneDescends) {
    put("b", "2");
    put("a", "1");
    put("c", "3");

    auto it = db_->reverse_iterator();
    EXPECT_EQ(keys_of(*it), (std::vector<std::string>{"c", "b", "a"}));
}

TEST_F(ReverseIteration, AnEmptyStoreYieldsNothing) {
    auto it = db_->reverse_iterator();
    EXPECT_TRUE(keys_of(*it).empty());
    EXPECT_EQ(it->status(), Status::Ok) << "exhausted is not failed";
}

/// The case the block layer exists for: enough entries to span many blocks and many restart points,
/// so `prev()` crosses both. A backward step is a rescan from the preceding restart, and an
/// off-by-one there shows up here and almost nowhere else.
TEST_F(ReverseIteration, ManyBlocksAndRestartPointsDescendExactly) {
    for (int i = 0; i < 4000; ++i) put(key_at(i), std::string(64, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_iterator();
    const std::vector<std::string> observed = keys_of(*it);

    ASSERT_EQ(observed.size(), 4000u) << "every entry, once";
    EXPECT_EQ(observed, forward_reversed());
    EXPECT_EQ(observed.front(), key_at(3999));
    EXPECT_EQ(observed.back(), key_at(0));
}

/// Data in several places at once — memtable over an L0 file over deeper levels — is the whole
/// reason the merge exists. Reverse has its own pick, so it needs its own proof.
TEST_F(ReverseIteration, DataSpreadAcrossMemtableAndFilesDescends) {
    for (int i = 0; i < 200; ++i) put(key_at(i), "old");
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 100; i < 300; ++i) put(key_at(i), "mid");
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 250; i < 400; ++i) put(key_at(i), "new");  // stays in the memtable

    auto it = db_->reverse_iterator();
    const std::vector<std::string> observed = keys_of(*it);

    EXPECT_EQ(observed.size(), 400u) << "one entry per distinct key, not one per version";
    EXPECT_EQ(observed, forward_reversed());
}

/// Recency is positional, and position does not depend on which way the scan runs: the newest
/// version of a key must win descending exactly as it does ascending.
TEST_F(ReverseIteration, TheNewestVersionOfAKeyWinsDescending) {
    put("k", "first");
    ASSERT_EQ(db_->flush(), Status::Ok);
    put("k", "second");
    ASSERT_EQ(db_->flush(), Status::Ok);
    put("k", "third");

    auto it = db_->reverse_iterator();
    EXPECT_EQ(values_of(*it), (std::vector<std::string>{"third"}));
}

TEST_F(ReverseIteration, DeletedKeysStayHiddenDescending) {
    for (int i = 0; i < 50; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->remove(Slice::from(key_at(10))), Status::Ok);
    ASSERT_EQ(db_->remove(Slice::from(key_at(49))), Status::Ok);  // the entry a reverse scan starts on

    auto it = db_->reverse_iterator();
    const std::vector<std::string> observed = keys_of(*it);

    EXPECT_EQ(observed.size(), 48u);
    EXPECT_EQ(observed.front(), key_at(48)) << "a tombstone at the top must not end the scan";
    EXPECT_EQ(std::count(observed.begin(), observed.end(), key_at(10)), 0);
}

/// Bounds describe the same set in both directions. Only delivery order changes, so the
/// lower bound stays inclusive and the upper stays exclusive — the reverse scan simply starts at
/// the other end of that set.
TEST_F(ReverseIteration, BoundsKeepTheirInclusivityDescending) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_iterator(Slice::from(key_at(3)), Slice::from(key_at(7)));
    const std::vector<std::string> observed = keys_of(*it);

    EXPECT_EQ(observed,
              (std::vector<std::string>{key_at(6), key_at(5), key_at(4), key_at(3)}))
            << "starts below the exclusive upper bound and ends on the inclusive lower one";
}

/// The upper bound is exclusive even when a key sits exactly on it — the case a reverse scan meets
/// first, and the one an inclusive seek would get wrong.
TEST_F(ReverseIteration, AKeyExactlyOnTheUpperBoundIsExcluded) {
    put("a", "v");
    put("b", "v");
    put("c", "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_iterator(Slice(), Slice::from(std::string("b")));
    EXPECT_EQ(keys_of(*it), (std::vector<std::string>{"a"}));
}

TEST_F(ReverseIteration, ALowerBoundOnlyScanDescendsToIt) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");

    auto it = db_->reverse_iterator(Slice::from(key_at(7)));
    EXPECT_EQ(keys_of(*it),
              (std::vector<std::string>{key_at(9), key_at(8), key_at(7)}));
}

TEST_F(ReverseIteration, AReversePrefixScanStaysInsideThePrefix) {
    put("aa", "v");
    put("ab", "v");
    put("ba", "v");
    put("bb", "v");
    put("bc", "v");
    put("ca", "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_prefix_iterator(Slice::from(std::string("b")));
    EXPECT_EQ(keys_of(*it), (std::vector<std::string>{"bc", "bb", "ba"}));
}

/// A prefix of all-0xFF has no successor, so the scan runs to the end of the keyspace rather than
/// to a synthesised upper bound. Descending, that means it *starts* there.
TEST_F(ReverseIteration, AReversePrefixOfMaximalBytesStillScans) {
    const std::string high(3, '\xFF');
    put(high, "v");
    put(high + "a", "v");
    put("aaa", "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_prefix_iterator(Slice::from(high));
    EXPECT_EQ(keys_of(*it), (std::vector<std::string>{high + "a", high}));
}

/// Keys that are prefixes of one another order by length, and a backward step has to respect that
/// the shorter one comes first — the block layer reconstructs keys from a shared prefix, so this is
/// where a rebuilt key would be wrong if `prev()` reused the wrong base.
TEST_F(ReverseIteration, KeysThatArePrefixesOfEachOtherDescendInOrder) {
    put("k", "v");
    put("ka", "v");
    put("kab", "v");
    put("kb", "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_iterator();
    EXPECT_EQ(keys_of(*it), (std::vector<std::string>{"kb", "kab", "ka", "k"}));
}

TEST_F(ReverseIteration, ARangeWithNothingInItYieldsNothing) {
    put("a", "v");
    put("z", "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_iterator(Slice::from(std::string("m")), Slice::from(std::string("q")));
    EXPECT_TRUE(keys_of(*it).empty());
    EXPECT_EQ(it->status(), Status::Ok);
}

/// A bound past every key must not walk off the end: the scan starts at the last entry that exists.
TEST_F(ReverseIteration, AnUpperBoundBeyondEveryKeyStartsAtTheLastOne) {
    for (int i = 0; i < 100; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->reverse_iterator(Slice(), Slice::from(std::string("zzzz")));
    const std::vector<std::string> observed = keys_of(*it);

    ASSERT_EQ(observed.size(), 100u);
    EXPECT_EQ(observed.front(), key_at(99));
}

/// Values must belong to the keys they arrive with. Reverse rebuilds keys and values from different
/// places in a block, so a mismatch here would not show up in a key-only check.
TEST_F(ReverseIteration, ValuesTrackTheirKeysDescending) {
    for (int i = 0; i < 500; ++i) put(key_at(i), "value-" + key_at(i));
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 0; i < 500; i += 3) put(key_at(i), "fresh-" + key_at(i));

    auto it = db_->reverse_iterator();
    int seen = 0;
    while (it->next()) {
        const std::string key(reinterpret_cast<const char*>(it->key().data()), it->key().size());
        const std::string value(reinterpret_cast<const char*>(it->value().data()),
                                it->value().size());
        ASSERT_EQ(value.substr(value.find('-') + 1), key) << "value belongs to another key";
        ++seen;
    }
    EXPECT_EQ(seen, 500);
}

}  // namespace
}  // namespace elysiumkv::test
