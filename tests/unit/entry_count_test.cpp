/* The entry count, and what it does and does not claim.
 *
 * The contract:
 *
 *   entry_count = records - tombstones
 *               = an UPPER BOUND on the number of distinct live keys.
 *
 * Provable rather than typical. Every distinct live key's *newest* record is a put, never a
 * tombstone, so tombstones are disjoint from the records representing live keys:
 *
 *     records >= live + tombstones      hence    records - tombstones >= live
 *
 * The slack is superseded versions not yet merged, and it closes under compaction.
 *
 * A tombstone shadowing nothing does not push the answer below the truth, though the argument that
 * it would reads as plausible: the tombstone counts *itself* among the records, so removing it can
 * only cancel its own contribution. `TheSubtractionIsAlsoAnUpperBoundAndATighterOne` pins that.
 *
 * The counts themselves are exact. `num_entries` and `num_tombstones` are written by the SST
 * builder as it appends, so nothing here is sampled. The approximation is entirely in the cross-file
 * dimension.
 */

#include "db/db_impl.hpp"

#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

class EntryCountTest : public ::testing::Test {
protected:
    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    void open_db() {
        // One durable tier, three levels, and a memtable large enough that nothing flushes unless
        // the test asks — otherwise "before compaction" is not a state the test controls.
        Options options = make_options(store_, Compression::None, 4u << 20);
        options.background = BackgroundMode::Inline;
        // Inline mode compacts after every flush, so L0's default capacity of four files would
        // merge the rounds as they were written and there would be no un-merged state to observe.
        // Raising it is what makes "before compaction" reachable.
        options.levels.at(0).max_files = 1000;
        options.levels.at(0).slowdown_at = 2000;
        options.levels.at(0).stop_at = 4000;
        auto opened = DB::open(options);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(*opened);
    }

    void put(int i, const std::string& value = "v") {
        ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(value)), Status::Ok);
    }
    void remove(int i) { ASSERT_EQ(db_->remove(Slice::from(key_at(i))), Status::Ok); }

    Stats stats() { return db_->stats(); }

    /// The way a binding computes it: records less tombstones, across the levels and the memtable.
    uint64_t entry_count() {
        const Stats s = stats();
        uint64_t total = s.memtable_entries - s.memtable_tombstones;
        for (const LevelStats& level : s.levels) total += level.entries - level.tombstones;
        return total;
    }

    /// The raw record count, before the subtraction — the looser of the two upper bounds.
    uint64_t record_count() {
        const Stats s = stats();
        uint64_t total = s.memtable_entries;
        for (const LevelStats& level : s.levels) total += level.entries;
        return total;
    }

    uint64_t tombstone_count() {
        const Stats s = stats();
        uint64_t total = s.memtable_tombstones;
        for (const LevelStats& level : s.levels) total += level.tombstones;
        return total;
    }

    /// Compacts every level in turn so records reach the bottommost one, where duplicates are merged
    /// and tombstones are dropped.
    void compact_everything() {
        for (int level = 0; level <= static_cast<DbImpl&>(*db_).levels().last(); ++level) {
            ASSERT_EQ(db_->compact_level(level), Status::Ok);
        }
    }

    TestStore store_;
    std::unique_ptr<DB> db_;
};

TEST_F(EntryCountTest, DistinctPutsAreCountedOnceEachBeforeAndAfterAFlush) {
    open_db();
    for (int i = 0; i < 200; ++i) put(i);

    EXPECT_EQ(entry_count(), 200u);
    EXPECT_EQ(stats().memtable_entries, 200u) << "nothing has been flushed yet";

    ASSERT_EQ(db_->flush(), Status::Ok);
    EXPECT_EQ(entry_count(), 200u) << "a flush moves records, it does not create or destroy them";
    EXPECT_EQ(stats().memtable_entries, 0u);
    EXPECT_EQ(stats().levels[0].entries, 200u);
}

// The case that makes "upper bound" mean something. Without it, an implementation that somehow
// reported distinct keys would pass everything else and the framing would be untested prose.
TEST_F(EntryCountTest, SupersededVersionsAreCountedUntilCompactionMergesThem) {
    open_db();
    constexpr int kKeys = 100;
    constexpr int kRounds = 5;

    for (int round = 0; round < kRounds; ++round) {
        for (int i = 0; i < kKeys; ++i) put(i, "value-" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);   // a separate file per round, so nothing merges yet
    }

    EXPECT_GT(entry_count(), static_cast<uint64_t>(kKeys))
        << "every superseded version is still a record — this is the slack the bound allows for";
    EXPECT_EQ(entry_count(), static_cast<uint64_t>(kKeys * kRounds));

    compact_everything();
    EXPECT_EQ(entry_count(), static_cast<uint64_t>(kKeys))
        << "the bound is tight once everything has merged, not merely safe";

    // And the values are the newest ones, which is what makes the merged count the right one.
    auto found = db_->get_copy(Slice::from(key_at(0)));
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->begin(), found->end()), "value-4");
}

TEST_F(EntryCountTest, ATombstoneIsARecordUntilItIsDropped) {
    open_db();
    constexpr int kKeys = 100;

    for (int i = 0; i < kKeys; ++i) put(i);
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 0; i < kKeys; ++i) remove(i);
    ASSERT_EQ(db_->flush(), Status::Ok);

    EXPECT_EQ(record_count(), static_cast<uint64_t>(2 * kKeys))
        << "the puts and the deletes are both records";
    EXPECT_EQ(tombstone_count(), static_cast<uint64_t>(kKeys));
    EXPECT_EQ(entry_count(), static_cast<uint64_t>(kKeys))
        << "the subtraction removes the tombstones' own contribution, leaving the shadowed puts — "
           "still an over-count, and closer than the raw record count";

    // Tombstones are dropped only when the output is bottommost for its key range.
    compact_everything();
    EXPECT_EQ(entry_count(), 0u) << "nothing is live, and nothing is left to say so";
    EXPECT_EQ(tombstone_count(), 0u);
}

/* The claim that nearly went in backwards.
 *
 * `entries - tombstones` was very nearly rejected on the grounds that it is "neither an upper nor a
 * lower bound", with the argument that a tombstone shadowing nothing subtracts a key that was never
 * counted. The argument is wrong and the arithmetic says so: the tombstone is itself one of the
 * records, so subtracting it cancels its own contribution and nothing else.
 *
 * This is the construction that argument was built on — deletes for keys that never existed — and it
 * lands exactly *on* the bound rather than below it.
 */
TEST_F(EntryCountTest, TheSubtractionIsAlsoAnUpperBoundAndATighterOne) {
    open_db();
    constexpr int kLive = 50;

    for (int i = 0; i < kLive; ++i) put(i);
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 1000; i < 1000 + 120; ++i) remove(i);   // nothing to shadow
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_GT(tombstone_count(), 0u) << "the construction needs tombstones to be interesting";

    EXPECT_GE(record_count(), static_cast<uint64_t>(kLive)) << "the loose bound holds";
    EXPECT_GE(entry_count(), static_cast<uint64_t>(kLive))
        << "and so does the tight one — a tombstone can never subtract below the live-key count, "
           "because it counted itself on the way in";
    EXPECT_LT(entry_count(), record_count())
        << "tighter, which is the reason to prefer it";
    EXPECT_EQ(entry_count(), static_cast<uint64_t>(kLive)) << "exactly on the bound here";
}

// The per-level split is the reason there is no single global in `Stats`: how much slack the bound
// carries depends on where the records are. This asserts the split is real rather than a copy of the
// same total.
TEST_F(EntryCountTest, RecordsAreReportedPerLevelSoTheSlackIsVisible) {
    open_db();
    for (int i = 0; i < 200; ++i) put(i);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    for (int i = 0; i < 200; ++i) put(i, "second");
    ASSERT_EQ(db_->flush(), Status::Ok);

    const Stats s = stats();
    ASSERT_GE(s.levels.size(), 2u);
    EXPECT_GT(s.levels[0].entries, 0u) << "the newer copies are at L0";
    EXPECT_GT(s.levels[1].entries, 0u) << "the older ones were compacted down";
    EXPECT_EQ(s.levels[0].entries + s.levels[1].entries + s.levels[2].entries, 400u);
}

TEST_F(EntryCountTest, AnEmptyStoreCountsNothing) {
    open_db();
    EXPECT_EQ(entry_count(), 0u);
    for (const LevelStats& level : stats().levels) {
        EXPECT_EQ(level.entries, 0u);
        EXPECT_EQ(level.tombstones, 0u);
    }
}

}  // namespace
}  // namespace elysiumkv::test
