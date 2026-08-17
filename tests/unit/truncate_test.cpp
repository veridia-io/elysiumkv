#include "db/db_impl.hpp"
#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
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

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%04d", i);
    return buf;
}

int total_files(const Stats& stats) {
    int files = 0;
    for (const LevelStats& level : stats.levels) files += level.file_count;
    return files;
}

/// `truncate_below` — dropping a prefix of the keyspace by moving one key in the manifest rather
/// than writing a tombstone per key.
class Truncate : public ::testing::Test {
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

    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    TestStore store_{1};
    std::unique_ptr<DB> db_;
};

TEST_F(Truncate, KeysBelowThePointBecomeAbsent) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(key_at(9))).error(), Status::NotFound)
            << "absence, not an error — a truncated key reads exactly like a deleted one";
    ASSERT_TRUE(db_->get(Slice::from(key_at(10))).has_value());

    auto it = db_->iterator();
    const std::vector<std::string> seen = keys_of(*it);
    ASSERT_EQ(seen.size(), 10u);
    EXPECT_EQ(seen.front(), key_at(10));
}

/// The unflushed memtable is not described by any Version, so it has to be truncated eagerly or a
/// key below the point would stay readable until its flush.
TEST_F(Truncate, KeysStillInTheMemtableGoToo) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");   // no flush: all in memory

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(key_at(3))).error(), Status::NotFound);
    auto it = db_->iterator();
    EXPECT_EQ(keys_of(*it).size(), 10u);
}

TEST_F(Truncate, ADescendingScanStopsAtThePoint) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    auto it = db_->reverse_iterator();
    const std::vector<std::string> seen = keys_of(*it);
    ASSERT_EQ(seen.size(), 10u);
    EXPECT_EQ(seen.front(), key_at(19));
    EXPECT_EQ(seen.back(), key_at(10)) << "the floor is where a descending scan ends";
}

TEST_F(Truncate, ThePointOnlyMovesForward) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);
    // Lower, and equal, are both no-ops rather than errors: the caller may not know where the
    // point already is, which is what makes this safe to drive from a loop.
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(5))), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(key_at(7))).error(), Status::NotFound)
            << "a lower call must not resurrect anything";
    auto it = db_->iterator();
    EXPECT_EQ(keys_of(*it).size(), 10u);
}

/// <b>The floor is permanent, and a write below it is refused rather than hidden.</b> The engine
/// cannot tell a key written before the truncation from one written after — positional recency is
/// the only ordering it has — so accepting the write would mean a `put` that returned `Ok` and then
/// could not be read back.
TEST_F(Truncate, AWriteBelowTheFloorIsRefused) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    EXPECT_EQ(db_->put(Slice::from(key_at(3)), Slice::from(std::string("v"))), Status::Config);
    EXPECT_EQ(db_->remove(Slice::from(key_at(3))), Status::Config);
    EXPECT_EQ(db_->get(Slice::from(key_at(3))).error(), Status::NotFound);

    // At or above the floor is ordinary.
    EXPECT_EQ(db_->put(Slice::from(key_at(10)), Slice::from(std::string("fresh"))), Status::Ok);
}

/// **`truncate_below` races `put`, and two application threads through one handle are supported.**
/// The floor used to be read on the way into `put`, before `check_entry_size` and before
/// `throttle_writes` — which *blocks* — so the window between deciding a write was legal and
/// applying it was not small at all. A write could pass the check, wait out a stall, and land
/// under a floor published long before it, returning `Ok` for a key that could never be read back.
///
/// The distinguishing case only exists inside the lock, so what this pins from outside is the
/// property that survives it: every outcome is `Ok` or `Config`, nothing below the final floor is
/// readable, and the invariants hold. Its real value is under TSan, which is where the publication
/// and its rollback are actually examined.
TEST_F(Truncate, ConcurrentWritesAndTruncationsStayConsistent) {
    for (int i = 0; i < 400; ++i) put(key_at(i), "seed");

    std::atomic<bool> stop{false};
    std::atomic<int> refused{0};
    std::atomic<int> accepted{0};

    std::thread writer([&] {
        for (int round = 0; !stop.load() && round < 4000; ++round) {
            const Status status =
                db_->put(Slice::from(key_at(round % 400)), Slice::from(std::string("w")));
            if (status == Status::Ok) {
                accepted.fetch_add(1);
            } else if (status == Status::Config) {
                refused.fetch_add(1);
            } else {
                ADD_FAILURE() << "a racing put must be Ok or Config, got " << status_name(status);
                return;
            }
        }
    });

    for (int floor = 1; floor <= 200; floor += 1) {
        ASSERT_EQ(db_->truncate_below(Slice::from(key_at(floor))), Status::Ok);
    }
    stop.store(true);
    writer.join();

    EXPECT_GT(accepted.load(), 0) << "the writer never got through, so this proved nothing";
    EXPECT_GT(refused.load(), 0) << "the floor never overtook the writer, so this proved nothing";

    // Nothing below the final floor survives, whoever won any individual race.
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound)
            << "key " << i << " is below the floor and still readable";
    }
    EXPECT_EQ(static_cast<DbImpl*>(db_.get())->check_invariants(), Status::Ok);
}

/// A batch lands whole or not at all, so one key under the floor refuses all of it.
TEST_F(Truncate, ABatchWithOneKeyBelowTheFloorLandsNotAtAll) {
    put(key_at(50), "v");
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    WriteBatch batch;
    batch.put(Slice::from(key_at(20)), Slice::from(std::string("a")));
    batch.put(Slice::from(key_at(3)), Slice::from(std::string("b")));   // below the floor
    EXPECT_EQ(db_->write(batch), Status::Config);

    EXPECT_EQ(db_->get(Slice::from(key_at(20))).error(), Status::NotFound)
            << "the legal half of the batch must not have landed either";
}

/// The reclaim that makes this cheap: a file every key of which is below the point leaves the
/// version by manifest edit, with nothing read and nothing rewritten.
TEST_F(Truncate, FilesEntirelyBelowThePointAreUnlinkedWithoutARewrite) {
    for (int i = 0; i < 100; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 100; i < 200; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    const int before = total_files(db_->stats());
    ASSERT_GE(before, 2);

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(100))), Status::Ok);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    EXPECT_LT(total_files(db_->stats()), before) << "the first file held nothing readable";
    auto it = db_->iterator();
    EXPECT_EQ(keys_of(*it).size(), 100u);
}

/// A file straddling the point keeps its live half, and compaction is what narrows it.
TEST_F(Truncate, AStraddlingFileIsNarrowedByCompaction) {
    for (int i = 0; i < 100; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(50))), Status::Ok);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    auto it = db_->iterator();
    const std::vector<std::string> seen = keys_of(*it);
    EXPECT_EQ(seen.size(), 50u);
    EXPECT_EQ(seen.front(), key_at(50));
    EXPECT_GT(total_files(db_->stats()), 0) << "the live half is still there";
}

TEST_F(Truncate, ThePointSurvivesAReopen) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);
    db_.reset();

    Options options = make_options(store_, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    auto reopened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(reopened.has_value());
    db_ = std::move(reopened->db);

    EXPECT_EQ(db_->get(Slice::from(key_at(3))).error(), Status::NotFound);
    auto it = db_->iterator();
    EXPECT_EQ(keys_of(*it).size(), 10u);
    EXPECT_EQ(db_->put(Slice::from(key_at(3)), Slice::from(std::string("v"))), Status::Config)
            << "the floor is durable, so it still refuses writes after a reopen";
}

/// Likewise the compaction filter: asserted on bytes, since visibility is the clamp's job.
TEST_F(Truncate, CompactionReclaimsTheSpaceBelowThePoint) {
    for (int i = 0; i < 400; ++i) put(key_at(i), std::string(200, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);
    uint64_t before = 0;
    for (const LevelStats& level : db_->stats().levels) before += level.bytes;
    ASSERT_GT(before, 0u);

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(200))), Status::Ok);
    // compact_level rather than compact_until_quiet: one L0 file gives the picker no score to act
    // on, so the filter would never be reached and the test would pass by not running it.
    ASSERT_EQ(db_->compact_level(0), Status::Ok);

    uint64_t after = 0;
    for (const LevelStats& level : db_->stats().levels) after += level.bytes;
    EXPECT_LT(after, before * 3 / 4) << "half the entries were dead; the bytes should have gone";
}

/// The point has to be in the *snapshot*, not only in the edits — otherwise it survives a reopen
/// only until the manifest rolls a generation and the edits carrying it are left behind.
TEST_F(Truncate, ThePointSurvivesAGenerationRoll) {
    Options options = make_options(store_, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    options.manifest_edits_per_generation = 2;   // roll almost immediately
    db_.reset();
    auto opened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(opened.has_value());
    db_ = std::move(opened->db);

    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);
    for (int i = 20; i < 40; ++i) put(key_at(i), "v");   // more edits, forcing the roll
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    db_.reset();

    auto reopened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(reopened.has_value());
    db_ = std::move(reopened->db);

    EXPECT_EQ(db_->get(Slice::from(key_at(3))).error(), Status::NotFound)
            << "the snapshot has to carry the point across a generation roll";
}

/// An iterator holds the Version it started on, so a truncation mid-scan does not change what it
/// yields — the same rule that already keeps its files readable after a compaction unlinks them.
TEST_F(Truncate, AnOpenIteratorKeepsTheViewItStartedWith) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto it = db_->iterator();
    ASSERT_TRUE(it->next());
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(it->key().data()), it->key().size()),
              key_at(0));

    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    int seen = 1;
    while (it->next()) ++seen;
    EXPECT_EQ(seen, 20) << "the scan finishes over the world it began in";
}

}  // namespace
}  // namespace elysiumkv::test
