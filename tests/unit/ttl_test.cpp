/* `Options::ttl` — dropping data by age with a manifest edit, nothing read and nothing rewritten.
 *
 * The same trick `truncate_below` uses, keyed on age instead of key order, and with the same
 * granularity: a file is the smallest thing an edit can drop, so this buys "data older than X
 * disappears" rather than "this key expires at X".
 */
#include "db/db_impl.hpp"
#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%04d", i);
    return buf;
}

constexpr uint64_t kLifetime = 60'000;

class Ttl : public ::testing::Test {
protected:
    void open_with_ttl(bool enabled = true) {
        Options options = make_options(store_, Compression::None, 64u << 10);
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_; };
        if (enabled) options.ttl = Duration(static_cast<int64_t>(kLifetime));
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value());
        db_ = std::move(opened->db);
    }

    void SetUp() override { open_with_ttl(); }

    void put(const std::string& key, const std::string& value) {
        ASSERT_EQ(db_->put(Slice::from(key), Slice::from(value)), Status::Ok);
    }

    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    void sweep() {
        Status status = Status::Ok;
        engine().reclaim_dead_files_for_test(status);
        ASSERT_EQ(status, Status::Ok);
    }

    int live_files() {
        int count = 0;
        auto version = engine().current_version();
        for (size_t level = 0; level < version->num_levels(); ++level) {
            count += static_cast<int>(version->files_at(static_cast<int>(level)).size());
        }
        return count;
    }

    TestStore store_{1};
    uint64_t now_ = 1'000'000;
    std::unique_ptr<DB> db_;
};

TEST_F(Ttl, DataOutlivingTheLimitIsDropped) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(live_files(), 1);

    now_ += kLifetime + 1;
    sweep();

    EXPECT_EQ(live_files(), 0) << "the file outlived the limit and was not dropped";
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound) << i;
    }
}

TEST_F(Ttl, DataInsideTheLimitStays) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    now_ += kLifetime - 1;
    sweep();

    EXPECT_EQ(live_files(), 1);
    EXPECT_TRUE(db_->get(Slice::from(key_at(0))).has_value());
}

/* Measured from the newest write, not the oldest. A file spans whatever arrived between its
 * memtable's creation and its seal, so keying on the oldest would drop a file that still holds data
 * well inside the limit — the failure being silent data loss rather than a stale read.
 */
TEST_F(Ttl, AFileIsJudgedByItsNewestWriteNotItsOldest) {
    put(key_at(0), "old");
    now_ += kLifetime - 1;         // the memtable is now nearly at the limit
    put(key_at(1), "fresh");       // …but this write is not
    ASSERT_EQ(db_->flush(), Status::Ok);

    now_ += 2;                     // the *oldest* write has now outlived the limit
    sweep();

    EXPECT_EQ(live_files(), 1) << "dropped on the oldest write, taking a fresh one with it";
    EXPECT_TRUE(db_->get(Slice::from(key_at(1))).has_value());
}

/* The soundness condition. Dropping a file that shadows an older version of the same key does
 * not remove the key — it *uncovers* the older version. That is a resurrection, and it would be far
 * worse than an expiry that runs late. So a file goes only once nothing older overlaps it.
 */
TEST_F(Ttl, AFileWithSomethingOlderBeneathItIsNotDropped) {
    put(key_at(0), "first");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);   // "first" is now deeper

    now_ += 10;
    put(key_at(0), "second");
    ASSERT_EQ(db_->flush(), Status::Ok);            // the newer version sits above it

    now_ += kLifetime + 1;                          // both are past the limit
    sweep();

    // Whatever survives, the one thing that must never happen is the key reading as "first" again.
    auto found = db_->get_copy(Slice::from(key_at(0)));
    if (found.has_value()) {
        EXPECT_EQ(std::string(found->begin(), found->end()), "second")
            << "dropping the newer file uncovered the older value";
    }
}

/// A file carrying range tombstones is left alone: they would go with it, and they shadow files the
/// age says nothing about.
TEST_F(Ttl, AFileCarryingRangeTombstonesIsNotExpired) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(2)), Slice::from(key_at(5))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    now_ += kLifetime + 1;
    sweep();

    for (const auto& level : {0, 1, 2}) {
        for (const FileMetadata& file : engine().current_version()->files_at(level)) {
            if (file.num_range_tombstones != 0) return;   // still here, which is the point
        }
    }
    // Reaching here means it went; the keys it covered must not have come back.
    for (int i = 2; i < 5; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound) << i;
    }
}

TEST_F(Ttl, WithoutTheOptionNothingExpires) {
    db_.reset();
    open_with_ttl(/*enabled=*/false);
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    now_ += kLifetime * 1000;
    sweep();

    EXPECT_EQ(live_files(), 1);
    EXPECT_TRUE(db_->get(Slice::from(key_at(0))).has_value());
}

/// Expiry survives a reopen, because the newest-write time is in the manifest rather than derived.
TEST_F(Ttl, TheJudgementSurvivesAReopen) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    db_.reset();

    now_ += kLifetime + 1;
    open_with_ttl();
    sweep();

    EXPECT_EQ(live_files(), 0);
}

}  // namespace
}  // namespace elysiumkv::test
