#include "db/db_impl.hpp"

#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <random>
#include <string>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Invariants and sanitizers" — the invariants that are checked continuously under a debug flag.
/// They are cheap to state and expensive to discover the hard way: a violated
/// one shows up as a wrong answer under load, not as a crash.
class InvariantTest : public ::testing::Test {
protected:
    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    void open(size_t memtable_bytes = 64u << 10) {
        Options options = make_options(store_, Compression::Zstd, memtable_bytes);
        options.paranoid_checks = true;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(opened->db);
    }

    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "cluster:%02d:key:%08d", i % 8, i);
        return buf;
    }

    TestStore store_;
    std::unique_ptr<DB> db_;
};

TEST_F(InvariantTest, HoldAfterEveryFlush) {
    open();
    std::mt19937_64 rng(4242);

    for (int round = 0; round < 8; ++round) {
        for (int i = 0; i < 500; ++i) {
            const int index = static_cast<int>(rng() % 2000);
            const std::string key = key_at(index);
            if (rng() % 6 == 0) {
                ASSERT_EQ(db_->remove(Slice::from(key)), Status::Ok);
            } else {
                const std::string value(20 + rng() % 100, static_cast<char>('a' + round));
                ASSERT_EQ(db_->put(Slice::from(key), Slice::from(value)), Status::Ok);
            }
        }
        ASSERT_EQ(db_->flush(), Status::Ok);
        // Every file exists in its recorded store, its recorded [smallest,
        // largest] matches its contents, its entry count is exact, and L1+ holds
        // no overlapping ranges.
        ASSERT_EQ(engine().check_invariants(), Status::Ok) << "round " << round;
    }
}

TEST_F(InvariantTest, HoldAfterReopen) {
    open();
    for (int i = 0; i < 1000; ++i) {
        ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                  Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);
    db_.reset();

    ASSERT_NO_FATAL_FAILURE(open());
    EXPECT_EQ(engine().check_invariants(), Status::Ok);
}

// The check has to be able to fail, or it is decoration. Removing a file the
// version still references is exactly the state ARCHITECTURE.md "Open and recovery" calls corruption on a
// Durable store.
TEST_F(InvariantTest, DetectAMissingFile) {
    open();
    for (int i = 0; i < 500; ++i) {
        ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                  Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(engine().check_invariants(), Status::Ok);

    // Take the file out from under the version, as a lost volume would.
    auto names = store_.store(0)->list("").get();
    ASSERT_TRUE(names.has_value());
    ASSERT_FALSE(names->empty());
    ASSERT_EQ(store_.store(0)->remove(names->front()).get(), Status::Ok);

    EXPECT_EQ(engine().check_invariants(), Status::Corrupt);
}

// ARCHITECTURE.md "Invariants and sanitizers" — the close-time invariants: zero live version references, zero open
// iterators, zero outstanding pins. ARCHITECTURE.md "Statistics are a buffer, not a struct" makes `pins_outstanding` a reported
// number, so the leak check is a plain assertion rather than a debug-build
// facility.
TEST_F(InvariantTest, AtCloseNothingIsStillHeld) {
    open();
    for (int i = 0; i < 500; ++i) {
        ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                  Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);

    {
        auto it = db_->iterator();
        int seen = 0;
        while (it->next()) ++seen;
        EXPECT_EQ(seen, 500);
        auto pinned = db_->get(Slice::from(key_at(0)));
        ASSERT_TRUE(pinned.has_value());
        EXPECT_FALSE(pinned->value().empty());
        EXPECT_EQ(db_->stats().pins_outstanding, 1u) << "a held pin must be counted";

        // Moving a pin transfers the count rather than duplicating it.
        Pinned moved = std::move(*pinned);
        EXPECT_EQ(db_->stats().pins_outstanding, 1u);
    }

    EXPECT_EQ(db_->stats().pins_outstanding, 0u) << "a pin outlived its scope";
    EXPECT_EQ(engine().pending_deletions(), 0u);
    EXPECT_EQ(engine().check_invariants(), Status::Ok);
}

}  // namespace
}  // namespace elysiumkv::test
