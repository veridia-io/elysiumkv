#include "db/db_impl.hpp"

#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Inside an SST" and ARCHITECTURE.md "Absence is an answer, not an error" — a codec change or a placement remap reaches new data at once
/// and old data as compaction sweeps past it. `compact_level()` is not what makes
/// migration *happen*; it is what makes it *finish*.
class MigrationTest : public ::testing::Test {
protected:
    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    void open(Options options) {
        options.background = BackgroundMode::Inline;
        options.paranoid_checks = true;
        options_ = options;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(opened->db);
    }

    void reopen_with(Options options) {
        db_.reset();
        options.background = BackgroundMode::Inline;
        options.paranoid_checks = true;
        options_ = options;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(opened->db);
    }

    /// Everything settled at the bottom, so the level under test is the one that
    /// holds the data.
    Options settled_options(Compression codec) {
        Options options = make_options(store_, codec, 1u << 20);
        options.levels[0].max_files = 1;
        options.levels[1].max_bytes = 1;
        options.levels[1].compression = codec;
        options.levels[2].compression = codec;
        // Small files, so a key range gets its own file: whether a cold range is
        // swept is only a question when it is not sharing a file with a hot one.
        options.levels[1].target_file_bytes = 4096;
        options.levels[2].target_file_bytes = 4096;
        return options;
    }

    /// Drives everything to the bottom deterministically. Score alone leaves up
    /// to max_files files at a level, so "flush and hope" is not a setup.
    void settle() {
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_EQ(db_->compact_level(0), Status::Ok);
        ASSERT_EQ(db_->compact_level(1), Status::Ok);
    }

    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    void put_range(int from, int to, const std::string& tag) {
        for (int i = from; i < to; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(tag + "-" + std::string(60, 'v'))),
                      Status::Ok);
        }
    }

    void expect_readable(int from, int to) {
        for (int i = from; i < to; ++i) {
            auto found = db_->get(Slice::from(key_at(i)));
            ASSERT_TRUE(found.has_value()) << key_at(i) << ": " << status_name(found.error());
        }
    }

    LevelStats level(int index) { return db_->stats().levels[static_cast<size_t>(index)]; }
    int bottom() { return static_cast<int>(db_->stats().levels.size()) - 1; }

    TestStore store_{2};
    Options options_;
    std::unique_ptr<DB> db_;
};

// ARCHITECTURE.md "Fault injection" — the codec-migration case.
TEST_F(MigrationTest, CompactLevelFinishesACodecChange) {
    open(settled_options(Compression::None));
    put_range(0, 600, "v1");
    settle();
    ASSERT_GT(level(bottom()).file_count, 0);
    EXPECT_EQ(level(bottom()).files_stale_codec, 0) << "written under the configured codec";

    // Reconfigure to Zstd. Everything already written is now stale, and still
    // perfectly readable — the per-block type byte sees to that.
    reopen_with(settled_options(Compression::Zstd));
    const int stale = level(bottom()).files_stale_codec;
    EXPECT_GT(stale, 0);
    EXPECT_EQ(stale, level(bottom()).file_count);
    expect_readable(0, 600);

    // New data arrives under the new codec at once.
    put_range(600, 700, "v2");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    expect_readable(0, 700);

    // And compact_level finishes the job.
    ASSERT_EQ(db_->compact_level(bottom()), Status::Ok);
    EXPECT_EQ(level(bottom()).files_stale_codec, 0);
    EXPECT_GT(level(bottom()).file_count, 0);
    expect_readable(0, 700);
    EXPECT_EQ(engine().check_invariants(), Status::Ok);

    // A second call performs no further rewriting.
    const uint64_t written = db_->stats().compaction_bytes_written;
    ASSERT_EQ(db_->compact_level(bottom()), Status::Ok);
    EXPECT_EQ(db_->stats().compaction_bytes_written, written)
        << "compact_level has a completion condition; it is not a periodic rewrite";
}

// ARCHITECTURE.md "Inside an SST" — cold key ranges may never be swept. This documents the limit rather
// than asserting a bug: correctness is unaffected, completion is what is lost.
TEST_F(MigrationTest, ColdRangesAreNotSweptByOrdinaryCompaction) {
    open(settled_options(Compression::None));

    // Range A (0..300) and range B (10000..10300), both settled at the bottom.
    put_range(0, 300, "a");
    put_range(10'000, 10'300, "b");
    settle();
    ASSERT_GT(level(bottom()).file_count, 0);

    reopen_with(settled_options(Compression::Zstd));
    ASSERT_GT(level(bottom()).files_stale_codec, 0);

    // Keep writing to A only. Its files migrate as compaction sweeps that range.
    for (int round = 0; round < 6; ++round) {
        put_range(0, 300, "a" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    }

    // B was never written to, so nothing ever compacted into its range: its file
    // is still on the old codec. This is the limit ARCHITECTURE.md "Inside an SST" describes.
    EXPECT_GT(level(bottom()).files_stale_codec, 0)
        << "a dormant range is never swept, however long the store runs";

    bool dormant_range_is_stale = false;
    for (const FileMetadata& file : engine().current_version()->files_at(bottom())) {
        if (file.compression == Compression::None && file.smallest_key >= key_at(10'000)) {
            dormant_range_is_stale = true;
        }
    }
    EXPECT_TRUE(dormant_range_is_stale);

    // compact_level is the answer, and it is the only one.
    ASSERT_EQ(db_->compact_level(bottom()), Status::Ok);
    EXPECT_EQ(level(bottom()).files_stale_codec, 0);
    expect_readable(0, 300);
    expect_readable(10'000, 10'300);
}

// ARCHITECTURE.md "Fault injection" — the tombstone-reclamation case.
TEST_F(MigrationTest, CompactLevelReclaimsTombstonesAtTheBottom) {
    open(settled_options(Compression::None));

    put_range(0, 500, "v1");
    settle();

    // Delete a large range and let it settle. A trivial move into the bottommost
    // level is refused for a tombstone-bearing file (ARCHITECTURE.md "Compaction"), so these arrive by
    // rewrite — and a rewrite at the bottom drops them.
    for (int i = 0; i < 400; ++i) {
        ASSERT_EQ(db_->remove(Slice::from(key_at(i))), Status::Ok);
    }
    settle();
    ASSERT_EQ(db_->compact_level(bottom()), Status::Ok);

    uint64_t tombstones = 0;
    uint64_t entries = 0;
    for (const FileMetadata& file : engine().current_version()->files_at(bottom())) {
        tombstones += file.num_tombstones;
        entries += file.num_entries;
    }
    EXPECT_EQ(tombstones, 0u) << "the bottommost level is where deleted keys stop costing anything";
    EXPECT_EQ(entries, 100u) << "only the surviving keys remain";

    for (int i = 0; i < 400; ++i) {
        EXPECT_FALSE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
    expect_readable(400, 500);
}

// ARCHITECTURE.md "Absence is an answer, not an error" — elsewhere it compacts into the level below, so the level empties.
TEST_F(MigrationTest, CompactLevelOnANonBottomLevelEmptiesIt) {
    Options options = make_options(store_, Compression::Zstd, 1u << 20);
    options.levels[0].max_files = 1000;   // no score pressure
    options.levels[1].max_bytes = 1u << 30;
    open(options);

    for (int round = 0; round < 4; ++round) {
        put_range(round * 200, round * 200 + 200, "v1");
        ASSERT_EQ(db_->flush(), Status::Ok);
    }
    ASSERT_EQ(level(0).file_count, 4);

    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    EXPECT_EQ(level(0).file_count, 0) << "one pass over the level's files empties it";
    EXPECT_GT(level(1).file_count, 0);
    expect_readable(0, 800);
    EXPECT_EQ(engine().check_invariants(), Status::Ok);

    // And again is a no-op, since the level is empty.
    const uint64_t written = db_->stats().compaction_bytes_written;
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    EXPECT_EQ(db_->stats().compaction_bytes_written, written);
}

// ARCHITECTURE.md "Statistics are a buffer, not a struct" — files_stale_store is the placement half of the same question.
TEST_F(MigrationTest, CompactLevelRejectsALevelThatDoesNotExist) {
    open(settled_options(Compression::None));
    EXPECT_EQ(db_->compact_level(-1), Status::Config);
    EXPECT_EQ(db_->compact_level(99), Status::Config);
}

}  // namespace
}  // namespace elysiumkv::test
