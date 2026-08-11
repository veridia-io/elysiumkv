/* `delete_range` — deleting a band from anywhere in the keyspace with one record rather than one
 * per key.
 *
 * The counterpart to `truncate_below`, which can only drop the lowest-sorting band and is permanent.
 * A tenant sitting in the middle of a keyspace is what needs this.
 *
 * These cases cover the **write** path: what a `delete_range` does to the memtable, and what the
 * file a flush produces records. What a *read* does with a flushed tombstone is a separate matter
 * and lives with the read path.
 */
#include "db/db_impl.hpp"
#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%04d", i);
    return buf;
}

class DeleteRange : public ::testing::Test {
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

    /// Every file the current version names, across all levels.
    std::vector<FileMetadata> files() {
        std::vector<FileMetadata> all;
        auto version = engine().current_version();
        for (size_t level = 0; level < version->num_levels(); ++level) {
            for (const FileMetadata& file : version->files_at(static_cast<int>(level))) {
                all.push_back(file);
            }
        }
        return all;
    }

    TestStore store_{1};
    std::unique_ptr<DB> db_;
};

/// Keys the memtable already holds are deleted on the spot, because the range tombstone about to be
/// written shadows nothing in the file those keys are headed for.
TEST_F(DeleteRange, KeysStillInTheMemtableGoImmediately) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);

    EXPECT_TRUE(db_->get(Slice::from(key_at(4))).has_value());
    EXPECT_EQ(db_->get(Slice::from(key_at(5))).error(), Status::NotFound) << "lower is included";
    EXPECT_EQ(db_->get(Slice::from(key_at(14))).error(), Status::NotFound);
    EXPECT_TRUE(db_->get(Slice::from(key_at(15))).has_value()) << "upper is excluded";
}

/// A write after the delete survives it: the memtable resolves that ordering at call time, which is
/// what lets the file format carry no ordering at all.
TEST_F(DeleteRange, AWriteIntoTheRangeAfterwardsSurvives) {
    put(key_at(7), "before");
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(19))), Status::Ok);
    put(key_at(7), "after");

    auto found = db_->get_copy(Slice::from(key_at(7)));
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->begin(), found->end()), "after");
}

/// The flushed file records the range and the span it covers, so a reader can decide whether to
/// open its tombstone block without any I/O.
TEST_F(DeleteRange, TheFlushedFileRecordsTheRangeItCovers) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    const std::vector<FileMetadata> all = files();
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all[0].num_range_tombstones, 1u);
    EXPECT_EQ(all[0].smallest_range_key, key_at(5));
    EXPECT_EQ(all[0].largest_range_key, key_at(15));
    EXPECT_TRUE(all[0].range_may_cover(Slice::from(key_at(9))));
    EXPECT_FALSE(all[0].range_may_cover(Slice::from(key_at(15))));
}

/* **A memtable holding nothing but a range delete still produces a file.** The tombstone is the
 * content: dropping it because there are no entries would silently discard the deletion, and the
 * keys it covers would come back from the older files it was meant to shadow.
 */
TEST_F(DeleteRange, ARangeDeleteAloneIsWorthAFile) {
    for (int i = 0; i < 5; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(files().size(), 1u);

    // A fresh memtable that sees only this.
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(3))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    const std::vector<FileMetadata> all = files();
    ASSERT_EQ(all.size(), 2u) << "the range delete produced a file of its own";
    const FileMetadata& tombstone_file =
        all[0].num_range_tombstones != 0 ? all[0] : all[1];
    EXPECT_EQ(tombstone_file.num_entries, 0u);
    EXPECT_EQ(tombstone_file.num_range_tombstones, 1u);
}

/// Bounds that describe no keys are not an error, matching an iterator over the same bounds — which
/// yields nothing rather than complaining.
TEST_F(DeleteRange, AnEmptyOrInvertedRangeIsANoOp) {
    for (int i = 0; i < 5; ++i) put(key_at(i), "v");

    EXPECT_EQ(db_->delete_range(Slice::from(key_at(2)), Slice::from(key_at(2))), Status::Ok);
    EXPECT_EQ(db_->delete_range(Slice::from(key_at(4)), Slice::from(key_at(1))), Status::Ok);

    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(files().size(), 1u);
    EXPECT_EQ(files()[0].num_range_tombstones, 0u) << "nothing was recorded, so the file stays v1";
}

/* A range reaching below the truncation floor is **clamped, not refused** — unlike a `put`, which is
 * refused there. A write below the floor is refused because the engine cannot tell it from one made
 * before the truncation; a delete below the floor asks for something already true.
 */
TEST_F(DeleteRange, ARangeReachingBelowTheTruncationFloorIsClamped) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(12))), Status::Ok)
        << "refusing this would make the caller reason about a floor they did not set";
    ASSERT_EQ(db_->flush(), Status::Ok);

    for (const FileMetadata& file : files()) {
        if (file.num_range_tombstones == 0) continue;
        EXPECT_EQ(file.smallest_range_key, key_at(10)) << "clamped up to the floor";
        EXPECT_EQ(file.largest_range_key, key_at(12));
    }
}

/// Wholly below the floor there is nothing left to delete, so nothing is recorded.
TEST_F(DeleteRange, ARangeEntirelyBelowTheFloorRecordsNothing) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(5))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    for (const FileMetadata& file : files()) {
        EXPECT_EQ(file.num_range_tombstones, 0u);
    }
}


// --- the read path -------------------------------------------------------------

/// The point of the whole exercise: a flushed range tombstone shadows what older files hold.
TEST_F(DeleteRange, AFlushedRangeShadowsOlderFiles) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);          // the data, in one file

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);          // the tombstone, in a newer file

    for (int i = 0; i < 20; ++i) {
        const bool deleted = i >= 5 && i < 15;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
}

/// …and an iterator agrees with the point lookups, in both directions.
TEST_F(DeleteRange, AScanSkipsWhatTheRangeDeleted) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    std::vector<std::string> forward;
    for (auto it = db_->iterator(); it->next();) forward.push_back(it->key().to_string());
    ASSERT_EQ(forward.size(), 10u);
    EXPECT_EQ(forward.front(), key_at(0));
    EXPECT_EQ(forward[5], key_at(15));

    std::vector<std::string> backward;
    for (auto it = db_->reverse_iterator(); it->next();) backward.push_back(it->key().to_string());
    std::reverse(backward.begin(), backward.end());
    EXPECT_EQ(backward, forward) << "a range delete is not a matter of which way you scan";
}

/* **A write after the range delete survives it, across a flush boundary too.** The newer file's
 * entry wins by position, and the older file's tombstone shadows only what is older than *it*.
 */
TEST_F(DeleteRange, AKeyRewrittenAfterTheDeleteComesBack) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    put(key_at(7), "again");
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto found = db_->get_copy(Slice::from(key_at(7)));
    ASSERT_TRUE(found.has_value()) << "a newer write outranks an older range tombstone";
    EXPECT_EQ(std::string(found->begin(), found->end()), "again");
    EXPECT_EQ(db_->get(Slice::from(key_at(8))).error(), Status::NotFound) << "its neighbours stay gone";
}

/* A file can delete a range it holds no keys in, and then its **data span says nothing useful**.
 * Pruning a lookup or a scan on that span alone would walk straight past the file that answers it,
 * and the keys would come back.
 */
TEST_F(DeleteRange, ATombstoneOutsideItsFilesDataSpanIsStillFound) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    // A memtable holding one key far above the range it deletes.
    put("zzzz", "v");
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(2)), Slice::from(key_at(6))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    for (int i = 2; i < 6; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound) << i;
    }
    std::vector<std::string> seen;
    for (auto it = db_->iterator(Slice::from(key_at(0)), Slice::from(key_at(10))); it->next();) {
        seen.push_back(it->key().to_string());
    }
    EXPECT_EQ(seen.size(), 6u) << "0,1 and 6..9 survive";
}

/// Reopening loses nothing: the tombstone is in the file and its span is in the manifest.
TEST_F(DeleteRange, ARangeDeleteSurvivesAReopen) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    Options options = make_options(store_, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    db_.reset();
    auto reopened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(reopened.has_value());
    db_ = std::move(reopened->db);

    for (int i = 0; i < 20; ++i) {
        const bool deleted = i >= 5 && i < 15;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
}

/// A range delete still in the memtable shadows the files under it, before any flush.
TEST_F(DeleteRange, AnUnflushedRangeAlreadyShadowsTheFilesBelow) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(key_at(9))).error(), Status::NotFound);
    std::vector<std::string> seen;
    for (auto it = db_->iterator(); it->next();) seen.push_back(it->key().to_string());
    EXPECT_EQ(seen.size(), 10u);
}

/* **A file's own entries survive its own range tombstone**, and that rule is what lets the format
 * carry no ordering at all. A memtable that saw a write, then a delete covering it, then the write
 * again flushes into a single file holding both the tombstone and the live entry — and the file
 * cannot say which came first. It does not need to: the memtable resolved that when the range was
 * recorded, so a tombstone shadows only what is strictly older than the file carrying it.
 */
TEST_F(DeleteRange, AFilesOwnEntriesSurviveItsOwnRangeTombstone) {
    put(key_at(7), "before");
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(19))), Status::Ok);
    put(key_at(7), "after");
    put(key_at(8), "sibling");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(files().size(), 1u) << "the tombstone and the live entries are in one file";
    ASSERT_NE(files()[0].num_range_tombstones, 0u);

    auto found = db_->get_copy(Slice::from(key_at(7)));
    ASSERT_TRUE(found.has_value()) << "the file's own entry was shadowed by the file's own range";
    EXPECT_EQ(std::string(found->begin(), found->end()), "after");

    std::vector<std::string> seen;
    for (auto it = db_->iterator(); it->next();) seen.push_back(it->key().to_string());
    EXPECT_EQ(seen, (std::vector<std::string>{key_at(7), key_at(8)}));
}

// --- compaction ----------------------------------------------------------------

/* **Compaction must not resurrect what a range deleted**, and there are two ways it could.
 *
 * It could copy the covered entries forward into the same file as the tombstone that covers them —
 * and a tombstone shadows nothing in its own file, so they would come back. And it could carry the
 * entries down while leaving the tombstone behind, so the files below stopped being shadowed. The
 * merge drops the first; carrying the tombstone into the output prevents the second.
 */
TEST_F(DeleteRange, CompactionDoesNotResurrectTheDeletedKeys) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->compact_level(0), Status::Ok);

    for (int i = 0; i < 20; ++i) {
        const bool deleted = i >= 5 && i < 15;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
    std::vector<std::string> seen;
    for (auto it = db_->iterator(); it->next();) seen.push_back(it->key().to_string());
    EXPECT_EQ(seen.size(), 10u);
}

/* A compaction that does **not** reach the bottommost level must keep the tombstone, because the
 * files it was shadowing were not part of it and are still down there.
 */
TEST_F(DeleteRange, ATombstoneIsCarriedDownWhileOlderFilesRemainBelow) {
    // Data at the bottom, then a delete above it, compacted only one level down.
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);   // the data is now at L1

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);            // the tombstone sits at L0

    for (int i = 5; i < 15; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound) << i;
    }
    // …and after the tombstone is compacted into L1 it must still be doing its job.
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    for (int i = 0; i < 20; ++i) {
        const bool deleted = i >= 5 && i < 15;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i << " after compaction";
    }
}

/* **The tombstone has to survive a compaction that does not reach the files it shadows.**
 *
 * The case above ends bottommost — the data is part of the same compaction, so once its entries are
 * dropped the tombstone has nothing left to shadow and is rightly let go. Here the data sits two
 * levels down and takes no part: the compaction runs L0 into L1, L2 is untouched, and a tombstone
 * that stopped at L1 without being written would stop shadowing L2 and every deleted key would come
 * back on the next read.
 */
TEST_F(DeleteRange, ATombstoneSurvivesACompactionThatDoesNotReachWhatItShadows) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    ASSERT_EQ(db_->compact_level(1), Status::Ok);   // the data is now at the bottom level

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);   // L0 -> L1 only; the data below is not read

    for (int i = 0; i < 20; ++i) {
        const bool deleted = i >= 5 && i < 15;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
    std::vector<std::string> seen;
    for (auto it = db_->iterator(); it->next();) seen.push_back(it->key().to_string());
    EXPECT_EQ(seen.size(), 10u);
}

/// A compaction where everything is covered leaves the tombstone a file of its own: it has older
/// files to shadow and no entry left to travel with.
TEST_F(DeleteRange, ATombstoneWithNothingLeftToCarryStillGetsAFile) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);

    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(20))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);

    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound) << i;
    }
}

/// Rewrites after the delete survive compaction, which is where a tombstone applied too eagerly
/// would show up.
TEST_F(DeleteRange, RewritesAfterTheDeleteSurviveCompaction) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(5)), Slice::from(key_at(15))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 6; i < 9; ++i) put(key_at(i), "again");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->compact_level(0), Status::Ok);

    for (int i = 0; i < 20; ++i) {
        const bool deleted = (i >= 5 && i < 15) && !(i >= 6 && i < 9);
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
    auto found = db_->get_copy(Slice::from(key_at(7)));
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(std::string(found->begin(), found->end()), "again");
}

/* Evicting a tenant that occupies whole files removes those files. **Which mechanism removes them
 * is deliberately not asserted here** — a compaction may reach them first, and on this fixture it
 * usually does. That the whole-file drop can do it *without reading anything* is the property worth
 * having, and it is asserted where it can be seen: `Version::files_entirely_range_deleted`.
 */
TEST_F(DeleteRange, EvictingATenantRemovesItsFiles) {
    // One tenant per file, so the eviction lines up with a file boundary.
    for (int i = 0; i < 10; ++i) put("tenant-a:" + key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 0; i < 10; ++i) put("tenant-b:" + key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(files().size(), 2u);

    ASSERT_EQ(db_->delete_range(Slice::from(std::string("tenant-a:")),
                                Slice::from(std::string("tenant-a;"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    // Whichever mechanism gets there first — the whole-file drop or a compaction — the evicted
    // tenant's file must not still be sitting in the version.
    Status status = Status::Ok;
    engine().reclaim_truncated_files_for_test(status);
    ASSERT_EQ(status, Status::Ok);

    for (const FileMetadata& file : files()) {
        EXPECT_NE(file.smallest_key.rfind("tenant-a:", 0), 0u)
            << "the evicted tenant's file is still here";
    }
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(db_->get(Slice::from("tenant-a:" + key_at(i))).error(), Status::NotFound) << i;
        EXPECT_TRUE(db_->get(Slice::from("tenant-b:" + key_at(i))).has_value()) << i;
    }
}

/// A file only partly covered keeps its live half: it is narrowed by compaction, not dropped.
TEST_F(DeleteRange, APartlyCoveredFileIsNotDropped) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(0)), Slice::from(key_at(10))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    Status status = Status::Ok;
    engine().reclaim_truncated_files_for_test(status);
    ASSERT_EQ(status, Status::Ok);

    for (int i = 10; i < 20; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i << " was dropped with the rest";
    }
}

/* **A file that deletes without holding anything must survive a truncation above it.**
 *
 * Found by the differential suite, and invisible from the data span alone: a memtable holding only
 * a range delete flushes to a file with no entries, so its largest key is the empty string — which
 * sorts below every truncation point. `files_entirely_truncated` read that as "entirely below the
 * floor" and unlinked the file, the tombstone went with it, and every key it covered came back on
 * the next open. Only streams that truncate *and* range-delete reach it.
 */
TEST_F(DeleteRange, ARangeDeleteIsNotMistakenForATruncatedFile) {
    put("aaa:1", "v");
    put("mmm:1", "v");
    ASSERT_EQ(db_->truncate_below(Slice::from(std::string("bbb"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    // A memtable holding nothing but the range: the file it flushes to has no data span at all.
    ASSERT_EQ(db_->delete_range(Slice::from(std::string("mmm:0")),
                                Slice::from(std::string("mmm:9"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    Status status = Status::Ok;
    engine().reclaim_truncated_files_for_test(status);
    ASSERT_EQ(status, Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(std::string("mmm:1"))).error(), Status::NotFound)
        << "the tombstone-only file was reclaimed as if it were below the floor";
}

// --- inside a batch ------------------------------------------------------------

/* **Order within the batch decides what survives**, and that is the whole reason to want this: a
 * put after the range lands on top of it, a put before it is covered. Two separate calls cannot
 * express that without the range being visibly empty in between.
 */
TEST_F(DeleteRange, OrderWithinABatchDecidesWhatSurvives) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "old");
    ASSERT_EQ(db_->flush(), Status::Ok);

    WriteBatch batch;
    batch.put(Slice::from(key_at(3)), Slice::from(std::string("covered")));                          // before the range
    batch.delete_range(Slice::from(key_at(2)), Slice::from(key_at(8)));
    batch.put(Slice::from(key_at(5)), Slice::from(std::string("survives")));                         // after it
    ASSERT_EQ(db_->write(batch), Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(key_at(3))).error(), Status::NotFound)
        << "written before the range in the same batch, so the range covers it";
    auto kept = db_->get_copy(Slice::from(key_at(5)));
    ASSERT_TRUE(kept.has_value());
    EXPECT_EQ(std::string(kept->begin(), kept->end()), "survives");
    EXPECT_TRUE(db_->get(Slice::from(key_at(1))).has_value()) << "outside the range";
    EXPECT_TRUE(db_->get(Slice::from(key_at(8))).has_value()) << "upper is excluded";
}

/// The range reaches older files too, exactly as the standalone call does.
TEST_F(DeleteRange, ABatchedRangeShadowsOlderFiles) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "old");
    ASSERT_EQ(db_->flush(), Status::Ok);

    WriteBatch batch;
    batch.delete_range(Slice::from(key_at(2)), Slice::from(key_at(8)));
    ASSERT_EQ(db_->write(batch), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    for (int i = 0; i < 10; ++i) {
        const bool deleted = i >= 2 && i < 8;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
}

/* **One batch, two rules about the floor.** A `put` below the truncation point is refused and takes
 * the whole batch with it; a `delete_range` reaching below it is clamped and applies. The batch
 * validates per operation rather than per batch, which is the only way it can hold both.
 */
TEST_F(DeleteRange, ABatchAppliesTheFloorRulePerOperation) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->truncate_below(Slice::from(key_at(10))), Status::Ok);

    WriteBatch refused;
    refused.put(Slice::from(key_at(5)), Slice::from(std::string("below the floor")));
    EXPECT_EQ(db_->write(refused), Status::Config) << "a write below the floor is still refused";

    WriteBatch clamped;
    clamped.delete_range(Slice::from(key_at(0)), Slice::from(key_at(12)));
    clamped.put(Slice::from(key_at(15)), Slice::from(std::string("fine")));
    EXPECT_EQ(db_->write(clamped), Status::Ok) << "the range is clamped, not refused";

    for (int i = 10; i < 12; ++i) {
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).error(), Status::NotFound) << i;
    }
    EXPECT_TRUE(db_->get(Slice::from(key_at(12))).has_value());
    EXPECT_TRUE(db_->get(Slice::from(key_at(15))).has_value());
}

/// An empty or inverted range in a batch deletes nothing and leaves the rest of the batch intact —
/// it must not knock the apply loop out of step with the bounds resolved for it.
TEST_F(DeleteRange, AnEmptyRangeInABatchLeavesTheRestOfItAlone) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");

    WriteBatch batch;
    batch.delete_range(Slice::from(key_at(4)), Slice::from(key_at(4)));   // empty
    batch.put(Slice::from(key_at(1)), Slice::from(std::string("one")));
    batch.delete_range(Slice::from(key_at(9)), Slice::from(key_at(2)));   // inverted
    batch.delete_range(Slice::from(key_at(6)), Slice::from(key_at(8)));   // real
    batch.put(Slice::from(key_at(2)), Slice::from(std::string("two")));
    ASSERT_EQ(db_->write(batch), Status::Ok);

    auto one = db_->get_copy(Slice::from(key_at(1)));
    ASSERT_TRUE(one.has_value());
    EXPECT_EQ(std::string(one->begin(), one->end()), "one");
    auto two = db_->get_copy(Slice::from(key_at(2)));
    ASSERT_TRUE(two.has_value());
    EXPECT_EQ(std::string(two->begin(), two->end()), "two");
    EXPECT_EQ(db_->get(Slice::from(key_at(6))).error(), Status::NotFound)
        << "the real range must still have been the one applied";
    EXPECT_TRUE(db_->get(Slice::from(key_at(4))).has_value());
}

/// The batch lands whole: a range and the writes around it reach one memtable, so no flush can
/// split them and no reader sees the range empty.
TEST_F(DeleteRange, ABatchedRangeAndItsWritesLandTogether) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "old");
    ASSERT_EQ(db_->flush(), Status::Ok);

    WriteBatch batch;
    batch.delete_range(Slice::from(key_at(0)), Slice::from(key_at(10)));
    for (int i = 0; i < 3; ++i) batch.put(Slice::from(key_at(i)), Slice::from(std::string("reseeded")));
    ASSERT_EQ(db_->write(batch), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    std::vector<std::string> seen;
    for (auto it = db_->iterator(); it->next();) seen.push_back(it->key().to_string());
    EXPECT_EQ(seen, (std::vector<std::string>{key_at(0), key_at(1), key_at(2)}))
        << "evict and re-seed is one step";
}

/* **A file carrying range tombstones is rewritten rather than trivially moved.** A move does not
 * rewrite, so moving one to the level that reclaims tombstones would carry them past the only point
 * that drops them, and nothing afterwards forces the rewrite. The rule already existed for point
 * tombstones; a file can carry range tombstones and no point tombstones at all — a flush whose
 * memtable saw only a `delete_range` is exactly that — so testing one and not the other lets the
 * commonest shape straight through.
 */
TEST_F(DeleteRange, ARangeTombstoneFileIsRewrittenRatherThanMovedToTheBottom) {
    for (int i = 0; i < 10; ++i) put(key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    ASSERT_EQ(db_->compact_level(1), Status::Ok);   // data at the bottom

    // A memtable holding nothing but a range over keys the bottom level does not hold, so the file
    // it flushes to overlaps nothing and would otherwise be a trivial move all the way down.
    ASSERT_EQ(db_->delete_range(Slice::from(std::string("zzz:0")),
                                Slice::from(std::string("zzz:9"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    ASSERT_EQ(db_->compact_level(1), Status::Ok);

    for (const FileMetadata& file : files()) {
        EXPECT_EQ(file.num_range_tombstones, 0u)
            << "a tombstone reached the bottommost level without being reclaimed";
    }
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

/* **A file covered by one of several disjoint ranges is dropped whole too**, which needs the
 * tombstones themselves rather than what the manifest records about them.
 *
 * The manifest keeps the *hull* of a file's ranges — the range itself when there is one, a bounding
 * interval with unknown gaps when there are more. A hull can show a file is not covered and never
 * that it is, so acting on it alone means giving up the moment two deletes land in one file, which
 * is what a compaction merging tombstones produces routinely. The cover's block is read instead,
 * once, and only for candidates the hull already made plausible.
 */
TEST_F(DeleteRange, AFileCoveredByOneOfSeveralRangesIsStillDroppedWhole) {
    for (int i = 0; i < 10; ++i) put("aaa:" + key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 0; i < 10; ++i) put("mmm:" + key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(files().size(), 2u);

    // Two deletes far enough apart that merging cannot join them: one file, two ranges, a hull that
    // spans both and the gap between.
    ASSERT_EQ(db_->delete_range(Slice::from(std::string("aaa:")),
                                Slice::from(std::string("aaa;"))), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(std::string("zzz:")),
                                Slice::from(std::string("zzz;"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (const FileMetadata& file : files()) {
        if (file.num_range_tombstones != 0) {
            ASSERT_EQ(file.num_range_tombstones, 2u) << "the two ranges must not have merged";
        }
    }

    // Not asserted on the return value: the reclaim runs on its own during the flush above, so by
    // the time this asks there is usually nothing left to do. What the exact path buys is asserted
    // by the outcome — with the manifest hull alone, a cover carrying two ranges authorises nothing
    // and the file below is still here.
    Status status = Status::Ok;
    engine().reclaim_truncated_files_for_test(status);
    ASSERT_EQ(status, Status::Ok);

    for (const FileMetadata& file : files()) {
        EXPECT_NE(file.smallest_key.rfind("aaa:", 0), 0u) << "the covered file is still here";
    }
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(db_->get(Slice::from("aaa:" + key_at(i))).error(), Status::NotFound) << i;
        EXPECT_TRUE(db_->get(Slice::from("mmm:" + key_at(i))).has_value()) << i;
    }
}

/// The gap in the hull is not coverage: a file sitting in it survives.
TEST_F(DeleteRange, AFileInTheGapBetweenTwoRangesIsNotDropped) {
    for (int i = 0; i < 10; ++i) put("mmm:" + key_at(i), "v");
    ASSERT_EQ(db_->flush(), Status::Ok);

    ASSERT_EQ(db_->delete_range(Slice::from(std::string("aaa:")),
                                Slice::from(std::string("aaa;"))), Status::Ok);
    ASSERT_EQ(db_->delete_range(Slice::from(std::string("zzz:")),
                                Slice::from(std::string("zzz;"))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    Status status = Status::Ok;
    engine().reclaim_truncated_files_for_test(status);
    ASSERT_EQ(status, Status::Ok);

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(db_->get(Slice::from("mmm:" + key_at(i))).has_value())
            << i << " sat in the gap between the two ranges and was dropped anyway";
    }
}

/* **Several real ranges in one batch, each with its own bounds.** The resolved bounds are held one
 * entry per operation and read back by the same index, so two ranges cannot land on each other's
 * bounds however many no-ops sit between them.
 */
TEST_F(DeleteRange, SeveralRangesInOneBatchEachApplyToTheirOwnBounds) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "old");
    ASSERT_EQ(db_->flush(), Status::Ok);

    WriteBatch batch;
    batch.delete_range(Slice::from(key_at(2)), Slice::from(key_at(5)));
    batch.delete_range(Slice::from(key_at(8)), Slice::from(key_at(8)));    // empty, between them
    batch.delete_range(Slice::from(key_at(12)), Slice::from(key_at(15)));
    ASSERT_EQ(db_->write(batch), Status::Ok);

    for (int i = 0; i < 20; ++i) {
        const bool deleted = (i >= 2 && i < 5) || (i >= 12 && i < 15);
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 0; i < 20; ++i) {
        const bool deleted = (i >= 2 && i < 5) || (i >= 12 && i < 15);
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i << " after flush";
    }
}

/// Ranges and writes interleaved: each range covers what came before it and not what comes after.
TEST_F(DeleteRange, RangesAndWritesInterleaveInOneBatch) {
    WriteBatch batch;
    batch.put(Slice::from(key_at(1)), Slice::from(std::string("first")));
    batch.delete_range(Slice::from(key_at(0)), Slice::from(key_at(5)));    // covers key 1
    batch.put(Slice::from(key_at(2)), Slice::from(std::string("second")));
    batch.delete_range(Slice::from(key_at(2)), Slice::from(key_at(3)));    // covers key 2
    batch.put(Slice::from(key_at(3)), Slice::from(std::string("third")));  // after both
    ASSERT_EQ(db_->write(batch), Status::Ok);

    EXPECT_EQ(db_->get(Slice::from(key_at(1))).error(), Status::NotFound);
    EXPECT_EQ(db_->get(Slice::from(key_at(2))).error(), Status::NotFound);
    auto third = db_->get_copy(Slice::from(key_at(3)));
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(std::string(third->begin(), third->end()), "third");
}

/// Overlapping ranges in one batch are just their union once they reach a file.
TEST_F(DeleteRange, OverlappingRangesInOneBatchMergeAtTheFile) {
    for (int i = 0; i < 20; ++i) put(key_at(i), "old");
    ASSERT_EQ(db_->flush(), Status::Ok);

    WriteBatch batch;
    batch.delete_range(Slice::from(key_at(2)), Slice::from(key_at(8)));
    batch.delete_range(Slice::from(key_at(6)), Slice::from(key_at(12)));
    ASSERT_EQ(db_->write(batch), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    for (const FileMetadata& file : files()) {
        if (file.num_range_tombstones == 0) continue;
        EXPECT_EQ(file.num_range_tombstones, 1u) << "the two overlap, so the file carries one";
        EXPECT_EQ(file.smallest_range_key, key_at(2));
        EXPECT_EQ(file.largest_range_key, key_at(12));
    }
    for (int i = 0; i < 20; ++i) {
        const bool deleted = i >= 2 && i < 12;
        EXPECT_EQ(db_->get(Slice::from(key_at(i))).has_value(), !deleted) << i;
    }
}

}  // namespace
}  // namespace elysiumkv::test
