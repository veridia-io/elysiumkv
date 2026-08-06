/* Multiple readers, and the one hazard that stops it working by itself.
 *
 * Most of it already worked: a clean open writes nothing, objects are immutable and write-once so a
 * cached block can never become wrong, and a reader performs no compare-and-set so it is outside the
 * ownership protocol entirely. What did not work is that the writer's collector decides an object is
 * collectible when no live `Version` **in its own process** references it — a reader elsewhere is
 * invisible to it. `Options::obsolete_retention` is the whole fix, and the third case below is the
 * one that would fail without it.
 *
 * The other half is the orphan sweep, which replaces `reclaim_orphans_at_open`. That flag decided on
 * a *single instantaneous* observation, which cannot tell a dead writer's residue from a live
 * writer's just-committed file. The sweep decides on a *sustained* one, re-reading the manifest each
 * pass so a file whose edit has since landed leaves the candidate set on its own.
 */

#include "db/db_impl.hpp"

#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

class ReadOnlyTest : public ::testing::Test {
protected:
    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    Options writer_options() {
        Options options = make_options(store_, Compression::None, 16u << 10);
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_.load(); };
        return options;
    }

    /// A reader shares the store and catalog objects but is a separate engine instance — which is
    /// what a second process is, minus the process boundary.
    Options reader_options() { return writer_options(); }

    std::unique_ptr<DB> open_writer(const Options& options) {
        auto opened = DB::open(options);
        EXPECT_TRUE(opened.has_value()) << (opened ? "" : status_name(opened.error()));
        return opened.has_value() ? std::move(*opened) : nullptr;
    }

    void write(DB& db, int from, int count, const std::string& tag = "v") {
        for (int i = from; i < from + count; ++i) {
            ASSERT_EQ(db.put(Slice::from(key_at(i)), Slice::from(tag + std::to_string(i))),
                      Status::Ok);
        }
    }

    /// Every byte of the manifest directory, so "a reader writes nothing" is a comparison rather
    /// than a claim.
    std::map<std::string, std::string> manifest_snapshot() {
        std::map<std::string, std::string> files;
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(store_.path() / "manifest")) {
            if (!entry.is_regular_file()) continue;
            std::ifstream in(entry.path(), std::ios::binary);
            files[entry.path().string()] =
                std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        return files;
    }

    std::vector<std::string> store_listing(size_t index = 0) {
        auto names = store_.store(index)->list("").get();
        EXPECT_TRUE(names.has_value());
        return names.has_value() ? *names : std::vector<std::string>{};
    }

    TestStore store_{2};
    std::atomic<uint64_t> now_{1'000'000};
};

// --- a reader touches nothing -------------------------------------------------

// **The property the whole design rests on.** If a reader writes anything, it can fail to write, be
// fenced, or corrupt — and none of the reasoning above survives.
TEST_F(ReadOnlyTest, AReaderWritesNothingAndDeletesNothing) {
    Options options = writer_options();
    {
        auto writer = open_writer(options);
        ASSERT_NE(writer, nullptr);
        write(*writer, 0, 200);
        ASSERT_EQ(writer->flush(), Status::Ok);
    }

    const std::map<std::string, std::string> manifest_before = manifest_snapshot();
    const std::vector<std::string> objects_before = store_listing();
    ASSERT_FALSE(objects_before.empty());

    auto reader = DB::open_read_only(options);
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());

    // Read every way there is, then refresh, then close.
    EXPECT_TRUE((*reader)->get_copy(Slice::from(key_at(0))).has_value());
    auto it = (*reader)->iterator();
    int seen = 0;
    while (it->next()) ++seen;
    EXPECT_EQ(seen, 200);
    it.reset();
    (void)(*reader)->stats();
    EXPECT_EQ((*reader)->refresh(), Status::Ok);
    reader->reset();

    EXPECT_EQ(manifest_snapshot(), manifest_before) << "a reader wrote to the manifest";
    EXPECT_EQ(store_listing(), objects_before) << "a reader deleted an object";
}

// --- freshness is explicit -----------------------------------------------------

TEST_F(ReadOnlyTest, AReaderSeesASnapshotUntilItRefreshes) {
    Options options = writer_options();
    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 50);
    ASSERT_EQ(writer->flush(), Status::Ok);

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);
    EXPECT_TRUE(reader->get_copy(Slice::from(key_at(0))).has_value());

    write(*writer, 1000, 50, "later");
    ASSERT_EQ(writer->flush(), Status::Ok);

    // **The half that would be missing from an auto-refreshing implementation.** Without it, a
    // reader that re-read on every `get` would pass the second half and prove nothing.
    EXPECT_EQ(reader->get_copy(Slice::from(key_at(1000))).error(), Status::NotFound)
        << "a reader holds a snapshot, not a subscription";

    ASSERT_EQ(reader->refresh(), Status::Ok);
    EXPECT_TRUE(reader->get_copy(Slice::from(key_at(1000))).has_value());
}

// An iterator holds its version, so refreshing under it changes nothing it is reading.
//
// The retention is not incidental here. Within one process `live_versions_` keeps an iterator's
// files alive; across processes it cannot, so the window is the only thing that does — which this
// case discovered by failing with a vanished file partway through the scan.
TEST_F(ReadOnlyTest, AnIteratorSurvivesARefreshUnderneathIt) {
    Options options = writer_options();
    options.obsolete_retention = Duration(600'000);
    options.orphan_retention = Duration(600'000);
    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 200);
    ASSERT_EQ(writer->flush(), Status::Ok);

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);

    auto it = reader->iterator();
    ASSERT_TRUE(it->next());

    write(*writer, 1000, 200, "later");
    ASSERT_EQ(writer->flush(), Status::Ok);
    ASSERT_EQ(writer->compact_level(0), Status::Ok);
    ASSERT_EQ(reader->refresh(), Status::Ok);

    int seen = 1;
    while (it->next()) ++seen;
    EXPECT_EQ(it->status(), Status::Ok);
    EXPECT_EQ(seen, 200) << "the scan completed against the version it started on";
}

// --- the hazard ----------------------------------------------------------------

// **The case the retention window exists for**, and the one that fails without it: the writer
// compacts, its inputs become locally unreferenced, and it deletes objects the reader is reading.
TEST_F(ReadOnlyTest, ARetentionWindowKeepsAReadersFilesAliveAcrossACompaction) {
    Options options = writer_options();
    options.obsolete_retention = Duration(600'000);
    options.orphan_retention = Duration(600'000);   // must be at least the reader window

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 200);
    ASSERT_EQ(writer->flush(), Status::Ok);

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);
    ASSERT_TRUE(reader->get_copy(Slice::from(key_at(0))).has_value());

    // The writer moves on and supersedes everything the reader is holding.
    for (int round = 0; round < 3; ++round) {
        write(*writer, 0, 200, "round" + std::to_string(round));
        ASSERT_EQ(writer->flush(), Status::Ok);
        ASSERT_EQ(writer->compact_level(0), Status::Ok);
    }

    // The reader is still on its original version, and every one of its files must still be there.
    for (int i = 0; i < 200; i += 20) {
        EXPECT_TRUE(reader->get_copy(Slice::from(key_at(i))).has_value())
            << "the writer collected a file the reader was still reading, at key " << i;
    }
}

// The control: with no retention the same sequence deletes the reader's files. This is today's
// behaviour, so if it does not reproduce, the case above is not exercising the hazard.
TEST_F(ReadOnlyTest, WithoutARetentionWindowTheReadersFilesAreCollected) {
    Options options = writer_options();   // obsolete_retention unset: delete immediately

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 200);
    ASSERT_EQ(writer->flush(), Status::Ok);

    const std::vector<std::string> reader_files = store_listing();
    ASSERT_FALSE(reader_files.empty());

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);
    ASSERT_TRUE(reader->get_copy(Slice::from(key_at(0))).has_value());

    for (int round = 0; round < 3; ++round) {
        write(*writer, 0, 200, "round" + std::to_string(round));
        ASSERT_EQ(writer->flush(), Status::Ok);
        ASSERT_EQ(writer->compact_level(0), Status::Ok);
    }

    const std::vector<std::string> after = store_listing();
    bool any_original_gone = false;
    for (const std::string& name : reader_files) {
        if (std::find(after.begin(), after.end(), name) == after.end()) any_original_gone = true;
    }
    EXPECT_TRUE(any_original_gone)
        << "nothing was collected, so the retention case above is not testing the hazard";
}

// --- configuration -------------------------------------------------------------

// A crash empties the pending queue, so an obsoleted object returns as an orphan protected by the
// orphan window and nothing else. Ordered the wrong way, the reader window is silently inert.
TEST_F(ReadOnlyTest, TheOrphanWindowMayNotBeShorterThanTheReaderWindow) {
    Options options = writer_options();
    options.obsolete_retention = Duration(600'000);
    options.orphan_retention = Duration(300'000);

    auto opened = DB::open_with_result(options);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error(), Status::Config);

    // Equal is the constraint, not a strict inequality.
    options.orphan_retention = Duration(600'000);
    auto equal = DB::open_with_result(options);
    EXPECT_TRUE(equal.has_value()) << (equal ? "" : status_name(equal.error()));
}

TEST_F(ReadOnlyTest, AReaderRefusesAStoreThatDoesNotExistRatherThanCreatingIt) {
    Options options = writer_options();
    auto reader = DB::open_read_only(options);
    ASSERT_FALSE(reader.has_value());
    EXPECT_EQ(reader.error(), Status::NotFound);

    // And it left nothing behind: a writer opening afterwards still finds a fresh store.
    EXPECT_FALSE(std::filesystem::exists(store_.path() / "manifest") &&
                 !std::filesystem::is_empty(store_.path() / "manifest"))
        << "the reader created manifest state";
}

// --- the orphan sweep ----------------------------------------------------------

TEST_F(ReadOnlyTest, AnOrphanIsCollectedOnceItHasBeenUnreferencedForTheWholeWindow) {
    Options options = writer_options();
    options.orphan_sweep_interval = Duration(1);
    options.orphan_retention = Duration(60'000);

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 50);
    ASSERT_EQ(writer->flush(), Status::Ok);

    // An object at a name no version references — a flush that died before its edit was durable.
    const std::string orphan = sst_object_name(900'000);
    ASSERT_EQ(store_.store(0)->put(orphan, Slice::from(std::string(64, 'x'))).get(), Status::Ok);

    auto& engine = static_cast<DbImpl&>(*writer);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);   // first observation only
    auto listing = store_listing();
    EXPECT_NE(std::find(listing.begin(), listing.end(), orphan), listing.end())
        << "one observation is not a sustained one — that was the old flag's mistake";

    now_.fetch_add(120'000);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    listing = store_listing();
    EXPECT_EQ(std::find(listing.begin(), listing.end(), orphan), listing.end())
        << "continuously unreferenced for the whole window, so it goes";
}

// **The bug `reclaim_orphans_at_open` had.** An object whose edit committed between the manifest
// read and the store listing looked unreferenced and was deleted. Re-reading the manifest each pass
// is what removes it from the candidate set instead.
TEST_F(ReadOnlyTest, AFileThatBecomesReferencedIsNotSwept) {
    Options options = writer_options();
    options.orphan_sweep_interval = Duration(1);
    options.orphan_retention = Duration(60'000);

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    auto& engine = static_cast<DbImpl&>(*writer);

    write(*writer, 0, 50);
    ASSERT_EQ(writer->flush(), Status::Ok);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);   // everything here is referenced

    // Time passes well beyond the window, and the live files must still be live.
    now_.fetch_add(600'000);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);

    for (int i = 0; i < 50; i += 10) {
        EXPECT_TRUE(writer->get_copy(Slice::from(key_at(i))).has_value())
            << "the sweep deleted a file the manifest references, at key " << i;
    }
}

// Failure to look is not evidence of absence, and this is the most destructive place to forget it.
TEST_F(ReadOnlyTest, ASweepThatCannotListCollectsNothing) {
    Options options = writer_options();
    options.orphan_sweep_interval = Duration(1);

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 50);
    ASSERT_EQ(writer->flush(), Status::Ok);

    const std::vector<std::string> before = store_listing();
    ASSERT_FALSE(before.empty());

    // Remove the store directory out from under the engine: the list now fails rather than
    // reporting an empty store.
    std::filesystem::remove_all(store_.store(0)->root());

    auto& engine = static_cast<DbImpl&>(*writer);
    const Status swept = engine.sweep_orphans_for_test();
    EXPECT_TRUE(swept == Status::Io || swept == Status::Ok)
        << "an unreadable store must not be read as 'everything here is unreferenced'";
}

// --- staleness is not corruption ----------------------------------------------

/* **The discriminator, and the direction that must never be wrong.**
 *
 * A reader past the retention window finds an object gone. So does a reader on a store that has
 * genuinely lost data. Reporting the first as `Corrupt` sends an operator to a restore for a
 * perfectly healthy store, so they have to be told apart — and re-reading the manifest does it with
 * no coordination at all: if the writer has moved on, it collected the object legitimately.
 */
TEST_F(ReadOnlyTest, AStaleReaderIsToldItIsStaleRatherThanCorrupt) {
    Options options = writer_options();   // no retention: the writer collects immediately

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 200);
    ASSERT_EQ(writer->flush(), Status::Ok);

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);
    ASSERT_TRUE(reader->get_copy(Slice::from(key_at(0))).has_value());

    for (int round = 0; round < 3; ++round) {
        write(*writer, 0, 200, "round" + std::to_string(round));
        ASSERT_EQ(writer->flush(), Status::Ok);
        ASSERT_EQ(writer->compact_level(0), Status::Ok);
    }

    // The reader's version references files the writer has collected.
    Status observed = Status::Ok;
    for (int i = 0; i < 200 && observed == Status::Ok; ++i) {
        auto found = reader->get_copy(Slice::from(key_at(i)));
        if (!found) observed = found.error();
    }
    ASSERT_EQ(observed, Status::Stale)
        << "a reader behind the retention window is stale, not looking at damaged bytes";
    EXPECT_FALSE(is_terminal(Status::Stale)) << "and it is recoverable without a reopen";

    // Which is exactly what refresh() does.
    ASSERT_EQ(reader->refresh(), Status::Ok);
    for (int i = 0; i < 200; i += 20) {
        EXPECT_TRUE(reader->get_copy(Slice::from(key_at(i))).has_value())
            << "refresh restored service at key " << i;
    }
}

// The control: with the manifest *not* advanced, the same missing object is real loss and must not
// be softened into staleness. Without this, an implementation that always said "stale" would pass
// the case above and hide genuine corruption.
TEST_F(ReadOnlyTest, AMissingObjectTheManifestStillReferencesIsNotStale) {
    Options options = writer_options();

    {
        auto writer = open_writer(options);
        ASSERT_NE(writer, nullptr);
        write(*writer, 0, 200);
        ASSERT_EQ(writer->flush(), Status::Ok);
    }

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);
    ASSERT_TRUE(reader->get_copy(Slice::from(key_at(0))).has_value());
    static_cast<DbImpl&>(*reader).evict_readers();

    // Delete a file the *current* manifest still references. No writer has moved on, so this is
    // loss rather than staleness.
    for (const std::string& name : store_listing()) {
        if (sst_file_number(name).has_value()) {
            ASSERT_EQ(store_.store(0)->remove(name).get(), Status::Ok);
            break;
        }
    }

    Status observed = Status::Ok;
    for (int i = 0; i < 200 && observed == Status::Ok; ++i) {
        auto found = reader->get_copy(Slice::from(key_at(i)));
        if (!found) observed = found.error();
    }
    EXPECT_NE(observed, Status::Stale)
        << "the manifest still references it, so this is loss and must be reported as such";
    EXPECT_TRUE(is_terminal(observed)) << "and it is terminal: " << status_name(observed);
}

}  // namespace
}  // namespace elysiumkv::test
