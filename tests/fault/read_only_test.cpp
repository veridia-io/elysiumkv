/* Multiple readers, and the one hazard that stops it working by itself.
 *
 * Most of it already worked: a clean open writes nothing, objects are immutable and write-once so a
 * cached block can never become wrong, and a reader performs no compare-and-set so it is outside the
 * ownership protocol entirely. What did not work is that the writer's collector decides an object is
 * collectible when no live `Version` in its own process references it — a reader elsewhere is
 * invisible to it. `Options::obsolete_retention` is the whole fix, and the third case below is the
 * one that would fail without it.
 *
 * The other half is the orphan sweep, which replaces `reclaim_orphans_at_open`. That flag decided on
 * a *single instantaneous* observation, which cannot tell a dead writer's residue from a live
 * writer's just-committed file. The sweep decides on a *sustained* one, re-reading the manifest each
 * pass so a file whose edit has since landed leaves the candidate set on its own.
 */

#include "db/db_impl.hpp"

#include "diff/oracle.hpp"
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
#include <set>
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

    /// A transient hot tier over a durable one, so a wipe of store 0 is a recoverable loss for a
    /// writer and an unserveable one for a reader.
    Options transient_writer_options() {
        Options options = make_transient_options(store_, Duration(60'000), Duration(120'000));
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_.load(); };
        return options;
    }

    /// Wipes a store the way a replaced volume does: the directory is still there and empty.
    void wipe(const std::shared_ptr<DiskBlobStore>& store) {
        auto names = store->list("").get();
        ASSERT_TRUE(names.has_value());
        for (const std::string& name : *names) {
            ASSERT_EQ(store->remove(name).get(), Status::Ok);
        }
    }

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

// The property the whole design rests on. If a reader writes anything, it can fail to write, be
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

    // The half that would be missing from an auto-refreshing implementation. Without it, a
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

// The case the retention window exists for, and the one that fails without it: the writer
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

// The bug `reclaim_orphans_at_open` had. An object whose edit committed between the manifest
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

/* The discriminator, and the direction that must never be wrong.
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

// --- the reader is outside the ownership protocol ------------------------------

// The claim the whole design rests on. A reader performs no compare-and-set, so it cannot take
// ownership from a writer however many of them come and go.
TEST_F(ReadOnlyTest, ReadersNeverFenceTheWriter) {
    Options options = writer_options();
    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 20);
    ASSERT_EQ(writer->flush(), Status::Ok);

    for (int round = 0; round < 10; ++round) {
        auto reader = DB::open_read_only(options);
        ASSERT_TRUE(reader.has_value()) << status_name(reader.error());
        EXPECT_TRUE((*reader)->get_copy(Slice::from(key_at(0))).has_value());
        EXPECT_EQ((*reader)->refresh(), Status::Ok);

        // The writer keeps working while a reader is open, and again after it closes.
        write(*writer, 100 * (round + 1), 20, "round" + std::to_string(round));
        ASSERT_EQ(writer->flush(), Status::Ok)
            << "a reader took ownership from the writer at round " << round;
        reader->reset();
        ASSERT_EQ(writer->compact_level(0), Status::Ok);
    }
    EXPECT_FALSE(static_cast<DbImpl&>(*writer).current_version()->all_files().empty());
}

// No registration means no roll to grow and nothing to cap, so this asserts the *absence* of a
// limit rather than a particular number.
TEST_F(ReadOnlyTest, ManyReadersAndAWriterAllMakeProgress) {
    Options options = writer_options();
    options.obsolete_retention = Duration(600'000);
    options.orphan_retention = Duration(600'000);

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    write(*writer, 0, 100);
    ASSERT_EQ(writer->flush(), Status::Ok);

    std::vector<std::unique_ptr<ReadOnlyDB>> readers;
    for (int i = 0; i < 16; ++i) {
        auto reader = DB::open_read_only(options);
        ASSERT_TRUE(reader.has_value()) << "reader " << i << ": " << status_name(reader.error());
        readers.push_back(std::move(*reader));
    }

    write(*writer, 1000, 100, "later");
    ASSERT_EQ(writer->flush(), Status::Ok);
    ASSERT_EQ(writer->compact_level(0), Status::Ok);

    for (size_t i = 0; i < readers.size(); ++i) {
        EXPECT_TRUE(readers[i]->get_copy(Slice::from(key_at(0))).has_value()) << "reader " << i;
        ASSERT_EQ(readers[i]->refresh(), Status::Ok) << "reader " << i;
        EXPECT_TRUE(readers[i]->get_copy(Slice::from(key_at(1000))).has_value()) << "reader " << i;
    }
}

// A reader cannot repair a damaged store — the discard is a manifest write — and serving the version
// unrepaired would be worse than refusing: dropping newer files uncovers older values, so reads
// would return *stale* data presented as current.
TEST_F(ReadOnlyTest, AReaderRefusesAStoreWhoseTransientTierLostFiles) {
    Options options = transient_writer_options();
    {
        // `DB::open` refuses a transient configuration outright — a check, not a precondition — so
        // a writer that intends the exposure asks for it explicitly.
        auto opened = DB::open_with_result(options);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        auto writer = std::move(opened->db);
        write(*writer, 0, 60);
        ASSERT_EQ(writer->flush(), Status::Ok);
    }

    wipe(store_.store(0));
    const std::map<std::string, std::string> manifest_before = manifest_snapshot();

    auto reader = DB::open_read_only(options);
    ASSERT_FALSE(reader.has_value()) << "a reader must not serve a version with holes";
    EXPECT_TRUE(is_terminal(reader.error())) << status_name(reader.error());
    EXPECT_EQ(manifest_snapshot(), manifest_before) << "and it must not have repaired anything";

    // The control: a *writer* opening the same store performs the discard, which is what the reader
    // was declining to do on its behalf.
    auto repaired = DB::open_with_result(options);
    ASSERT_TRUE(repaired.has_value()) << status_name(repaired.error());
    EXPECT_FALSE(repaired->discarded_stores.empty());
}

// --- the two windows do not undercut each other -------------------------------

/* The interaction the two-knob split introduced. An obsolete object is, to the sweep,
 * indistinguishable from an orphan: the edit that removed it is committed, so the current manifest
 * does not reference it, which is the sweep's own test. What keeps the sweep off it is the
 * pending-deletion queue — those objects have an exact unreferenced-since time and a window of their
 * own — and without that exclusion the sweep would delete files the reader window is still
 * protecting.
 */
TEST_F(ReadOnlyTest, TheSweepDoesNotCollectWhatTheReaderWindowIsStillProtecting) {
    Options options = writer_options();
    options.obsolete_retention = Duration(600'000);
    options.orphan_retention = Duration(600'000);
    options.orphan_sweep_interval = Duration(1);

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);
    auto& engine = static_cast<DbImpl&>(*writer);

    write(*writer, 0, 200);
    ASSERT_EQ(writer->flush(), Status::Ok);
    const std::vector<std::string> before = store_listing();

    // Compaction obsoletes the inputs; the reader window keeps them on disk.
    ASSERT_EQ(writer->compact_level(0), Status::Ok);
    ASSERT_GT(engine.pending_deletions(), 0u) << "the retention window is what holds them";

    // Well past the *orphan* window, which is the trap: to the sweep these look unreferenced, and
    // only the queue exclusion stops it acting on that.
    now_.fetch_add(1'200'000);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);

    const std::vector<std::string> after = store_listing();
    for (const std::string& name : before) {
        EXPECT_NE(std::find(after.begin(), after.end(), name), after.end())
            << "the sweep took " << name << ", which the reader window was still protecting";
    }
}

// The sweep re-reads the manifest, so a pointer that moved under it means another writer owns the
// store. That re-read is the same thing that keeps a committed file from being swept, so it is worth
// asserting it detects the case it is also named for.
TEST_F(ReadOnlyTest, TheSweepReportsFencedWhenAnotherWriterHasMovedThePointer) {
    Options options = writer_options();
    options.orphan_sweep_interval = Duration(1);
    // Roll on almost every edit, so the second writer moves the pointer quickly.
    options.manifest_edits_per_generation = 1;

    auto first = open_writer(options);
    ASSERT_NE(first, nullptr);
    write(*first, 0, 20);
    ASSERT_EQ(first->flush(), Status::Ok);
    ASSERT_EQ(static_cast<DbImpl&>(*first).sweep_orphans_for_test(), Status::Ok)
        << "nothing has moved yet";

    // A second writer opens the same store — open takes no lock, which is the window the fence
    // exists to close later — and rolls the generation out from under the first.
    auto second = open_writer(options);
    ASSERT_NE(second, nullptr);
    write(*second, 1000, 20, "second");
    ASSERT_EQ(second->flush(), Status::Ok);

    EXPECT_EQ(static_cast<DbImpl&>(*first).sweep_orphans_for_test(), Status::Fenced)
        << "the manifest moved under this writer and the sweep is where it noticed";
}

// --- the strongest property available --------------------------------------

/* Every state a reader observes is one the writer actually published.
 *
 * Note what this is *not*: "the reader matches the oracle". It legitimately lags — that is the whole
 * point of `refresh()` being explicit — so equality with the current oracle would be the wrong
 * assertion and would fail for a correct implementation. What must hold is that the reader never
 * observes a state the writer never had: no mixture of two versions, no half-installed edit, no
 * file from one version paired with a file from another.
 *
 * That is what a torn version install would look like, and it is the one failure the case-by-case
 * tests above could not catch — each of them fixes the writer's state before looking.
 */
TEST_F(ReadOnlyTest, EveryStateAReaderObservesIsOneTheWriterPublished) {
    Options options = writer_options();
    options.obsolete_retention = Duration(600'000);
    options.orphan_retention = Duration(600'000);

    auto writer = open_writer(options);
    ASSERT_NE(writer, nullptr);

    Oracle oracle;
    const auto snapshot_of = [](const Oracle& o) {
        std::string image;
        for (const auto& [key, value] : o.entries()) {
            image += key;
            image.push_back('\0');
            image += value;
            image.push_back('\1');
        }
        return image;
    };

    // Seed and publish, so the reader has something to open against.
    for (int i = 0; i < 40; ++i) {
        const std::string key = key_at(i);
        ASSERT_EQ(writer->put(Slice::from(key), Slice::from(std::string("seed"))), Status::Ok);
        oracle.put(key, "seed");
    }
    ASSERT_EQ(writer->flush(), Status::Ok);

    std::set<std::string> published{snapshot_of(oracle)};

    auto opened = DB::open_read_only(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    std::unique_ptr<ReadOnlyDB> reader = std::move(*opened);

    const auto reader_state = [&reader] {
        std::string image;
        auto it = reader->iterator();
        while (it->next()) {
            image += it->key().to_string();
            image.push_back('\0');
            image += it->value().to_string();
            image.push_back('\1');
        }
        EXPECT_EQ(it->status(), Status::Ok);
        return image;
    };

    // A deterministic pseudo-random stream, so a failure reproduces from the seed alone.
    uint64_t rng = 0x9E3779B97F4A7C15ull;
    const auto next = [&rng] {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        return rng;
    };

    for (int round = 0; round < 60; ++round) {
        const int ops = static_cast<int>(next() % 20) + 1;
        for (int i = 0; i < ops; ++i) {
            const std::string key = key_at(static_cast<int>(next() % 120));
            if (next() % 4 == 0) {
                ASSERT_EQ(writer->remove(Slice::from(key)), Status::Ok);
                oracle.remove(key);
            } else {
                const std::string value = "r" + std::to_string(round) + "-" + std::to_string(i);
                ASSERT_EQ(writer->put(Slice::from(key), Slice::from(value)), Status::Ok);
                oracle.put(key, value);
            }
        }

        // The reader must not see writes that are only in the memtable, so it is checked *before*
        // the publish as well as after.
        EXPECT_TRUE(published.count(reader_state()) != 0)
            << "round " << round << ": the reader observed a state the writer never published";

        ASSERT_EQ(writer->flush(), Status::Ok);
        if (round % 5 == 0) {
            ASSERT_EQ(writer->compact_level(0), Status::Ok);
        }
        published.insert(snapshot_of(oracle));

        ASSERT_EQ(reader->refresh(), Status::Ok);
        const std::string observed = reader_state();
        EXPECT_TRUE(published.count(observed) != 0)
            << "round " << round << ": after refresh the reader observed a state the writer never "
               "published — a version install was torn, or two versions were mixed";
        EXPECT_EQ(observed, snapshot_of(oracle))
            << "round " << round << ": a refresh should install the newest published version";
    }

    // And the control for the whole construction: a reader that never refreshed would be stuck on
    // the seed state, so the assertions above would be vacuous if `published` grew to hold
    // everything trivially. It does not — every entry came from an actual publish.
    EXPECT_GT(published.size(), 30u) << "the writer published many distinct states";
}

}  // namespace
}  // namespace elysiumkv::test
