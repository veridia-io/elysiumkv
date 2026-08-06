#include "diff/oracle.hpp"
#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "db/db_impl.hpp"

#include "elysiumkv/db.hpp"
#include "elysiumkv/file_manifest_catalog.hpp"
#include "elysiumkv/local_file_blob_store.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

using Op = FaultInjectingBlobStore::Op;

/// A store whose SST traffic runs through the fault injector, while the manifest
/// catalog stays on plain files.
class DbFaultTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directories(dir_.path() / "store");
        local_ = std::make_shared<LocalFileBlobStore>(dir_.path() / "store", "store-a");
        local_->set_sync_writes(false);
        faulty_ = std::make_shared<FaultInjectingBlobStore>(local_);
        catalog_ = std::make_shared<FileManifestCatalog>(dir_.path());
    }

    mutable std::atomic<uint64_t> now_{1'000'000};

    Options options() const {
        Options options;
        options.manifest_catalog = catalog_;
        // Injectable, so the sweep's retention can be crossed without waiting for it.
        options.clock = [this] { return now_.load(); };
        // Large enough that nothing auto-flushes: every one of these cases is
        // about what happens at a flush the test asks for. (The arena allocates
        // in 4 KiB blocks, so a memtable budget near that size would freeze on
        // essentially every write.)
        options.memtable_bytes = 256u << 10;
        options.block_bytes = 512;

        LevelOptions l0;
        l0.max_files = 100;  // no compaction pressure: these cases are about I/O
        LevelOptions l1;
        options.levels = {{0, l0}, {1, l1}};
        options.tiers = {Tier{.store = faulty_, .durability = Durability::Durable}};
        return options;
    }

    std::unique_ptr<DB> open() {
        auto opened = DB::open(options());
        EXPECT_TRUE(opened.has_value())
            << (opened.has_value() ? "" : status_name(opened.error()));
        return opened.has_value() ? std::move(*opened) : nullptr;
    }

    static std::string key_at(int i) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "key:%06d", i);
        return buf;
    }

    void fill(DB& db, Oracle& oracle, int count, const std::string& tag) {
        for (int i = 0; i < count; ++i) {
            const std::string key = key_at(i);
            const std::string value = tag + "-" + std::to_string(i);
            ASSERT_EQ(db.put(Slice::from(key), Slice::from(value)), Status::Ok);
            oracle.put(key, value);
        }
    }

    void expect_matches(DB& db, const Oracle& oracle) {
        std::vector<std::pair<std::string, std::string>> observed;
        auto it = db.iterator();
        while (it->next()) observed.emplace_back(it->key().to_string(), it->value().to_string());
        ASSERT_EQ(it->status(), Status::Ok);

        std::vector<std::pair<std::string, std::string>> expected;
        for (const auto& entry : oracle.entries()) expected.push_back(entry);
        EXPECT_EQ(observed, expected);
    }

    TempDir dir_;
    std::shared_ptr<LocalFileBlobStore> local_;
    std::shared_ptr<FaultInjectingBlobStore> faulty_;
    std::shared_ptr<ManifestCatalog> catalog_;
};

// ARCHITECTURE.md "Positional recency" — on crash, all writes since the last SST flush are lost — and everything
// before it comes back exactly.
TEST_F(DbFaultTest, KillMidStreamLosesOnlyTheUnflushedMemtable) {
    Oracle flushed;
    {
        auto db = open();
        ASSERT_NE(db, nullptr);
        fill(*db, flushed, 200, "durable");
        ASSERT_EQ(db->flush(), Status::Ok);

        // Written after the flush: the embedder restores these, not ElysiumKV.
        Oracle lost = flushed;
        fill(*db, lost, 50, "volatile");
        // No flush, no close: the process simply stops existing.
        db.reset();
    }

    auto reopened = open();
    ASSERT_NE(reopened, nullptr);
    expect_matches(*reopened, flushed);
}

// A flush that cannot write its object must not acknowledge, must not lose the
// memtable, and must succeed once the store recovers.
TEST_F(DbFaultTest, AFailedFlushIsRetriedRatherThanDropped) {
    auto db = open();
    ASSERT_NE(db, nullptr);

    Oracle oracle;
    fill(*db, oracle, 100, "v1");

    faulty_->add_rule({.op = Op::Put, .status = Status::Io});
    EXPECT_NE(db->flush(), Status::Ok) << "a flush that could not write must say so";

    faulty_->clear_rules();
    EXPECT_EQ(db->flush(), Status::Ok);
    expect_matches(*db, oracle);

    db.reset();
    auto reopened = open();
    ASSERT_NE(reopened, nullptr);
    // The retried flush is what made these durable.
    expect_matches(*reopened, oracle);
}

// ARCHITECTURE.md "Fault injection" — a blob put that fails after a partial write must leave no corrupt
// version visible. The fragment is an orphan; the version never referenced it.
TEST_F(DbFaultTest, ATornObjectNeverEntersAVersion) {
    auto db = open();
    ASSERT_NE(db, nullptr);

    Oracle oracle;
    fill(*db, oracle, 100, "v1");
    ASSERT_EQ(db->flush(), Status::Ok);

    Oracle after_torn = oracle;
    fill(*db, after_torn, 100, "v2");

    faulty_->add_rule({.op = Op::Put, .status = Status::Io, .torn_write = true, .torn_bytes = 64});
    EXPECT_NE(db->flush(), Status::Ok);
    faulty_->clear_rules();

    db.reset();
    auto reopened = open();
    ASSERT_NE(reopened, nullptr);
    // The torn object exists in the store but no version names it, so the store
    // reads as it did before the failed flush.
    expect_matches(*reopened, oracle);
}

// ARCHITECTURE.md "Immutable named objects" — Status::Io means "ask again later". A read that hits it must be
// retryable, never reported as absence.
TEST_F(DbFaultTest, ATransientReadFailureIsRetryableAndNotAbsence) {
    auto db = open();
    ASSERT_NE(db, nullptr);

    Oracle oracle;
    fill(*db, oracle, 200, "v1");
    ASSERT_EQ(db->flush(), Status::Ok);

    faulty_->add_rule({.op = Op::Get, .first_match = 0, .match_count = 1, .status = Status::Io});

    bool saw_io = false;
    for (int i = 0; i < 200 && !saw_io; ++i) {
        auto found = db->get(Slice::from(key_at(i)));
        if (!found.has_value()) {
            EXPECT_EQ(found.error(), Status::Io)
                << "an unreadable file is not an absent key";
            saw_io = true;
        }
    }
    EXPECT_TRUE(saw_io);

    faulty_->clear_rules();
    // The data was there the whole time; only the read failed.
    expect_matches(*db, oracle);
}

// A byte flipped inside an SST is caught by CRC and surfaces as Corrupt — never
// silently returned as a value.
TEST_F(DbFaultTest, ACorruptedBlockIsDetectedOnRead) {
    auto db = open();
    ASSERT_NE(db, nullptr);

    Oracle oracle;
    fill(*db, oracle, 300, "v1");
    ASSERT_EQ(db->flush(), Status::Ok);
    db.reset();

    // Damage the flushed object directly.
    std::vector<std::filesystem::path> ssts;
    for (const auto& entry : std::filesystem::directory_iterator(dir_.path() / "store")) {
        if (entry.path().extension() == ".sst") ssts.push_back(entry.path());
    }
    ASSERT_FALSE(ssts.empty());
    {
        std::fstream file(ssts.front(), std::ios::in | std::ios::out | std::ios::binary);
        file.seekp(32);
        file.put('\xAA');
    }

    auto reopened = open();
    ASSERT_NE(reopened, nullptr);

    bool saw_corrupt = false;
    for (int i = 0; i < 300; ++i) {
        auto found = reopened->get(Slice::from(key_at(i)));
        if (!found.has_value() && found.error() == Status::Corrupt) saw_corrupt = true;
        if (found.has_value()) {
            // Whatever comes back must be a value that was actually written.
            EXPECT_EQ(found->value().to_string(), *oracle.get(key_at(i)));
        }
    }
    EXPECT_TRUE(saw_corrupt) << "a flipped byte must not read as valid data";
}

// ARCHITECTURE.md "Open and recovery" — replay stops at the first edit that fails to decode. The store then
// describes the last complete version — not a partially applied one.
TEST_F(DbFaultTest, ATruncatedManifestEditLeavesTheLastCompleteVersion) {
    Oracle first_batch;
    {
        auto db = open();
        ASSERT_NE(db, nullptr);
        fill(*db, first_batch, 100, "v1");
        ASSERT_EQ(db->flush(), Status::Ok);

        Oracle second_batch = first_batch;
        fill(*db, second_batch, 100, "v2");
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    // Truncate the last edit, as a half-written record would appear.
    std::vector<std::filesystem::path> edits;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(dir_.path() / "manifest")) {
        if (entry.path().filename().string().starts_with("edit-")) edits.push_back(entry.path());
    }
    ASSERT_GE(edits.size(), 2u);
    std::sort(edits.begin(), edits.end());
    std::filesystem::resize_file(edits.back(), 4);

    auto reopened = open();
    ASSERT_NE(reopened, nullptr);
    expect_matches(*reopened, first_batch);
}

// ARCHITECTURE.md "Open and recovery" — a store that cannot answer is not a store that lost data. Open fails
// retryably — no discard, no manifest write — and succeeds once it can answer.
TEST_F(DbFaultTest, AnUnreadableStoreFailsOpenRetryably) {
    Oracle oracle;
    {
        auto db = open();
        ASSERT_NE(db, nullptr);
        fill(*db, oracle, 100, "v1");
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    faulty_->set_unreachable(true);
    auto failed = DB::open(options());
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), Status::Io)
        << "an unreachable store must never look like an empty one";

    faulty_->set_unreachable(false);
    auto db = open();
    ASSERT_NE(db, nullptr);
    // The data was there the whole time; only the listing failed.
    expect_matches(*db, oracle);
}

// ARCHITECTURE.md "The ABI boundary" — **obsolete-object collection reaches the store as one bulk call.** The
// unit test proves the version set batches; this proves the engine's side of it, at
// the interface an S3 store actually implements. Against a remote store the
// per-file shape was one HTTP round trip per obsolete object after every
// compaction, so the count is the property, not the deletions.
TEST_F(DbFaultTest, CollectingObsoleteObjectsIsOneBulkCall) {
    Options with_compaction = options();
    // Four L0 files trigger a compaction, and its inputs become obsolete together.
    LevelOptions l0;
    l0.max_files = 4;
    with_compaction.levels = {{0, l0}, {1, LevelOptions{}}};
    with_compaction.memtable_bytes = 8u << 10;

    auto opened = DB::open(with_compaction);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);

    Oracle oracle;
    for (int round = 0; round < 6; ++round) {
        fill(*db, oracle, 60, "v" + std::to_string(round));
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    ASSERT_EQ(db->compact_level(0), Status::Ok);

    const uint64_t bulk = faulty_->call_count(Op::RemoveMany);
    const uint64_t single = faulty_->call_count(Op::Remove);
    ASSERT_GT(single, 0u) << "nothing was collected, so this proves nothing";
    EXPECT_GT(bulk, 0u) << "collection went straight to per-object removes";
    // The fault store's remove_many loops over its own remove, so `single` counts
    // objects and `bulk` counts calls. One call carrying several objects is the
    // whole point; one call per object would make them equal.
    EXPECT_LT(bulk, single)
        << bulk << " bulk calls for " << single
        << " objects — collection is still one round trip per file";

    expect_matches(*db, oracle);
}

// ARCHITECTURE.md "The manifest is snapshots plus edits", ARCHITECTURE.md "Open and recovery" — **an orphan from a crashed write must not make the store
// unwritable**, and it is `collect_orphans` at open that ensures it.
//
// The hazard is worth spelling out because it is not obvious that anything saves
// you. A crash between an SST `put` and the manifest edit recording it leaves an
// object no edit mentions; the edit never landed, so recovery restores
// `next_file_number` to the value that object already used, and the next flush
// would put at a name that exists. Write-once refuses that — correctly, and now
// with `Status::Fenced` — and the counter returns to the same number on every
// subsequent open, so the store would be unwritable forever.
//
// Open collecting unreferenced objects is what breaks that cycle, and this had no
// test. The object planted here is exactly what such a crash leaves behind.
TEST_F(DbFaultTest, AnOrphanFromACrashedWriteDoesNotBlockTheNextFlush) {
    Oracle oracle;
    {
        auto db = open();
        ASSERT_NE(db, nullptr);
        fill(*db, oracle, 50, "v1");
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    // File 1 is recorded; 2 is the number the next flush will allocate. Plant it,
    // unrecorded, as a crashed writer would have left it.
    ASSERT_EQ(local_->put("000000000002.sst", Slice::from(std::string_view("half an sst"))).get(),
              Status::Ok);

    auto db = open();
    ASSERT_NE(db, nullptr);
    fill(*db, oracle, 50, "v2");
    EXPECT_EQ(db->flush(), Status::Ok)
        << "a flush must route around an orphan, not collide with it forever";
    expect_matches(*db, oracle);

    // And it survives another round trip, which is what "not forever" means.
    db.reset();
    auto reopened = open();
    ASSERT_NE(reopened, nullptr);
    fill(*reopened, oracle, 50, "v3");
    EXPECT_EQ(reopened->flush(), Status::Ok);
    expect_matches(*reopened, oracle);
}

// ARCHITECTURE.md "Immutable named objects" — **a taken object name is survived by renumbering, not by failing.** The contract says
// so outright: "a failed `put` must not be retried under the same name — allocate a new file
// number instead". This used to report `Fenced`, which made a crashed writer's leftover object
// permanently fatal, since recovery hands its number straight back out.
//
// Ownership is arbitrated at the manifest, the only place with a compare-and-swap; see
// `VersionSetTest`'s two fencing cases and the Java `theSecondWriterFencesTheFirst`.
TEST_F(DbFaultTest, ATakenSstNameIsSurvivedByRenumbering) {
    Oracle oracle;
    auto db = open();
    ASSERT_NE(db, nullptr);
    fill(*db, oracle, 50, "v1");
    ASSERT_EQ(db->flush(), Status::Ok);

    // Take the name this instance will allocate next, *after* it has opened, so the counter
    // advance at open cannot have stepped over it.
    ASSERT_EQ(local_->put("000000000002.sst", Slice::from(std::string_view("theirs"))).get(),
              Status::Ok);

    fill(*db, oracle, 50, "v2");
    EXPECT_EQ(db->flush(), Status::Ok) << "the flush must route around the taken name";
    expect_matches(*db, oracle);

    // On a fresh number, and the squatter untouched.
    auto names = local_->list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_NE(std::find(names->begin(), names->end(), "000000000002.sst"), names->end())
        << "and must not have overwritten or removed what was there";
    EXPECT_GE(names->size(), 3u) << "file 1, the squatter, and the flush's own output";
}

// **The bug this all exists for.** Open takes no lock and performs no compare-and-set, so it
// cannot know it owns the store — and the version it recovers and the listing it takes are two
// different points in time, so a file whose edit became durable in between is present in the
// listing and absent from the version. Deleting it destroyed committed data whenever two
// processes overlapped on one store: silently, surfacing much later as a vanished file.
TEST_F(DbFaultTest, OpenLeavesAnotherWritersObjectsAlone) {
    Oracle oracle;
    {
        auto db = open();
        ASSERT_NE(db, nullptr);
        fill(*db, oracle, 50, "v1");
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    // Standing in for a concurrent writer's file: present in the store, absent from the
    // manifest this instance will read.
    ASSERT_EQ(local_->put("000000004242.sst", Slice::from(std::string_view("not yours"))).get(),
              Status::Ok);

    auto db = open();
    ASSERT_NE(db, nullptr);
    auto names = local_->list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_NE(std::find(names->begin(), names->end(), "000000004242.sst"), names->end())
        << "open must not delete an object it cannot prove is abandoned";
    expect_matches(*db, oracle);
}

// The same store, opened by something that *asserts* exclusivity. The capability still works —
// which is what makes the case above a decision rather than an omission.
// Reclamation still exists — it moved. Deleting at open rested on a *single instantaneous*
// observation, which cannot tell a dead writer's residue from a live writer's just-committed file;
// the sweep rests on a sustained one. So this asserts the capability, on the trigger it now has.
TEST_F(DbFaultTest, ReclamationHappensOnTheSweepRatherThanAtOpen) {
    Options reclaiming = options();
    reclaiming.orphan_sweep_interval = Duration(1);
    reclaiming.orphan_retention = Duration(60'000);

    Oracle oracle;
    auto opened = DB::open(reclaiming);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);
    fill(*db, oracle, 50, "v1");
    ASSERT_EQ(db->flush(), Status::Ok);

    ASSERT_EQ(local_->put("000000004242.sst", Slice::from(std::string_view("residue"))).get(),
              Status::Ok);

    auto& engine = static_cast<DbImpl&>(*db);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    auto names = local_->list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_NE(std::find(names->begin(), names->end(), "000000004242.sst"), names->end())
        << "one observation is not a sustained one";

    now_.fetch_add(120'000);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    names = local_->list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_EQ(std::find(names->begin(), names->end(), "000000004242.sst"), names->end())
        << "continuously unreferenced for the whole window, so it goes";
    expect_matches(*db, oracle);
}

// The mechanism that replaces deletion: open steps the file-number counter over whatever the
// stores already hold, so residue at the number recovery would hand back out is simply skipped.
TEST_F(DbFaultTest, OpenStepsTheFileNumberCounterOverExistingObjects) {
    Oracle oracle;
    {
        auto db = open();
        ASSERT_NE(db, nullptr);
        fill(*db, oracle, 50, "v1");
        ASSERT_EQ(db->flush(), Status::Ok);   // file 1
    }

    // Numbers 2..5 taken, as a crashed writer's residue would leave them.
    for (int number = 2; number <= 5; ++number) {
        char name[32];
        std::snprintf(name, sizeof(name), "%012d.sst", number);
        ASSERT_EQ(local_->put(name, Slice::from(std::string_view("residue"))).get(), Status::Ok);
    }

    auto db = open();
    ASSERT_NE(db, nullptr);
    fill(*db, oracle, 50, "v2");
    ASSERT_EQ(db->flush(), Status::Ok);

    // The new file is above every planted number, so the flush never had to collide at all.
    // Read off the store rather than the engine: the names are the observable fact.
    auto names = local_->list("").get();
    ASSERT_TRUE(names.has_value());
    std::string highest;
    for (const std::string& name : *names) highest = std::max(highest, name);
    EXPECT_GT(highest, std::string("000000000005.sst"))
        << "the counter must step over what the store already held; highest name was " << highest;
    expect_matches(*db, oracle);
}

// ARCHITECTURE.md "A process-wide memory budget" — **the reader cache is bounded end to end, and correctness does not depend on
// the bound.** This was the one cache in the engine with neither a limit nor a
// statistic: each reader holds its file's bloom filter, ~1.25 MB at 10 bits per key for
// a million-entry file, so a store with a thousand files held over a gigabyte
// unaccounted.
TEST_F(DbFaultTest, TheReaderCacheIsBoundedAndReadsStayCorrectAcrossEviction) {
    Options generous = options();
    generous.memtable_bytes = 8u << 10;  // many small files, so many readers
    generous.block_bytes = 256;

    // **Measured, not guessed.** The first draft picked an 8 KiB bound and asserted
    // that not every reader would fit; every reader fitted, because these files are
    // small. The per-reader cost is a property of the data, so read it off the engine
    // and derive the bound from it.
    size_t per_reader = 0;
    int file_count = 0;
    Oracle oracle;
    {
        auto opened = DB::open(generous);
        ASSERT_TRUE(opened.has_value());
        auto db = std::move(*opened);
        for (int round = 0; round < 8; ++round) {
            fill(*db, oracle, 60, "v" + std::to_string(round));
            ASSERT_EQ(db->flush(), Status::Ok);
        }
        expect_matches(*db, oracle);

        const Stats stats = db->stats();
        ASSERT_GT(stats.open_readers, 2u) << "not enough readers to say anything about a bound";
        per_reader = stats.reader_cache_bytes / stats.open_readers;
        file_count = stats.levels[0].file_count;
        ASSERT_GT(per_reader, 0u);
    }

    Options bounded = generous;
    bounded.reader_cache_bytes = per_reader * 2 + per_reader / 2;  // room for two

    auto opened = DB::open(bounded);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);

    // Every key still reads correctly with a cache far too small to hold every reader —
    // the point being that eviction costs a reopen and nothing else.
    expect_matches(*db, oracle);

    const Stats after = db->stats();
    EXPECT_LE(after.reader_cache_bytes, bounded.reader_cache_bytes) << "the bound is a bound";
    EXPECT_LT(after.open_readers, static_cast<uint64_t>(file_count))
        << after.open_readers << " readers resident out of " << file_count
        << " files, with room for two";
    EXPECT_GT(after.reader_cache_misses, 0u);

    // An iterator holds its readers for its whole life (ARCHITECTURE.md "Versions are immutable snapshots"). Eviction while it runs must
    // be invisible to it: the cache's reference is not the only one.
    auto it = db->iterator();
    ASSERT_NE(it, nullptr);
    int seen = 0;
    while (it->next()) {
        ++seen;
        // Point lookups interleaved with the scan, evicting readers underneath it.
        if (seen % 20 == 0) (void)db->get(Slice::from(key_at(seen % 60)));
    }
    EXPECT_EQ(seen, 60) << "the scan must be unaffected by readers being evicted under it";
}

}  // namespace
}  // namespace elysiumkv::test
