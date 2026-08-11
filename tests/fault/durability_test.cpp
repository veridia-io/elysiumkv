#include "db/db_impl.hpp"

#include "fault/fault_injecting_blob_store.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

using Op = FaultInjectingBlobStore::Op;

/// ARCHITECTURE.md "A tier is not a level" and ARCHITECTURE.md "Open and recovery" — what happens when a store loses its contents, and what must
/// happen when it merely fails to answer.
class DurabilityTest : public ::testing::Test {
protected:
    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    /// Levels 0 and 1 transient on store 0, level 2 durable on store 1.
    Options transient_options() {
        Options options = make_transient_options(store_, Duration(60'000), Duration(120'000));
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_; };
        return options;
    }

    Options durable_options() {
        Options options = make_options(store_, Compression::None, 64u << 10);
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_; };
        return options;
    }

    std::unique_ptr<DB> open_reporting(const Options& options,
                                       std::vector<std::string>* discarded = nullptr,
                                       bool* requires_recovery = nullptr) {
        auto opened = DB::open_with_result(options);
        EXPECT_TRUE(opened.has_value())
            << (opened.has_value() ? "" : status_name(opened.error()));
        if (!opened.has_value()) return nullptr;
        if (discarded != nullptr) *discarded = opened->discarded_stores;
        discarded_files_ = opened->discarded_files;
        if (requires_recovery != nullptr) *requires_recovery = opened->requires_recovery;
        return std::move(opened->db);
    }

    /// Wipes a store the way a replaced volume does: the directory is still
    /// there, it is simply empty. That is the only evidence a discard may act on.
    void wipe(const std::shared_ptr<DiskBlobStore>& store) {
        auto names = store->list("").get();
        ASSERT_TRUE(names.has_value());
        for (const std::string& name : *names) {
            ASSERT_EQ(store->remove(name).get(), Status::Ok);
        }
    }

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

    TestStore store_{2};
    uint64_t now_ = 1'000'000;
    uint64_t discarded_files_ = 0;
};

// --- what a clean shutdown saves, and what it does not ------------------------

/* **A clean shutdown flushes.** There is no write-ahead log, so a memtable dropped at destruction
 * is lost — and on a *clean* close that is a loss for no reason at all, since the process had every
 * opportunity to write it. RocksDB, which Kafka Streams runs with its WAL disabled, does the same
 * thing for the same reason: `avoid_flush_during_shutdown` defaults to false.
 *
 * **The attempt promises nothing**, which is why this asserts the outcome rather than a status: a
 * destructor has nowhere to report a failed flush to, and a durability guarantee nobody can check
 * would be worse than no guarantee. `flush()` remains the only way to know.
 */
TEST_F(DurabilityTest, AStoreClosedWithoutAFlushKeepsItsWrites) {
    Options options = durable_options();
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        ASSERT_EQ(db->put(Slice::from(key_at(1)), Slice::from("v1")), Status::Ok);
    }
    auto reopened = open_reporting(options);
    ASSERT_NE(reopened, nullptr);
    auto value = reopened->get_copy(Slice::from(key_at(1)));
    ASSERT_TRUE(value.has_value()) << "a clean close threw the memtable away";
    EXPECT_EQ(std::string(value->begin(), value->end()), "v1");
}

// The control, and the thing that makes the case above mean something: told to abandon, the store
// drops the memtable exactly as a crash would. Without this there would be no way left to write a
// test about losing unflushed state.
TEST_F(DurabilityTest, AnAbandonedStoreLosesWhatWasNeverFlushed) {
    Options options = durable_options();
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        ASSERT_EQ(db->put(Slice::from(key_at(1)), Slice::from("v1")), Status::Ok);
        db->abandon_unflushed();
    }
    auto reopened = open_reporting(options);
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(reopened->get_copy(Slice::from(key_at(1))).error(), Status::NotFound);
}

// Flushed writes are unaffected by abandoning, which only ever concerns the memtable.
TEST_F(DurabilityTest, AbandoningDropsOnlyTheUnflushedPart) {
    Options options = durable_options();
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        ASSERT_EQ(db->put(Slice::from(key_at(1)), Slice::from("durable")), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_EQ(db->put(Slice::from(key_at(2)), Slice::from("transient")), Status::Ok);
        db->abandon_unflushed();
    }
    auto reopened = open_reporting(options);
    ASSERT_NE(reopened, nullptr);
    auto kept = reopened->get_copy(Slice::from(key_at(1)));
    ASSERT_TRUE(kept.has_value());
    EXPECT_EQ(std::string(kept->begin(), kept->end()), "durable");
    EXPECT_EQ(reopened->get_copy(Slice::from(key_at(2))).error(), Status::NotFound);
}

// ARCHITECTURE.md "Open and recovery" — missing from a Durable store is corruption, and the failure names the
// file.
TEST_F(DurabilityTest, AMissingFileOnADurableStoreIsCorruption) {
    Options options = durable_options();
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        for (int i = 0; i < 200; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    auto names = store_.store(0)->list("").get();
    ASSERT_TRUE(names.has_value());
    ASSERT_FALSE(names->empty());
    const std::string lost = names->front();
    ASSERT_EQ(store_.store(0)->remove(lost).get(), Status::Ok);

    auto opened = DB::open_with_result(options);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error(), Status::Corrupt);
}

// ARCHITECTURE.md "Open and recovery" — missing from a Transient store drops **every** file on that store, not
// just the missing one — a partially-populated transient level is the
// resurrection hazard in its least tractable form.
TEST_F(DurabilityTest, LosingATransientStoreDiscardsEveryLevelOnIt) {
    Options options = transient_options();
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        for (int i = 0; i < 400; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_GT(db->stats().levels[0].file_count + db->stats().levels[1].file_count, 0u);
    }

    wipe(store_.store(0));

    std::vector<std::string> discarded;
    bool requires_recovery = false;
    auto db = open_reporting(options, &discarded, &requires_recovery);
    ASSERT_NE(db, nullptr) << "a lost transient store is expected and routine";
    EXPECT_TRUE(requires_recovery);
    ASSERT_EQ(discarded.size(), 1u);
    EXPECT_EQ(discarded.front(), store_.store(0)->id());
    EXPECT_GT(discarded_files_, 0u);

    const Stats stats = db->stats();
    EXPECT_EQ(stats.tiers[0].file_count, 0) << "every file on that store is gone, whatever level";
    EXPECT_TRUE(stats.requires_recovery);

    // ARCHITECTURE.md "A tier is not a level" — the engine reports; it does not enforce read blocking.
    db->mark_recovery_complete();
    EXPECT_FALSE(db->stats().requires_recovery);
}

// ARCHITECTURE.md "A tier is not a level" — **the case the whole design exists to make safe.** A discard does not
// merely lose recent writes: it resurrects stale ones, and a stale value is
// indistinguishable from a valid one.
TEST_F(DurabilityTest, DiscardResurrectsStaleValuesAndSaysSo) {
    Options options = transient_options();

    auto db = open_reporting(options);
    ASSERT_NE(db, nullptr);
    auto& engine = *static_cast<DbImpl*>(db.get());

    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from("v1")), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);

    // Age v1 past the transient tier's max_age: migration copies it to the
    // durable tier, which is what makes it survive the wipe below.
    now_ += 200'000;
    ASSERT_EQ(engine.compact_until_quiet(), Status::Ok);
    ASSERT_EQ(db->stats().tiers[0].file_count, 0)
        << "v1 must be resident on the durable tier for this test to mean anything";

    // Roll the memtable past the clock jump, so what follows is genuinely young
    // and stays on the transient tier.
    ASSERT_EQ(db->put(Slice::from(std::string("zzz-warmup")), Slice::from("x")), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(engine.compact_until_quiet(), Status::Ok);

    // v2 for half the keys, and a tombstone for a quarter — both land on the
    // transient store and stay there.
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from("v2")), Status::Ok);
    }
    for (int i = 50; i < 75; ++i) {
        ASSERT_EQ(db->remove(Slice::from(key_at(i))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_GT(db->stats().tiers[0].file_count, 0) << "v2 is young, so it is on the transient tier";

    // Everything reads correctly while the transient store is intact.
    EXPECT_EQ(db->get_copy(Slice::from(key_at(0)))->size(), 2u);
    EXPECT_FALSE(db->get(Slice::from(key_at(60))).has_value());
    db.reset();

    wipe(store_.store(0));

    bool requires_recovery = false;
    std::vector<std::string> discarded;
    auto reopened = open_reporting(options, &discarded, &requires_recovery);
    ASSERT_NE(reopened, nullptr);
    EXPECT_TRUE(requires_recovery) << "after a discard the store is wrong, not merely incomplete";

    // The overwritten keys read as their *older* value — not as absent.
    auto stale = reopened->get_copy(Slice::from(key_at(0)));
    ASSERT_TRUE(stale.has_value());
    EXPECT_EQ(std::string(stale->begin(), stale->end()), "v1")
        << "this is the resurrection: a stale value indistinguishable from a valid one";

    // And the deleted keys come back.
    auto resurrected = reopened->get_copy(Slice::from(key_at(60)));
    ASSERT_TRUE(resurrected.has_value())
        << "a tombstone shadowing a durable value behaves the same way";
    EXPECT_EQ(std::string(resurrected->begin(), resurrected->end()), "v1");

    // The embedder restores the window it was told about, and the store converges.
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(reopened->put(Slice::from(key_at(i)), Slice::from("v2")), Status::Ok);
    }
    for (int i = 50; i < 75; ++i) {
        ASSERT_EQ(reopened->remove(Slice::from(key_at(i))), Status::Ok);
    }
    reopened->mark_recovery_complete();

    for (int i = 0; i < 50; ++i) {
        auto found = reopened->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i;
        EXPECT_EQ(std::string(found->begin(), found->end()), "v2");
    }
    for (int i = 50; i < 75; ++i) {
        EXPECT_FALSE(reopened->get(Slice::from(key_at(i))).has_value()) << i;
    }
    for (int i = 75; i < 100; ++i) {
        auto found = reopened->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i;
        EXPECT_EQ(std::string(found->begin(), found->end()), "v1");
    }
    EXPECT_FALSE(reopened->stats().requires_recovery);
}

// ARCHITECTURE.md "Open and recovery" — **where a bug silently destroys intact data.** A store that fails to
// answer has not lost anything: open must fail retryably, with no discard and no
// manifest write.
TEST_F(DurabilityTest, AnUnreachableStoreIsNotALostStore) {
    Options options = transient_options();
    auto faulty = std::make_shared<FaultInjectingBlobStore>(store_.store(0));
    options.tiers[0].store = faulty;  // the transient tier, behind the injector

    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        for (int i = 0; i < 200; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    const std::map<std::string, std::string> before = manifest_snapshot();
    ASSERT_FALSE(before.empty());

    faulty->set_unreachable(true);
    auto failed = DB::open_with_result(options);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), Status::Io) << "failure to look is not evidence of absence";

    EXPECT_EQ(manifest_snapshot(), before)
        << "a retryable failure must not have rewritten the manifest";

    // And once the store can answer again, nothing was lost.
    faulty->set_unreachable(false);
    std::vector<std::string> discarded;
    bool requires_recovery = true;
    auto db = open_reporting(options, &discarded, &requires_recovery);
    ASSERT_NE(db, nullptr);
    EXPECT_TRUE(discarded.empty());
    EXPECT_FALSE(requires_recovery);
    for (int i = 0; i < 200; ++i) {
        EXPECT_TRUE(db->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

// ARCHITECTURE.md "Immutable named objects" and "Open and recovery" — a missing root directory is ambiguous between a fresh volume, a
// wrong path and a failed mount. It must never look like absence — even when the
// level is Transient.
TEST_F(DurabilityTest, AMissingRootIsNotAbsence) {
    Options options = transient_options();
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    const std::map<std::string, std::string> before = manifest_snapshot();
    std::filesystem::remove_all(store_.store(0)->root());

    auto failed = DB::open_with_result(options);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), Status::Io);
    EXPECT_EQ(manifest_snapshot(), before) << "no discard, and no manifest write";
}

// ARCHITECTURE.md "Open and recovery" — objects no version references are the residue of work that died before
// its edit was durable. Open collects them.
// ARCHITECTURE.md "Immutable named objects" — reclamation happens on the **sweep**, not at open: open
// cannot tell a dead writer's residue from a live writer's committed file, having taken no lock and
// performed no compare-and-set, and no default fixes an observation that weak. The engine does not
// need the deletion either — a stale file number is stepped over at open — so this tests a
// storage-reclamation feature rather than a correctness mechanism.
TEST_F(DurabilityTest, OrphansAreCollectedBySweeping) {
    Options options = durable_options();
    options.orphan_sweep_interval = Duration(1);
    options.orphan_retention = Duration(60'000);
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    // An object nothing points at, as a killed compaction would leave behind.
    ASSERT_EQ(store_.store(0)->put("000000009999.sst", Slice::from(std::string("orphan"))).get(),
              Status::Ok);

    auto db = open_reporting(options);
    ASSERT_NE(db, nullptr);
    auto& engine = static_cast<DbImpl&>(*db);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    now_ += 120'000;
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    auto names = store_.store(0)->list("").get();
    ASSERT_TRUE(names.has_value());
    EXPECT_EQ(std::find(names->begin(), names->end(), "000000009999.sst"), names->end())
        << "unreferenced for the whole window, so the sweep takes it";
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(db->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

// ARCHITECTURE.md "Fault injection" — kill mid-compaction -> reopen -> matches the oracle, orphans collected.
// A compaction that dies part-way has written objects no version references; the
// version it was building never becomes visible.
TEST_F(DurabilityTest, AKilledCompactionLeavesNoVisibleTraceAndItsOrphansAreCollected) {
    Options options = durable_options();
    // Score pressure at exactly two L0 files, so the compaction this test wants
    // happens once the fault is in place and not before.
    options.levels[0].max_files = 1;
    auto faulty = std::make_shared<FaultInjectingBlobStore>(store_.store(0));
    options.tiers = {Tier{.store = faulty, .durability = Durability::Durable}};

    std::map<std::string, std::string> expected;
    {
        auto db = open_reporting(options);
        ASSERT_NE(db, nullptr);
        auto& engine = *static_cast<DbImpl*>(db.get());

        // One flush, so nothing compacts yet: L0 holds a single file against a
        // limit of one, which is not over it.
        for (int i = 0; i < 300; ++i) {
            const std::string key = key_at(i);
            const std::string value = "v-" + std::to_string(i);
            ASSERT_EQ(db->put(Slice::from(key), Slice::from(value)), Status::Ok);
            expected[key] = value;
        }
        ASSERT_EQ(db->flush(), Status::Ok);

        const size_t before = engine.current_version()->all_files().size();
        ASSERT_EQ(before, 1u) << "one flush, one file, and no compaction yet";

        // Fail the write of file number 3 — the compaction's output — while
        // leaving the next flush itself free to succeed. Inline mode compacts at
        // the end of a flush, so this is where the killed compaction happens.
        faulty->add_rule({.op = Op::Put,
                          .name_contains = "000000000003",
                          .match_count = 0,
                          .status = Status::Io});
        // Overlapping the first batch, so the compaction merges rather than
        // trivially moving — a move writes no object and would dodge the fault.
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from("late")), Status::Ok);
        }
        const Status status = db->flush();
        EXPECT_NE(status, Status::Ok) << "a compaction that cannot write must not report success";
        faulty->clear_rules();

        // The flushed file landed; the compaction that followed it did not.
        EXPECT_EQ(engine.current_version()->all_files().size(), 2u);
        for (int i = 0; i < 100; ++i) expected[key_at(i)] = "late";
    }

    // Reopen: the last committed version is intact, and the residue is gone.
    // (The writes after the fault were never flushed, so ARCHITECTURE.md "Positional recency" says they are lost.)
    auto db = open_reporting(options);
    ASSERT_NE(db, nullptr);
    for (const auto& [key, value] : expected) {
        auto found = db->get_copy(Slice::from(key));
        ASSERT_TRUE(found.has_value()) << key;
        EXPECT_EQ(std::string(found->begin(), found->end()), value) << key;
    }

    std::set<std::string> referenced;
    for (const FileMetadata& file : static_cast<DbImpl*>(db.get())->current_version()->all_files()) {
        referenced.insert(sst_object_name(file.file_number));
    }
    auto names = store_.store(0)->list("").get();
    ASSERT_TRUE(names.has_value());
    for (const std::string& name : *names) {
        EXPECT_TRUE(referenced.count(name) != 0) << name << " is an uncollected orphan";
    }
}

// ARCHITECTURE.md "A tier is not a level" — a file vanishing while the store is open cannot be repaired in place —
// live iterators hold Versions referencing it.
TEST_F(DurabilityTest, ReadTimeDisappearanceMakesTheInstanceUnusable) {
    Options options = transient_options();
    auto db = open_reporting(options);
    ASSERT_NE(db, nullptr);

    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(std::string(50, 'v'))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_TRUE(db->get(Slice::from(key_at(0))).has_value());

    // Take the file away from under the running instance, and clear the reader
    // that is still holding it open.
    auto& engine = *static_cast<DbImpl*>(db.get());
    const auto files = engine.current_version()->files_at(0);
    ASSERT_FALSE(files.empty());
    wipe(store_.store(0));
    engine.evict_readers();

    bool saw_unusable = false;
    for (int i = 0; i < 200 && !saw_unusable; ++i) {
        auto found = db->get(Slice::from(key_at(i)));
        if (!found.has_value() && found.error() == Status::Unusable) saw_unusable = true;
    }
    EXPECT_TRUE(saw_unusable) << "close and reopen is the only repair";
    EXPECT_FALSE(engine.last_error().empty());
}

/// `Options::flush_interval` — the front of the durability story. A tier's `max_age` can only
/// act on a file, so without a time trigger a trickle of writes that never fills a memtable
/// keeps data in memory for as long as the process lives, whatever the tiers are set to.
class FlushIntervalTest : public ::testing::Test {
protected:
    void SetUp() override { now_.store(1'000'000); }

    Options options_with_interval(std::optional<Duration> interval, BackgroundMode mode) {
        Options options = make_options(store_, Compression::None);
        options.background = mode;
        options.flush_interval = interval;
        // Far above anything one small write reaches, so size can never be the reason a flush
        // happened — the test would otherwise pass without the feature existing.
        options.memtable_bytes = 64u << 20;
        options.clock = [this] { return now_.load(std::memory_order_relaxed); };
        return options;
    }

    static int l0_files(DB& db) { return db.stats().levels[0].file_count; }

    /// Waits for the flush thread to notice, since an age-driven flush is not synchronous with
    /// anything the test does. Bounded so a failure reports rather than hangs.
    static bool wait_for_l0(DB& db, int expected, std::chrono::milliseconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            if (l0_files(db) >= expected) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return l0_files(db) >= expected;
    }

    std::atomic<uint64_t> now_{1'000'000};
    TestStore store_;
};

TEST_F(FlushIntervalTest, AnIdleMemtableIsFlushedOnceTheIntervalElapses) {
    auto opened = DB::open_with_result(
        options_with_interval(std::chrono::milliseconds(200), BackgroundMode::Threaded));
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(opened->db);

    ASSERT_EQ(db->put(Slice::from(std::string("k")), Slice::from(std::string("v"))), Status::Ok);
    // Nothing else happens: no further writes, no explicit flush. This is the situation the
    // option exists for, and the case the write path cannot possibly notice.
    EXPECT_EQ(l0_files(*db), 0) << "not due yet, so nothing should have been written";

    now_.fetch_add(500);
    EXPECT_TRUE(wait_for_l0(*db, 1, std::chrono::seconds(5)))
        << "the memtable outlived the interval and was never flushed";
}

/// The negative control. Same store, same clock jump, same single write — only the interval is
/// absent. If this ever reports a flush, the test above proves nothing.
TEST_F(FlushIntervalTest, WithoutAnIntervalAnIdleMemtableIsNeverFlushed) {
    auto opened = DB::open_with_result(
        options_with_interval(std::nullopt, BackgroundMode::Threaded));
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(opened->db);

    ASSERT_EQ(db->put(Slice::from(std::string("k")), Slice::from(std::string("v"))), Status::Ok);
    now_.fetch_add(500);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(l0_files(*db), 0) << "size was the only trigger, and it was never reached";
}

/// An idle store must not accumulate empty files once per interval, forever.
TEST_F(FlushIntervalTest, AnEmptyMemtableIsNotFlushedByAge) {
    auto opened = DB::open_with_result(
        options_with_interval(std::chrono::milliseconds(50), BackgroundMode::Threaded));
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(opened->db);

    now_.fetch_add(10'000);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(l0_files(*db), 0) << "an empty memtable has nothing whose durability is at risk";
}

/// Inline mode has no thread to wake, so age is evaluated on the next write. Asserted rather
/// than left implicit, because it is a real behavioural difference between the modes.
TEST_F(FlushIntervalTest, InlineModeAppliesTheIntervalOnTheNextWrite) {
    auto opened = DB::open_with_result(
        options_with_interval(std::chrono::milliseconds(200), BackgroundMode::Inline));
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(opened->db);

    ASSERT_EQ(db->put(Slice::from(std::string("k1")), Slice::from(std::string("v"))), Status::Ok);
    now_.fetch_add(500);
    EXPECT_EQ(l0_files(*db), 0) << "no write has arrived since the interval elapsed";

    ASSERT_EQ(db->put(Slice::from(std::string("k2")), Slice::from(std::string("v"))), Status::Ok);
    EXPECT_EQ(l0_files(*db), 1) << "the write that follows an elapsed interval flushes";
    EXPECT_TRUE(db->get(Slice::from(std::string("k1"))).has_value());
}

}  // namespace
}  // namespace elysiumkv::test
