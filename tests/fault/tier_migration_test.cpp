#include "db/db_impl.hpp"

#include "fault/fault_injecting_blob_store.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Migration between tiers" — migration moves files between tiers. It is the third kind of
/// background work and structurally the simplest: it moves bytes without
/// interpreting them.
class TierMigrationTest : public ::testing::Test {
protected:
    static constexpr Duration kMaxAge{60'000};
    static constexpr Duration kStallAge{120'000};

    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    void open(Options options) {
        options.background = BackgroundMode::Inline;
        // Atomic: the background thread reads the clock while the test advances
        // it, and a plain member would be a race the moment a test runs
        // threaded.
        options.clock = [this] { return now_.load(std::memory_order_relaxed); };
        options_ = options;
        auto opened = DB::open_with_result(options);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(opened->db);
    }

    void open_transient() { open(make_transient_options(store_, kMaxAge, kStallAge)); }

    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    void write(int count, const std::string& tag = "v", int from = 0) {
        for (int i = from; i < from + count; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(tag + std::to_string(i))),
                      Status::Ok);
        }
    }

    /// Gets data out of L0 and into L1 without advancing the clock, so the file
    /// is still young and stays on the hot tier. L0 files are never migrated
    /// (the positional recency), so this is the setup any migration test needs.
    void settle_into_l1() {
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_EQ(db_->compact_level(0), Status::Ok);
    }

    /// Moves the clock forward and lets the engine act on it. In Inline mode the
    /// caller is the only thread that will ever run migration or compaction.
    void advance(Duration by) {
        now_.fetch_add(static_cast<uint64_t>(by.count()), std::memory_order_relaxed);
        ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    }

    TierStats tier(int index) { return db_->stats().tiers[static_cast<size_t>(index)]; }

    TestStore store_{2};
    Options options_;
    std::atomic<uint64_t> now_{1'000'000};
    std::unique_ptr<DB> db_;
};

// ARCHITECTURE.md "Migration between tiers" — a low-traffic instance never fills a level, so nothing compacts — and
// data would sit on a losable store indefinitely. Migration is what prevents it,
// and it is driven by the clock rather than by write volume.
TEST_F(TierMigrationTest, IdlingPastMaxAgeMovesFilesOffTheTransientTier) {
    open_transient();

    write(50);
    settle_into_l1();
    ASSERT_GT(tier(0).file_count, 0) << "young files live on the hot tier";
    EXPECT_EQ(tier(1).file_count, 0);

    // Well under max_age: nothing should move.
    advance(Duration(30'000));
    EXPECT_GT(tier(0).file_count, 0);
    EXPECT_EQ(tier(0).files_pending_migration, 0);

    // Past it: the file leaves and the exposure goes away.
    advance(Duration(60'000));
    EXPECT_EQ(tier(0).file_count, 0);
    EXPECT_GT(tier(1).file_count, 0);
    EXPECT_EQ(tier(0).oldest_file_age, Duration(0)) << "an empty tier has no exposure";

    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
    EXPECT_GT(db_->stats().migrations, 0u);
    EXPECT_GT(db_->stats().migration_bytes, 0u);
}

// ARCHITECTURE.md "Positional recency" and ARCHITECTURE.md "The manifest is snapshots plus edits" pull in opposite directions at L0: recency there is resolved by
// file number, and a migration must allocate a fresh — therefore higher — one.
// An L0 file that migrated would read as the newest at its level whatever it
// held. So it leaves its tier by being compacted down instead, and the exposure
// bound is met either way.
TEST_F(TierMigrationTest, AnL0FileLeavesATransientTierByCompactionNotMigration) {
    open_transient();

    write(50);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_GT(tier(0).file_count, 0);
    ASSERT_EQ(engine().current_version()->file_count(0), 1u) << "still at L0";
    const uint64_t migrations_before = db_->stats().migrations;

    advance(Duration(90'000));

    EXPECT_EQ(tier(0).file_count, 0) << "the exposure bound is met regardless of mechanism";
    EXPECT_EQ(engine().current_version()->file_count(0), 0u) << "it left L0 as well as tier 0";
    EXPECT_EQ(db_->stats().migrations, migrations_before)
        << "an L0 file is never copied between stores; it is rewritten downward";
    EXPECT_GT(db_->stats().compactions, 0u);

    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

// ARCHITECTURE.md "Migration between tiers" and ARCHITECTURE.md "The manifest is snapshots plus edits" — migration is a byte copy under a **fresh file number**. A
// number is never reused, so the old object is unambiguously obsolete and every
// component keyed on the number alone stays correct.
TEST_F(TierMigrationTest, MigrationCopiesBytesUnderAFreshFileNumber) {
    open_transient();

    write(50);
    settle_into_l1();
    const std::vector<FileMetadata> before = engine().current_version()->all_files();
    ASSERT_EQ(before.size(), 1u);
    ASSERT_GT(before[0].level, 0) << "L0 files are compacted off a tier, not migrated";

    auto original = store_.store(0)->get(sst_object_name(before[0].file_number), 0,
                                         BlobStore::kReadToEnd).get();
    ASSERT_TRUE(original.has_value());

    advance(Duration(90'000));

    const std::vector<FileMetadata> after = engine().current_version()->all_files();
    ASSERT_EQ(after.size(), 1u);
    EXPECT_GT(after[0].file_number, before[0].file_number)
        << "a file number is never reused, including across a migration (ARCHITECTURE.md - The manifest is snapshots plus edits)";
    EXPECT_EQ(after[0].level, before[0].level) << "migration does not change a file's level";
    EXPECT_EQ(after[0].compression, before[0].compression) << "and never decodes anything";
    EXPECT_EQ(after[0].min_write_time_ms, before[0].min_write_time_ms)
        << "the file is exactly as old as it was, so placement stays monotone";
    EXPECT_EQ(after[0].num_entries, before[0].num_entries);
    EXPECT_EQ(after[0].file_bytes, before[0].file_bytes);
    EXPECT_EQ(after[0].store_id, store_.store(1)->id());

    // Byte-for-byte on the cold tier, and gone from the hot one.
    auto copied = store_.store(1)->get(sst_object_name(after[0].file_number), 0,
                                       BlobStore::kReadToEnd).get();
    ASSERT_TRUE(copied.has_value());
    EXPECT_EQ(*copied, *original);

    auto hot = store_.store(0)->list("").get();
    ASSERT_TRUE(hot.has_value());
    EXPECT_TRUE(hot->empty()) << "durable edit first, delete second — and the delete happened";
}

// ARCHITECTURE.md "Migration between tiers" — **placement is monotone.** A file only ever moves colder. A violation
// means files oscillate between stores and pay a copy each way.
TEST_F(TierMigrationTest, PlacementIsMonotoneOverALongRun) {
    open(make_tiered_options(store_, Duration(20'000)));

    std::map<uint64_t, int> lowest_seen;
    auto record = [&] {
        for (const FileMetadata& file : engine().current_version()->all_files()) {
            const int index = file.store_id == store_.store(0)->id() ? 0 : 1;
            auto it = lowest_seen.find(file.file_number);
            if (it == lowest_seen.end()) {
                lowest_seen[file.file_number] = index;
            } else {
                EXPECT_GE(index, it->second)
                    << "file " << file.file_number << " moved back to a hotter tier";
                it->second = index;
            }
        }
    };

    for (int round = 0; round < 20; ++round) {
        write(100, std::string(200, static_cast<char>('a' + round)));
        ASSERT_EQ(db_->flush(), Status::Ok);
        record();
        now_ += 5'000;
        ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
        record();
    }

    EXPECT_GT(db_->stats().migrations, 0u) << "the run has to have migrated something";
}

// ARCHITECTURE.md "Migration between tiers" — a tier over `max_bytes` migrates its oldest files down until it fits,
// and terminates — placement is monotone and the last tier is unbounded.
TEST_F(TierMigrationTest, CapacityEvictionMovesOldestFirstAndTerminates) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 100;  // keep the files separate, no compaction
    options.tiers = {
        Tier{.store = store_.store(0), .durability = Durability::Durable, .max_bytes = 8u << 10},
        Tier{.store = store_.store(1), .durability = Durability::Durable},
    };
    open(options);

    // Disjoint ranges settled into L1, so the tier accumulates several files
    // that are candidates for eviction rather than one that keeps being merged.
    for (int round = 0; round < 8; ++round) {
        write(100, std::string(200, static_cast<char>('a' + round)), round * 1000);
        settle_into_l1();
        now_.fetch_add(1'000, std::memory_order_relaxed);
        ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    }

    EXPECT_LE(tier(0).bytes, 8u << 10) << "the tier is brought back under its capacity";
    EXPECT_GT(tier(1).file_count, 0) << "and what left it went to the next tier down";

    // Oldest first: whatever remains on the hot tier is younger than what left.
    uint64_t oldest_remaining = 0;
    for (const FileMetadata& file : engine().current_version()->all_files()) {
        if (file.store_id != store_.store(0)->id()) continue;
        if (oldest_remaining == 0 || file.min_write_time_ms < oldest_remaining) {
            oldest_remaining = file.min_write_time_ms;
        }
    }
    for (const FileMetadata& file : engine().current_version()->all_files()) {
        if (file.store_id != store_.store(1)->id()) continue;
        EXPECT_LE(file.min_write_time_ms, oldest_remaining)
            << "a younger file was evicted ahead of an older one";
    }

    // Terminates: a second pass has nothing left to do.
    const uint64_t migrations = db_->stats().migrations;
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    EXPECT_EQ(db_->stats().migrations, migrations);
}

// ARCHITECTURE.md "Migration between tiers" — a tier with no files never migrates, so an idle instance does not churn
// on a timer.
TEST_F(TierMigrationTest, AnIdleStoreWithAnEmptyHotTierNeverMigrates) {
    open_transient();

    write(50);
    ASSERT_EQ(db_->flush(), Status::Ok);
    advance(Duration(200'000));
    ASSERT_EQ(tier(0).file_count, 0);

    const uint64_t migrations = db_->stats().migrations;
    const uint64_t compactions = db_->stats().compactions;
    for (int i = 0; i < 5; ++i) advance(Duration(1'000'000));
    EXPECT_EQ(db_->stats().migrations, migrations);
    EXPECT_EQ(db_->stats().compactions, compactions)
        << "nothing was at risk and nothing was over its score";
}

TEST_F(TierMigrationTest, OldestFileAgeTracksTheOldestWriteNotTheFile) {
    open_transient();

    write(50);
    ASSERT_EQ(db_->flush(), Status::Ok);
    EXPECT_LE(tier(0).oldest_file_age, Duration(1));

    now_.fetch_add(10'000, std::memory_order_relaxed);
    EXPECT_EQ(tier(0).oldest_file_age, Duration(10'000));

    // The file keeps the age of the writes it holds when it migrates: migration
    // changes where data lives, not when it was written (ARCHITECTURE.md "The manifest is snapshots plus edits").
    advance(Duration(60'000));
    EXPECT_EQ(tier(0).oldest_file_age, Duration(0));
    EXPECT_EQ(tier(1).oldest_file_age, Duration(70'000));
}

TEST_F(TierMigrationTest, MinWriteTimeSurvivesReopen) {
    open_transient();
    write(50);
    ASSERT_EQ(db_->flush(), Status::Ok);
    now_.fetch_add(20'000, std::memory_order_relaxed);
    const Duration before = tier(0).oldest_file_age;
    ASSERT_EQ(before, Duration(20'000));

    db_.reset();
    auto reopened = DB::open_with_result(options_);
    ASSERT_TRUE(reopened.has_value()) << status_name(reopened.error());
    db_ = std::move(reopened->db);

    EXPECT_EQ(tier(0).oldest_file_age, before)
        << "the age cannot be recomputed after a restart, so it must be persisted";
}

// ARCHITECTURE.md "Migration between tiers" — the stall valve is what makes the exposure bound a guarantee rather than
// an expectation, so it is not optional and must not be configurable off.
TEST_F(TierMigrationTest, TheStallValveFiresPastATierStallAge) {
    Options options = make_transient_options(store_, kMaxAge, kStallAge);
    options.background = BackgroundMode::Threaded;
    options.block_on_stall = false;
    options.clock = [this] { return now_.load(std::memory_order_relaxed); };
    // **The valve now engages on a coordinator tick, not on the writing thread.** One evaluator:
    // the maintenance loop owns the `stall_age` predicate and publishes the answer, so the write
    // path cannot compute a different one. The cost is that engaging lags by up to one interval,
    // which is the `+ interval` term the exposure window already carries — so the test waits for
    // it rather than assuming the very next `put` sees it.
    options.maintenance_interval = Duration(20);

    // **Migration must be unable to keep up, not merely slow.** A slow cold store was the first
    // shape here and it makes the stalled state a *window*: the rescue the age crossing makes due is
    // exactly what clears the flag again, so the test was racing the thing it had slowed down, and it
    // lost that race on a CI runner while passing locally. An unreachable cold store is the honest
    // version of "migration cannot keep up" — the rescue keeps failing with `Io`, exposure genuinely
    // grows, and the stalled state is stable rather than momentary.
    auto cold = std::make_shared<FaultInjectingBlobStore>(store_.store(1));
    options.tiers[1].store = cold;

    auto opened = DB::open_with_result(options);
    ASSERT_TRUE(opened.has_value());
    db_ = std::move(opened->db);

    write(20);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_GT(tier(0).file_count, 0);
    EXPECT_FALSE(tier(0).stalling);

    // From here the rescue can never succeed, so nothing will take the file off the hot tier.
    cold->set_unreachable(true);

    now_.fetch_add(90'000, std::memory_order_relaxed);  // past max_age, short of stall_age
    EXPECT_GT(tier(0).files_pending_migration, 0) << "degraded";
    EXPECT_FALSE(tier(0).stalling) << "but not yet stalled";

    now_.fetch_add(60'000, std::memory_order_relaxed);  // past stall_age

    // The valve engages on a coordinator tick rather than on the writing thread — one evaluator, so
    // the write path cannot compute a different answer — which costs up to one interval. That is the
    // `+ interval` term the exposure window already carries. The wait converges rather than racing:
    // the stalled state is now permanent until the cold store comes back.
    bool stalled = false;
    for (int i = 0; i < 400 && !stalled; ++i) {
        if (db_->put(Slice::from(key_at(1000 + i)), Slice::from("x")) == Status::Stalled) {
            stalled = true;
        }
        if (!stalled) std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(stalled) << "writes must stop rather than let exposure grow without bound";
    EXPECT_TRUE(tier(0).stalling);
    EXPECT_GT(db_->stats().stall_count, 0u);
}

// ARCHITECTURE.md "A tier is not a level", lag = 0: with no transient tier nothing is at risk beyond the
// memtable, and nothing ever migrates.
TEST_F(TierMigrationTest, LagZeroNeverMigratesAndNeverStalls) {
    open(make_options(store_, Compression::None, 1u << 20));

    write(200);
    ASSERT_EQ(db_->flush(), Status::Ok);
    for (int i = 0; i < 5; ++i) advance(Duration(10'000'000));

    EXPECT_EQ(db_->stats().migrations, 0u) << "one tier: there is nowhere to migrate to";
    for (const TierStats& stats : db_->stats().tiers) {
        EXPECT_FALSE(stats.stalling);
        EXPECT_EQ(stats.files_pending_migration, 0);
    }
}

// ARCHITECTURE.md "Migration between tiers" — only reaching a durable tier counts. Flushing into a transient tier
// moves data from one losable location to another and reduces exposure not at
// all.
TEST_F(TierMigrationTest, FlushingIntoATransientTierDoesNotReduceExposure) {
    open_transient();

    write(50);
    now_.fetch_add(30'000, std::memory_order_relaxed);
    // Unflushed, the memtable is what is at risk.
    EXPECT_EQ(db_->stats().memtable_age, Duration(30'000));
    EXPECT_EQ(tier(0).oldest_file_age, Duration(0));

    ASSERT_EQ(db_->flush(), Status::Ok);
    // Flushed onto the transient tier, the exposure simply moved: same age,
    // different place.
    EXPECT_EQ(db_->stats().memtable_age, Duration(0));
    EXPECT_EQ(tier(0).oldest_file_age, Duration(30'000));
    EXPECT_EQ(tier(1).file_count, 0);

    advance(Duration(60'000));
    EXPECT_EQ(tier(0).oldest_file_age, Duration(0));
    EXPECT_GT(tier(1).file_count, 0);
}

// ARCHITECTURE.md "Fault injection" — kill points around a migration. The order is copy, durable edit,
// delete (ARCHITECTURE.md "Open and recovery"), and each gap has a defined outcome.
TEST_F(TierMigrationTest, AKillBeforeTheEditLeavesTheOriginalServing) {
    Options options = make_tiered_options(store_, Duration(20'000));
    auto cold = std::make_shared<FaultInjectingBlobStore>(store_.store(1));
    options.tiers[1].store = cold;
    open(options);

    write(50);
    settle_into_l1();
    const std::vector<FileMetadata> before = engine().current_version()->all_files();
    ASSERT_EQ(before.size(), 1u);
    ASSERT_GT(before[0].level, 0) << "L0 files are compacted off a tier, not migrated";

    // The copy itself fails: nothing is committed, and the file stays put.
    cold->add_rule({.op = FaultInjectingBlobStore::Op::Put,
                    .match_count = 0,
                    .status = Status::Io});
    now_.fetch_add(60'000, std::memory_order_relaxed);
    EXPECT_NE(engine().compact_until_quiet(), Status::Ok);
    cold->clear_rules();

    const std::vector<FileMetadata> after = engine().current_version()->all_files();
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(after[0].store_id, store_.store(0)->id()) << "still on the tier it started on";
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }

    // And once the cold store answers again, the migration completes.
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    EXPECT_EQ(tier(0).file_count, 0);
    EXPECT_EQ(tier(1).file_count, 1);
}

TEST_F(TierMigrationTest, AKillBetweenTheEditAndTheDeleteLeavesACollectableOrphan) {
    Options options = make_tiered_options(store_, Duration(20'000));
    // Reclamation is on the sweep (ARCHITECTURE.md "Immutable named objects"): open cannot tell a dead
    // writer's residue from a live writer's committed file, so it deletes nothing at all. This case
    // is about the residue being *collectable*, so it configures the sweep.
    options.orphan_sweep_interval = Duration(1);
    options.orphan_retention = Duration(10'000);
    auto hot = std::make_shared<FaultInjectingBlobStore>(store_.store(0));
    options.tiers[0].store = hot;
    open(options);

    write(50);
    ASSERT_EQ(db_->flush(), Status::Ok);

    // The edit lands; the delete of the original does not.
    hot->add_rule({.op = FaultInjectingBlobStore::Op::Remove,
                   .match_count = 0,
                   .status = Status::Io});
    now_.fetch_add(60'000, std::memory_order_relaxed);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok)
        << "a failed delete does not fail the migration: the edit is already durable";
    hot->clear_rules();

    EXPECT_EQ(tier(1).file_count, 1) << "the version names the copy";
    auto stranded = store_.store(0)->list("").get();
    ASSERT_TRUE(stranded.has_value());
    EXPECT_FALSE(stranded->empty()) << "and the original is still sitting there";

    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }

    // **While this process lives the object is not an orphan**, it is a *pending deletion* whose
    // delete failed — it has an exact unreferenced-since time and `obsolete_retention` of its own,
    // and the sweep deliberately leaves it alone rather than double-counting it.
    ASSERT_EQ(engine().sweep_orphans_for_test(), Status::Ok);
    now_.fetch_add(60'000, std::memory_order_relaxed);
    ASSERT_EQ(engine().sweep_orphans_for_test(), Status::Ok);
    auto still_pending = store_.store(0)->list("").get();
    ASSERT_TRUE(still_pending.has_value());
    EXPECT_FALSE(still_pending->empty()) << "the pending queue owns it, so the sweep must not";

    // A restart empties that queue, and the same object comes back as an orphan — governed by the
    // orphan window and nothing else. That composition is why open validates
    // `orphan_retention >= obsolete_retention`.
    db_.reset();
    auto reopened = DB::open_with_result(options_);
    ASSERT_TRUE(reopened.has_value()) << status_name(reopened.error());
    db_ = std::move(reopened->db);

    ASSERT_EQ(engine().sweep_orphans_for_test(), Status::Ok);
    now_.fetch_add(60'000, std::memory_order_relaxed);
    ASSERT_EQ(engine().sweep_orphans_for_test(), Status::Ok);

    auto swept = store_.store(0)->list("").get();
    ASSERT_TRUE(swept.has_value());
    EXPECT_TRUE(swept->empty()) << "an unreferenced object must not survive its retention window";
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

// ARCHITECTURE.md "Fault injection" — compaction output old enough to exceed the hot tier's `max_age` is
// written **directly** to the colder store. Asserting only on where it ends up
// would also pass for an implementation that writes it hot and migrates it out
// a moment later, which pays a write to fast storage and a copy for every deep
// compaction — so assert that no migration happened at all.
TEST_F(TierMigrationTest, CompactionOutputOldEnoughIsBornCold) {
    Options options = make_tiered_options(store_, Duration(10'000), Compression::None, 1u << 20);
    options.levels[0].max_files = 1;
    open(options);

    // Two overlapping flushes, so the compaction merges rather than moving.
    write(200, "v0");
    ASSERT_EQ(db_->flush(), Status::Ok);
    write(200, "v1");
    ASSERT_EQ(db_->flush(), Status::Ok);

    // Age everything past the hot tier, then let the migrator drain it so the
    // counter below starts from a known place.
    now_.fetch_add(60'000, std::memory_order_relaxed);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    ASSERT_EQ(tier(0).file_count, 0);
    const uint64_t migrations_before = db_->stats().migrations;

    // Now compact old inputs: the output inherits their age via min(), so it
    // belongs on the cold tier from the moment it is written.
    write(200, "v2");
    ASSERT_EQ(db_->flush(), Status::Ok);
    now_.fetch_add(60'000, std::memory_order_relaxed);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    const uint64_t migrated = db_->stats().migrations - migrations_before;
    // The freshly flushed file is young when written and must migrate once; the
    // compaction *output* built from old inputs must not.
    EXPECT_LE(migrated, 1u) << "compaction output made from old data was written hot and moved";
    EXPECT_EQ(tier(0).file_count, 0);
    for (int i = 0; i < 200; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

// ARCHITECTURE.md "Migration between tiers" — migration between durable tiers is an optimisation and may be starved
// indefinitely without harm.
TEST_F(TierMigrationTest, ADurableToDurableMigrationMayBeStarved) {
    Options options = make_tiered_options(store_, Duration(10'000));
    open(options);

    write(50);
    ASSERT_EQ(db_->flush(), Status::Ok);
    now_.fetch_add(60'000, std::memory_order_relaxed);

    // Never run the migrator: everything still reads, nothing stalls, and the
    // only cost is that the file sits on more expensive storage than it needs.
    EXPECT_GT(tier(0).files_pending_migration, 0);
    EXPECT_FALSE(tier(0).stalling) << "a durable tier never stalls: nothing is at risk";
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
    for (int i = 0; i < 20; ++i) {
        ASSERT_EQ(db_->put(Slice::from(key_at(2000 + i)), Slice::from("x")), Status::Ok);
    }
}

// ARCHITECTURE.md "Compaction" — **the divergence between the two axes, which is the whole reason they
// are separate.** A dormant key range generates no score pressure, so its file
// lingers at a shallow level and ages there until it belongs on cold storage;
// a range receiving fresh writes has young files at the same level. One level,
// two tiers, at the same time. Level-derived placement would have pinned the
// dormant range to fast storage forever.
TEST_F(TierMigrationTest, OneLevelSpansTwoTiersWhenPartOfTheKeyspaceGoesQuiet) {
    open(make_tiered_options(store_, Duration(30'000)));

    // A dormant range, settled at L1 and then never touched again.
    write(100, "dormant", /*from=*/900'000);
    settle_into_l1();
    // Tracked by key range, not by file number: a migration renumbers (ARCHITECTURE.md "The manifest is snapshots plus edits").
    const std::string dormant_key = key_at(900'000);

    // Meanwhile a live range keeps receiving *new* keys, so its files carry
    // recent timestamps rather than inheriting an old min() from a merge.
    for (int round = 0; round < 4; ++round) {
        advance(Duration(20'000));
        write(100, "live", /*from=*/round * 1000);
        settle_into_l1();
    }

    // The dormant file is now well past tier 0's max_age; the newest live file
    // is not.
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    // Held in a local: all_files() returns by value, so pointers into the
    // range-for temporary would dangle the moment the loop ends.
    const std::vector<FileMetadata> files = engine().current_version()->all_files();

    const FileMetadata* dormant = nullptr;
    uint64_t newest_live_time = 0;
    const FileMetadata* newest_live = nullptr;
    for (const FileMetadata& file : files) {
        const bool is_dormant_range = file.smallest_key >= dormant_key;
        if (is_dormant_range) {
            dormant = &file;
        } else if (file.min_write_time_ms >= newest_live_time) {
            newest_live_time = file.min_write_time_ms;
            newest_live = &file;
        }
    }
    ASSERT_NE(dormant, nullptr) << "the dormant range's data is still somewhere";
    ASSERT_NE(newest_live, nullptr);

    EXPECT_EQ(dormant->store_id, store_.store(1)->id())
        << "nothing reads or rewrites it, so it belongs on cheap storage";
    EXPECT_EQ(newest_live->store_id, store_.store(0)->id())
        << "recent data stays hot, at the very same level";
    EXPECT_EQ(dormant->level, newest_live->level)
        << "one level, two tiers — which is why tier cannot be derived from level";

    EXPECT_GT(tier(0).file_count, 0);
    EXPECT_GT(tier(1).file_count, 0);

    // And the dormant data is still readable from where it ended up.
    for (int i = 900'000; i < 900'100; ++i) {
        EXPECT_TRUE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
}

// `SizePlacesAFileJustAsAgeDoes` lived here and is **deliberately deleted rather than inverted.**
// It asserted that a large, brand-new file was placed on a cold tier at birth — which was the whole
// problem with `Tier::max_file_bytes`: placement has to be monotone in age, and a file arriving cold
// on the day it was written is the opposite of that. There is no replacement assertion, because
// there is no longer a second input to placement to assert anything about. Keeping the test with its
// expectations flipped would have implied the behaviour still had a size dimension.

// ARCHITECTURE.md "Positional recency", ARCHITECTURE.md "Migration between tiers" — **an L0 file that leaves its tier must be the oldest one positionally.**
//
// L0 recency is resolved by file number, and L0 always shadows L1, so pushing a *newer*
// L0 file down leaves older L0 files above it and reverts a committed write. The old
// code chose by `min_write_time_ms`, which is a memtable's creation time — so two
// flushes in the same clock tick tie, and among ties the loop kept the first it saw,
// which is the newest file because `files_at(0)` runs newest-first.
//
// A fixed clock is what makes this deterministic: it is exactly the tie the differential
// suite hit by accident with a 64 KiB memtable, where several flushes land in one tick.
TEST_F(TierMigrationTest, AnL0FileLeavesItsTierOldestFirstEvenWhenWriteTimesTie) {
    Options options = make_tiered_options(store_, Duration(50));
    options.memtable_bytes = 1u << 10;  // flush on nearly every write
    LevelOptions l0;
    l0.max_files = 100;  // no ordinary L0 pressure: the tier is the only reason to move
    options.levels = {{0, l0}, {1, LevelOptions{}}, {2, LevelOptions{}}};
    open(options);

    // Both flushes happen at the same instant, so both files carry the same
    // min_write_time_ms and neither is distinguishable by age.
    ASSERT_EQ(db_->put(Slice::from(std::string("k")), Slice::from(std::string("old"))),
              Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->put(Slice::from(std::string("k")), Slice::from(std::string("new"))),
              Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);

    auto version = engine().current_version();
    ASSERT_EQ(version->file_count(0), 2u) << "both flushes must still be at L0";
    ASSERT_EQ(version->files_at(0)[0].min_write_time_ms, version->files_at(0)[1].min_write_time_ms)
        << "the tie is the point of this test";

    // Now both files are past the hot tier's age, so both want to move. Only the
    // older one may.
    // `advance` is what drives it: `compact_level(0)` is the *ordinary* L0 compaction
    // and never reaches the tier-driven path, so a first draft of this test passed with
    // the bug reinstated. `compact_until_quiet` is the inline equivalent of the
    // background loop, which is where `compact_l0_file_off_its_tier` lives.
    advance(Duration(1000));

    // Scoped: a `Pinned` holds the engine's live-pin counter, so one outliving its DB
    // writes to freed memory when it releases. ASan caught exactly that here.
    {
        auto found = db_->get(Slice::from(std::string("k")));
        ASSERT_TRUE(found.has_value()) << status_name(found.error());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(found->value().data()),
                              found->value().size()),
                  "new")
            << "the newer write must survive the older L0 file being pushed down";
    }

    // And across a reopen, since the manifest is what records the damage.
    db_.reset();
    auto reopened = DB::open(options_);
    ASSERT_TRUE(reopened.has_value()) << status_name(reopened.error());
    {
        auto after = (*reopened)->get(Slice::from(std::string("k")));
        ASSERT_TRUE(after.has_value()) << status_name(after.error());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(after->value().data()),
                              after->value().size()),
                  "new");
    }
}

}  // namespace
}  // namespace elysiumkv::test
