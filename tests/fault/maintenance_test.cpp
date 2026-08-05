/* Maintenance scheduling: the coordinator that decides for itself what is due, instead of waiting
 * for a caller to say so.
 *
 * **The defect this exists for.** Three age-bounded behaviours in this engine shipped with a write
 * as their only trigger — the flush interval, age-driven migration between durable tiers, and the
 * L0 escape off a mismatched tier — and each failure was identical: the only thing that could have
 * noticed was the thing that had stopped happening. A store that goes quiet with a file on a
 * transient tier used to leave it there indefinitely, however the tiers were configured.
 *
 * So the first case below is the whole point, and **its control is what distinguishes a fix from a
 * test that would have passed before the fix**. The control here is a maintenance interval long
 * enough that the coordinator cannot tick during the test, which is the engine as it was.
 *
 * A note on how these tests wait. The coordinator ticks on *real* time while judging age by the
 * injected clock, which is deliberate: a thread waiting 20 ms wakes regardless of what the
 * injected clock says, so a test advances the clock and then simply waits. No wake path exists
 * only for tests.
 *
 * **What is deliberately not asserted here: the exposure window as a number.** The design states it
 * as `max_age + interval + queueing behind an in-flight compaction + the migration itself`, with the
 * queueing term derived from `max_compaction_bytes` over an assumed throughput. Asserting that as a
 * millisecond bound would be gating on wall-clock time, which CONTRIBUTING.md forbids for exactly
 * the reason it would bite: a shared runner would fail the test for reasons unrelated to the change,
 * and the assumption the window rests on is the operator's storage rather than anything the engine
 * controls. So the tests assert that each constituent *happens*, bounded generously; the window
 * itself is documented in ARCHITECTURE.md, *A tier is not a level*, where its throughput assumption
 * can be stated honestly.
 */

#include "db/db_impl.hpp"

#include "fault/fault_injecting_blob_store.hpp"
#include "support/alloc_counter.hpp"
#include "support/sanitizers.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace elysiumkv::test {
namespace {

/// Short enough that a test waits milliseconds rather than seconds, and short enough that the
/// periodic gate bypass — a fixed number of ticks — also lands inside a test's patience.
constexpr Duration kTick{20};

/// How long any of these tests will wait for the coordinator to converge before failing.
///
/// **Pure patience, so it is set generously.** Every positive case here waits for a *terminal* state
/// — a file has left a tier, a counter has advanced, a budget is met — so a longer bound cannot turn
/// a failure into a pass; it can only stop a slow runner from reporting one spuriously. The negative
/// cases pass their own short limits, and ctest's per-test timeout is the real backstop. The
/// workloads that go through a fault-injected store pay their injected latency on every operation,
/// which is several times slower on a shared CI runner than locally.
constexpr auto kSettle = std::chrono::seconds(30);

class MaintenanceTest : public ::testing::Test {
protected:
    static constexpr Duration kMaxAge{60'000};
    static constexpr Duration kStallAge{120'000};

    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    /// Every configuration here runs threaded with an injected clock: the coordinator is a thread,
    /// and there is nothing to test in inline mode, where the caller is the only thing that runs.
    Options base(Options options) {
        options.background = BackgroundMode::Threaded;
        options.maintenance_interval = kTick;
        options.clock = [this] { return now_.load(std::memory_order_relaxed); };
        return options;
    }

    void open(Options options) {
        options_ = std::move(options);
        auto opened = DB::open_with_result(options_);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(opened->db);
    }

    void write(int count, const std::string& tag = "v", int from = 0) {
        for (int i = from; i < from + count; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(tag + std::to_string(i))),
                      Status::Ok);
        }
    }

    void advance(Duration by) {
        now_.fetch_add(static_cast<uint64_t>(by.count()), std::memory_order_relaxed);
    }

    /// Polls `predicate` until it holds or `kSettle` elapses. Returns whether it held, so the
    /// caller reports its own message rather than a timeout.
    template <typename Predicate>
    static bool settle(Predicate predicate, std::chrono::milliseconds limit = kSettle) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return predicate();
    }

    static constexpr size_t kTierBudget = 4u << 10;

    /// Two durable tiers with a byte budget and **no age bound anywhere**, so capacity is the only
    /// thing that can be out of place and no time transition can open the gate on its behalf.
    Options capacity_bound_options() {
        Options options = make_options(store_, Compression::None, 16u << 10);
        options.tiers = {
            Tier{.store = store_.store(0),
                 .durability = Durability::Durable,
                 .max_bytes = kTierBudget},
            Tier{.store = store_.store(1), .durability = Durability::Durable},
        };
        return options;
    }

    /// Puts more than `kTierBudget` on tier 0, **below L0** — the migrator skips level 0, because a
    /// fresh file number there would reorder positional recency, so an L0 file is not a candidate
    /// for capacity eviction at all.
    void fill_over_capacity() {
        write(400, std::string(60, 'v'));
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_EQ(db_->compact_level(0), Status::Ok);
        ASSERT_GT(tier(0).bytes, kTierBudget)
            << "the tier has to be over its budget for eviction to be due";
    }

    TierStats tier(int index) { return db_->stats().tiers[static_cast<size_t>(index)]; }

    int files_on(int tier_index) { return tier(tier_index).file_count; }

    /// Waits until nothing has invalidated the coordinator's epoch for several ticks.
    ///
    /// **A control that takes the clock out of the gate needs this, and is racy without it.** The
    /// gate opens on *any* epoch change, and an executor that did work invalidates the epoch — so
    /// for a while after the last `put`, reconciles keep happening for reasons that have nothing to
    /// do with time. One of them would observe the advanced clock and do exactly the work the
    /// control asserts cannot happen. "No writes are arriving" is not the same as "quiescent".
    void wait_until_quiescent() {
        uint64_t last = engine().maintenance_epoch_for_test();
        int stable = 0;
        const auto deadline = std::chrono::steady_clock::now() + kSettle;
        while (stable < 5 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(kTick);
            const uint64_t now = engine().maintenance_epoch_for_test();
            stable = now == last ? stable + 1 : 0;
            last = now;
        }
        EXPECT_GE(stable, 5) << "the store never settled, so nothing after this is a clean control";
    }

    TestStore store_{2};
    Options options_;
    std::atomic<uint64_t> now_{1'000'000};
    std::unique_ptr<DB> db_;
};

// --- the finding ---------------------------------------------------------------

// **A quiet store rescues files off a transient tier.** Write, flush, advance the clock past
// `max_age`, then do *nothing at all*. Nothing but the coordinator can move the file.
TEST_F(MaintenanceTest, AQuietStoreRescuesFilesOffATransientTier) {
    open(base(make_transient_options(store_, kMaxAge, kStallAge)));

    write(60);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; })) << "nothing landed on the hot tier";

    advance(Duration(300'000));
    EXPECT_TRUE(settle([&] { return files_on(0) == 0 && files_on(1) > 0; }))
        << "the file aged past max_age on a store nobody was writing to, and stayed";
}

// The control. This is the engine as it was: age-driven work triggered only by a write. The
// interval is long enough that the coordinator cannot tick inside the test, so if the file still
// leaves the transient tier then something other than the coordinator moved it and the case above
// proves nothing.
TEST_F(MaintenanceTest, WithoutATimedReconcileTheQuietStoreNeverRescuesAnything) {
    open(base(make_transient_options(store_, kMaxAge, kStallAge)));
    // Not merely a long interval: any notification wakes the coordinator early and it then
    // reconciles for real, so a long interval is not the engine as it was. This takes the clock out
    // of the gate entirely, leaving a write as the only thing that can cause work.

    write(60);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; }));
    wait_until_quiescent();
    engine().suppress_timed_maintenance_for_test(true);

    advance(Duration(300'000));
    EXPECT_FALSE(settle([&] { return files_on(0) == 0; }, std::chrono::milliseconds(400)))
        << "with no timed reconcile the file must sit there — that is the defect being fixed";
}

// The same for a durable-to-durable age migration, which is the lowest-priority background task
// and documented as starvable. Starvable is not the same as never scheduled.
TEST_F(MaintenanceTest, AQuietStoreMigratesBetweenDurableTiersOnAge) {
    open(base(make_tiered_options(store_, kMaxAge)));

    write(60);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);   // out of L0, which never migrates
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; }));

    advance(Duration(300'000));
    EXPECT_TRUE(settle([&] { return files_on(0) == 0 && files_on(1) > 0; }));
}

/* **Rescue completes end to end with a compaction deliberately in flight.**
 *
 * The exposure window has four terms and the third — queueing behind an in-flight compaction — is
 * the largest, bounded only by `max_compaction_bytes` over throughput. The other rescue cases here
 * run on a quiet store, where that term is zero, so they would pass on a design where rescue shared
 * a worker with compaction and waited for it. This one puts a slow compaction in the way first.
 *
 * **What it asserts is that the term is survivable, not how large it is.** Asserting the window as a
 * number would be gating on wall-clock time, which CONTRIBUTING.md forbids, and the conversion from
 * a byte bound to a time bound rests on an assumption about the operator's storage rather than
 * anything the engine controls. So: the crossing happens while compaction is busy, and the file
 * still reaches durable storage.
 */
TEST_F(MaintenanceTest, RescueCompletesWithACompactionInFlight) {
    Options options = base(make_transient_options(store_, kMaxAge, kStallAge, 8u << 10));
    // Slow enough that a compaction spans many coordinator ticks, so the crossing below lands
    // inside one rather than between two.
    auto slow_hot = std::make_shared<FaultInjectingBlobStore>(store_.store(0));
    slow_hot->set_latency(std::chrono::milliseconds(5));
    options.tiers[0].store = slow_hot;
    open(std::move(options));

    // Enough L0 churn to keep the compaction executor busy well past the last write.
    write(1500, std::string(64, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return db_->stats().compactions > 0u; }))
        << "no compaction ran, so there was nothing for the rescue to queue behind";

    // The crossing, taken while that backlog is still draining rather than after it.
    advance(Duration(300'000));

    EXPECT_TRUE(settle([&] { return files_on(0) == 0 && files_on(1) > 0; }))
        << "the rescue never got past the compaction it was queued behind";
    EXPECT_GT(db_->stats().migrations, 0u);
}

// --- the notification is optional, the invalidation is not ---------------------

// Suppressing the *wake* must cost latency and nothing else. Only the wake is suppressed, not the
// epoch invalidation — suppressing both would assert a guarantee this design does not make.
TEST_F(MaintenanceTest, WorkStillRunsWithTheWakeNotificationSuppressed) {
    open(base(make_transient_options(store_, kMaxAge, kStallAge)));
    engine().suppress_maintenance_wakes_for_test(true);

    write(60);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; }));

    advance(Duration(300'000));
    EXPECT_TRUE(settle([&] { return files_on(0) == 0; }))
        << "late is acceptable; never is not";
}

// A predicate whose invalidating transitions were never wired into the epoch. The gate hides it,
// and the **periodic bypass** is what finds it anyway — which is the bound the bypass exists to
// provide, and the reason it is not optional.
//
// Capacity eviction is the right task for this: it is driven by bytes, not by time, so no
// time-transition deadline can open the gate on its behalf.
TEST_F(MaintenanceTest, AnUnInvalidatedPredicateStillRunsOnThePeriodicBypass) {
    open(base(capacity_bound_options()));
    // The epoch is frozen from here, and there is no age bound anywhere so `next_time_transition`
    // is unbounded: the gate can never open on state. The pause lets the coordinator observe the
    // pinned value once, so the *only* thing left that can open it is the periodic bypass.
    engine().pin_maintenance_epoch_for_test(true);
    engine().suppress_maintenance_wakes_for_test(true);
    std::this_thread::sleep_for(kTick * 4);

    fill_over_capacity();
    EXPECT_TRUE(settle([&] { return tier(0).bytes <= kTierBudget; }))
        << "only the periodic gate bypass can have done this";
}

// The control: the bypass is part of the clock's contribution, so taking the clock out must leave
// the work undone. Without this the case above would also pass on an engine with no bypass at all,
// because the pinned epoch differs from the last reconciled one exactly once.
TEST_F(MaintenanceTest, WithTheClockOutOfTheGateTheUnInvalidatedPredicateNeverRuns) {
    open(base(capacity_bound_options()));
    engine().pin_maintenance_epoch_for_test(true);
    engine().suppress_maintenance_wakes_for_test(true);
    engine().suppress_timed_maintenance_for_test(true);
    std::this_thread::sleep_for(kTick * 4);

    fill_over_capacity();
    EXPECT_FALSE(settle([&] { return tier(0).bytes <= kTierBudget; },
                        std::chrono::milliseconds(600)))
        << "with no bypass and a frozen epoch there is nothing left to notice";
}

// A memtable reaching `memtable_bytes` with the wake suppressed. The version never changed, so a
// gate keyed on version installs alone would hide the most common task in the engine — which is
// why the flush predicate is evaluated *ahead* of the gate rather than represented in it.
TEST_F(MaintenanceTest, AFillingMemtableIsRotatedWithNoWakeAndNoVersionChange) {
    Options options = make_options(store_, Compression::None, 8u << 10);
    open(base(std::move(options)));
    engine().suppress_maintenance_wakes_for_test(true);
    engine().pin_maintenance_epoch_for_test(true);   // the gate can never open on state

    const uint64_t before = engine().flush_count();
    write(300, std::string(64, 'v'));
    EXPECT_TRUE(settle([&] { return engine().flush_count() > before; }))
        << "the size predicate must be evaluated before the gate, not behind it";
}

// An iterator closing releases the last reference to an old version and makes objects
// collectible. The *current* version's generation does not change when that happens, so this is
// the second predicate the gate must not be allowed to hide.
TEST_F(MaintenanceTest, ObsoleteObjectsAreCollectedAfterAnIteratorClosesWithNoWake) {
    open(base(make_options(store_, Compression::None, 16u << 10)));

    write(200, std::string(48, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);

    // Hold a version across a compaction: its inputs become pending deletions that cannot be
    // collected while the iterator lives.
    auto iterator = db_->iterator();
    ASSERT_TRUE(iterator->next());
    ASSERT_EQ(db_->compact_level(0), Status::Ok);
    ASSERT_GT(engine().pending_deletions(), 0u)
        << "the live iterator is what keeps them pending";

    engine().suppress_maintenance_wakes_for_test(true);
    iterator.reset();   // no notification of any kind

    EXPECT_TRUE(settle([&] { return engine().pending_deletions() == 0u; }))
        << "a released version is collectible, and nothing else would have noticed";
}

// --- the gate is cheap ---------------------------------------------------------

// **An idle tick performs no version scan and allocates nothing.** This is what makes a
// one-second tick affordable across dozens of partition stores in one process, and it is asserted
// by allocation count rather than by wake count — a wake that scans is the failure being excluded.
TEST_F(MaintenanceTest, AnIdleReconcileAllocatesNothing) {
    if (running_under_sanitizer()) {
        GTEST_SKIP() << "sanitizer runtimes allocate on their own account";
    }
    // No age bound anywhere, so `next_time_transition` is unbounded and the gate has only the
    // epoch to consider. The tick is long so the background coordinator does not interleave with
    // the measurement; `reconcile_for_test` drives the same code it runs.
    Options options = base(make_options(store_, Compression::None, 4u << 20));
    options.maintenance_interval = Duration(3'600'000);
    open(std::move(options));
    engine().suppress_maintenance_wakes_for_test(true);

    write(200, std::string(48, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return engine().pending_deletions() == 0u; }));
    wait_until_quiescent();

    engine().reconcile_for_test(/*force_full=*/true);   // bring the gate up to date

    AllocationScope scope;
    for (int i = 0; i < 100; ++i) engine().reconcile_for_test(/*force_full=*/false);
    EXPECT_EQ(scope.count(), 0u)
        << "a closed gate must be two comparisons — no version reference, no file walk";
}

// The control: a forced reconcile does the O(files) work, so the assertion above is about the gate
// and not about the whole function being empty.
TEST_F(MaintenanceTest, AForcedReconcileDoesTheWorkTheGateSkips) {
    if (running_under_sanitizer()) {
        GTEST_SKIP() << "sanitizer runtimes allocate on their own account";
    }
    Options options = base(make_transient_options(store_, kMaxAge, kStallAge));
    options.maintenance_interval = Duration(3'600'000);
    open(std::move(options));
    engine().suppress_maintenance_wakes_for_test(true);

    write(200, std::string(48, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);
    engine().reconcile_for_test(/*force_full=*/true);

    AllocationScope scope;
    engine().reconcile_for_test(/*force_full=*/true);
    EXPECT_GT(scope.count(), 0u)
        << "if a full evaluation is also free, the idle assertion measures nothing";
}

// --- the stall predicate has one owner ----------------------------------------

/* The coordinator evaluates `stall_age` and publishes the answer; the write path reads the flag. One
 * owner, one definition — the same predicate in two places is how a valve ends up engaged by one and
 * not the other.
 *
 * **Both directions pin the flag against what the clock says**, and that is the only construction
 * that discriminates. Letting the flag become true *naturally* — advance past `stall_age` and wait —
 * proves nothing: a write path that computed the predicate itself would stall too, since the clock
 * agrees. It is also a race, because the rescue that the advance makes due is exactly what clears the
 * flag again; an earlier version of this test arranged the true state with a deliberately slow cold
 * store and lost the window on a CI runner while passing locally. The end-to-end valve behaviour is
 * `TierMigrationTest.TheStallValveFiresPastATierStallAge`; this pair is about who owns the decision.
 */
TEST_F(MaintenanceTest, TheWritePathStallsOnThePublishedFlagAloneAndNotOnTheClock) {
    Options options = base(make_transient_options(store_, kMaxAge, kStallAge));
    options.block_on_stall = false;
    open(std::move(options));

    write(40);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; }));

    // The clock has not moved, so nothing is anywhere near `stall_age`: a write path evaluating the
    // predicate itself would find no reason to hold anything.
    ASSERT_FALSE(engine().transient_stalled());
    EXPECT_EQ(db_->put(Slice::from(key_at(2000)), Slice::from("x")), Status::Ok);

    engine().pin_transient_stall_for_test(true);
    EXPECT_EQ(db_->put(Slice::from(key_at(2001)), Slice::from("x")), Status::Stalled)
        << "the write path is not reading the published flag";
    EXPECT_GT(db_->stats().stall_count, 0u);
}

// The control: with the published flag forced clear, the write path must *not* stall on its own
// reading of `stall_age`. If it does, there are two evaluators.
TEST_F(MaintenanceTest, WithTheFlagClearTheWritePathDoesNotStallOnItsOwn) {
    Options options = base(make_transient_options(store_, kMaxAge, kStallAge));
    options.block_on_stall = false;
    open(std::move(options));

    write(40);
    ASSERT_EQ(db_->flush(), Status::Ok);
    // The flag is pinned clear, so what the write path reads can never become true — while the clock
    // says the tier is far past `stall_age`, which a second evaluator would notice. Suppressing the
    // coordinator's *timing* instead would not do: any epoch change wakes it and it then publishes
    // for real, which is what made an earlier version of this control pass by accident.
    engine().pin_transient_stall_for_test(false);
    advance(Duration(600'000));
    ASSERT_FALSE(engine().transient_stalled());

    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(db_->put(Slice::from(key_at(3000 + i)), Slice::from("x")), Status::Ok)
            << "the write path computed the stall predicate itself, so there are two definitions";
    }
}

// --- the flush executor is not behind the compaction one ----------------------

// A memtable that fills while a long compaction is running must still be flushed. One immutable
// memtable is allowed in flight, so sharing a worker with compaction would stall writes for the
// length of that compaction.
TEST_F(MaintenanceTest, AFlushIsNotBlockedByALongCompaction) {
    Options options = base(make_options(store_, Compression::None, 8u << 10));
    // Slow the store so a compaction takes long enough to overlap the flush being asked for.
    auto slow = std::make_shared<FaultInjectingBlobStore>(store_.store(0));
    slow->set_latency(std::chrono::milliseconds(20));
    options.tiers = {Tier{.store = slow, .durability = Durability::Durable}};
    open(std::move(options));

    write(600, std::string(64, 'v'));   // enough L0 churn to keep compaction busy
    const uint64_t before = engine().flush_count();
    write(600, std::string(64, 'v'), 600);

    EXPECT_TRUE(settle([&] { return engine().flush_count() > before + 1; }))
        << "flushes stopped happening while compaction ran";
}

// --- configuration ------------------------------------------------------------

// A `Transient` tier with one level is permanent exposure: an L0 file leaves its tier by being
// compacted into L1, and with no L1 there is nowhere to go. No timer helps, and the stall valve
// eventually holds every write — a store that is neither durable nor writable.
TEST_F(MaintenanceTest, ATransientTierWithOneLevelIsRejectedAtOpen) {
    Options options = base(make_transient_options(store_, kMaxAge, kStallAge));
    LevelOptions only = options.levels.at(0);
    options.levels = {{0, only}};

    auto opened = DB::open_with_result(options);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error(), Status::Config)
        << "a silent livelock should be a configuration error instead";
}

// The control: the same single-level configuration is fine when every tier is durable, so the
// rejection is about the transient tier and not about level counts in general.
TEST_F(MaintenanceTest, ASingleLevelIsFineWhenEveryTierIsDurable) {
    Options options = base(make_options(store_, Compression::None, 64u << 10));
    LevelOptions only = options.levels.at(0);
    options.levels = {{0, only}};

    auto opened = DB::open_with_result(options);
    EXPECT_TRUE(opened.has_value())
        << (opened.has_value() ? "" : status_name(opened.error()));
}

// --- flush interval is one predicate among the rest ---------------------------

// A quiet store that flushes on the interval also *migrates* what it flushed, rather than
// accumulating it on the hot tier. The two used to be on separate machinery, which is how one of
// two adjacent loops ends up with a bug the other does not have.
TEST_F(MaintenanceTest, AQuietStoreFlushesOnTheIntervalAndThenMigratesWhatItFlushed) {
    Options options = base(make_transient_options(store_, kMaxAge, kStallAge, 64u << 20));
    options.flush_interval = Duration(50);
    open(std::move(options));

    // One small write, never enough to fill the memtable. Only the interval can flush it — and the
    // engine judges age by the injected clock, so the clock is what has to move.
    ASSERT_EQ(db_->put(Slice::from(key_at(1)), Slice::from("v")), Status::Ok);
    advance(Duration(200));
    ASSERT_TRUE(settle([&] { return engine().flush_count() > 0; }))
        << "the age predicate did not fire";
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; }));

    advance(Duration(300'000));
    EXPECT_TRUE(settle([&] { return files_on(0) == 0 && files_on(1) > 0; }))
        << "flushed and then left on a losable store is the worst of both";
}

// --- the single-deleter constraint -------------------------------------------

#ifdef ELYSIUMKV_PARANOID
// Flush and a compaction overlapping is **legal and must stay legal** — flush only adds an L0
// file, so its edit cannot contend with a delete-set chosen from an older version snapshot. The
// assertion is narrower than "one version-mutating task at a time", and getting that wrong would
// produce an assertion that fires immediately. This case is that check.
TEST_F(MaintenanceTest, TheSingleDeleterAssertionDoesNotFireOnFlushOverlappingCompaction) {
    Options options = base(make_options(store_, Compression::None, 8u << 10));
    auto slow = std::make_shared<FaultInjectingBlobStore>(store_.store(0));
    slow->set_latency(std::chrono::milliseconds(5));
    options.tiers = {Tier{.store = slow, .durability = Durability::Durable}};
    open(std::move(options));

    // Sustained write pressure: flushes and compactions run concurrently throughout, which is
    // exactly the overlap that must not trip the guard.
    write(2000, std::string(64, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);
    EXPECT_TRUE(settle([&] { return engine().flush_count() >= 2; }));
    EXPECT_GT(db_->stats().compactions, 0u) << "no compaction ran, so nothing overlapped";
}

/* **The control**, and without it the assertion above is one nobody has ever seen fire —
 * CONTRIBUTING.md is explicit that such a check has not been tested.
 *
 * A second deleting worker cannot be created from outside: compaction, migration and eviction share
 * one executor and `compaction_work_mutex_` serialises them, which is precisely the property being
 * protected. So the slot is claimed and not released, which is exactly what the guard would see if
 * someone added that worker, and the next deleting task must abort.
 *
 * Inline mode on purpose: a death test forks, and forking a process with a coordinator and two
 * executor threads running is asking for a flaky test rather than a meaningful one. Skipped under
 * the sanitizers for the same reason — their runtimes and `fork` do not mix well, and the assertion
 * is a build-gate check whose home is the debug preset.
 */
TEST_F(MaintenanceTest, ASecondDeletingTaskTripsTheAssertion) {
    if (running_under_sanitizer()) {
        GTEST_SKIP() << "death tests fork; sanitizer runtimes make that unreliable";
    }
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    Options options = make_options(store_, Compression::None, 8u << 10);
    options.background = BackgroundMode::Inline;
    options.clock = [this] { return now_.load(std::memory_order_relaxed); };
    open(std::move(options));

    write(400, std::string(64, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);

    EXPECT_DEATH(
        {
            engine().claim_deleting_task_for_test();
            (void)db_->compact_level(0);
        },
        "two deleting tasks in flight");
}
#endif

// --- the invariant, rather than three more cases -----------------------------

/* **Placement converges when idle.** Once every age bound has been crossed and maintenance has
 * settled, no file sits on a tier `placement()` does not select for it, no tier exceeds its
 * `max_bytes`, and no L0 file remains on a tier its age has outgrown.
 *
 * This is the third age-bounded behaviour in this engine whose only trigger turned out to be a
 * write. Three is a pattern, and the cases above still only cover the instances we happen to know
 * about; this is a check that catches the fourth without naming it, because the symptom is always
 * the same — a file left where it should no longer be, with nothing coming to move it.
 *
 * **It is a quiescence property, not a continuous invariant.** During normal operation it is false
 * by design: a file that has just aged past its bound is mismatched until maintenance runs, which
 * is exactly the window maintenance exists to close. So it belongs as an assertion after a settle,
 * in a test — not as a fifth `Invariant` enumerator, where it would fire constantly during healthy
 * operation.
 *
 * Its controls are what give it teeth. Four constituents, four controls, below: a single control
 * that removed the whole timer would be satisfied by any partial implementation, and a partial fix
 * is the most likely way this regresses.
 */
class ConvergenceTest : public MaintenanceTest {
protected:
    /// Every age bound that exists, plus a capacity bound, plus enough levels for the L0 escape
    /// to have somewhere to go.
    Options converging_options() {
        Options options = make_options(store_, Compression::None, 16u << 10);
        options.tiers = {
            Tier{.store = store_.store(0),
                 .durability = Durability::Transient,
                 .max_age = Duration(60'000),
                 .max_bytes = 64u << 10,
                 .stall_age = Duration(600'000)},
            Tier{.store = store_.store(1), .durability = Durability::Durable},
        };
        return base(std::move(options));
    }

    /// The assertion itself, in one place so the four control cases share it.
    ///
    /// Returns an empty string when placement has converged, and otherwise says what is
    /// mismatched — which is the difference between a diagnosable failure and a boolean.
    std::string convergence_failure() {
        auto version = engine().current_version();
        const uint64_t now = now_.load(std::memory_order_relaxed);
        const ResolvedTiers& tiers = tiers_for_test();

        for (const FileMetadata& file : version->all_files()) {
            const int at = tiers.tier_of_store(file.store_id);
            if (at < 0) return "file names a store the configuration does not have";
            const int wants = placement(tiers, file.min_write_time_ms, file.file_bytes, now);
            if (wants != at) {
                return "file " + std::to_string(file.file_number) + " at L" +
                       std::to_string(file.level) + " sits on tier " + std::to_string(at) +
                       " but placement selects tier " + std::to_string(wants);
            }
        }
        for (int index = 0; index <= tiers.last(); ++index) {
            const Tier& t = tiers.tiers[static_cast<size_t>(index)];
            if (!t.max_bytes.has_value()) continue;
            uint64_t bytes = 0;
            for (const FileMetadata& file : version->all_files()) {
                // **L0 excluded, matching what capacity eviction can act on.** An L0 file cannot be
                // migrated — a fresh file number there would reorder positional recency — so
                // counting it would make the invariant demand something the engine cannot do, and a
                // permanently-failing check is worse than no check. L0's size is bounded by its own
                // level policy (`stop_at`), on a different axis; the tier budget is about the
                // long-lived data below it.
                if (file.level == 0) continue;
                if (file.store_id == t.store->id()) bytes += file.file_bytes;
            }
            if (bytes > *t.max_bytes) {
                return "tier " + std::to_string(index) + " holds " + std::to_string(bytes) +
                       " bytes over a budget of " + std::to_string(*t.max_bytes);
            }
        }
        return {};
    }

    /// `resolve_tiers` is deterministic, so re-resolving the options gives the same table the
    /// engine is using without widening `DbImpl`'s test surface for it.
    const ResolvedTiers& tiers_for_test() {
        if (!resolved_.has_value()) {
            auto resolved = resolve_tiers(options_.tiers);
            EXPECT_TRUE(resolved.has_value());
            resolved_ = std::move(*resolved);
        }
        return *resolved_;
    }

    /// Produces files across levels, including L0 files on the transient tier, and then **waits for
    /// the write-driven work to drain**.
    ///
    /// The wait is not cosmetic. Writes bump the maintenance epoch, and so does an executor that
    /// did work, so reconciles keep happening for a while after the last `put` — and a control that
    /// advanced the clock before they finished would have one of them observe the new clock and do
    /// the very work the control is asserting cannot happen.
    void fill() {
        write(1200, std::string(64, 'v'));
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_TRUE(settle([&] { return engine().current_version()->file_count(0) > 0; }))
            << "the setup needs L0 files, which are what the L0 escape acts on";
        ASSERT_TRUE(settle([&] { return convergence_failure().empty(); }))
            << "the fill itself did not settle, so nothing after this measures the clock: "
            << convergence_failure();
    }

    /// Stops writing and advances the clock past every configured bound. From here the clock is the
    /// only thing that has changed.
    void go_quiet() { advance(Duration(900'000)); }

    void fill_then_go_quiet() {
        fill();
        go_quiet();
    }

    std::optional<ResolvedTiers> resolved_;
};

TEST_F(ConvergenceTest, PlacementConvergesOnAStoreThatHasGoneQuiet) {
    open(converging_options());
    fill_then_go_quiet();

    const bool converged = settle([&] { return convergence_failure().empty(); });
    EXPECT_TRUE(converged) << "placement did not converge: " << convergence_failure();
}

// Control 1 of four: **no timed reconcile at all.** This is the engine as it was, so the case
// above must fail here or it is proving nothing.
TEST_F(ConvergenceTest, WithNoTimedReconcileItDoesNotConverge) {
    open(converging_options());
    fill();
    // Suppressed *after* the store is quiescent, so the only change left for anything to notice is
    // the clock — and the clock has been taken out of the gate.
    wait_until_quiescent();
    engine().suppress_timed_maintenance_for_test(true);
    go_quiet();

    EXPECT_FALSE(settle([&] { return convergence_failure().empty(); },
                        std::chrono::milliseconds(500)))
        << "without a trigger that is not a write, nothing can have moved";
}

// Control 2: **transient rescue drained, the L0 escape not.** An L0 file cannot be migrated — a
// fresh file number would reorder L0's positional recency — so it leaves its tier by being
// compacted into L1, on a code path that has nothing to do with the migrator. Draining one and not
// the other is what a partial fix looks like, and it is the most likely way this regresses.
TEST_F(ConvergenceTest, DrainingMigrationsWithoutTheLevelZeroEscapeDoesNotConverge) {
    open(converging_options());
    engine().suppress_maintenance_tasks_for_test(
        static_cast<uint32_t>(MaintenanceTask::LevelZeroEscape));
    fill_then_go_quiet();

    EXPECT_FALSE(settle([&] { return convergence_failure().empty(); },
                        std::chrono::milliseconds(500)))
        << "an over-age L0 file left on the transient tier is exactly the symptom";
}

// Control 3: **the L0 escape drained, capacity eviction not.** Same argument in the other
// direction — capacity is a byte comparison rather than an age one, so it is a third path.
TEST_F(ConvergenceTest, DrainingEverythingExceptCapacityEvictionDoesNotConverge) {
    // A capacity bound with no age bound: nothing else can move these files, so suppressing
    // capacity eviction leaves the tier over budget with nothing coming for it. An age bound here
    // would drain the tier for the wrong reason and the control would pass vacuously.
    open(base(capacity_bound_options()));
    ASSERT_TRUE(settle([&] { return convergence_failure().empty(); }));

    engine().suppress_maintenance_tasks_for_test(
        static_cast<uint32_t>(MaintenanceTask::CapacityEviction));
    fill_over_capacity();

    EXPECT_FALSE(settle([&] { return convergence_failure().empty(); },
                        std::chrono::milliseconds(600)))
        << "a tier over max_bytes has not converged, whatever its files' placement says";
}

// Control 4: **transient rescue drained, durable-to-durable age migration not.** This is the
// constituent most likely to be quietly dropped, because it is explicitly the lowest priority and
// documented as starvable — starving it wastes money rather than risking data. The invariant should
// catch it, and this control is how that stops being an assumption.
TEST_F(ConvergenceTest, DurableToDurableAgeMigrationIsPartOfConvergence) {
    // Three tiers, with the age bound between the two *durable* ones, so the only thing that can
    // converge this configuration is the lowest-priority task.
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.tiers = {
        Tier{.store = store_.store(0),
             .durability = Durability::Durable,
             .max_age = Duration(60'000)},
        Tier{.store = store_.store(1), .durability = Durability::Durable},
    };
    open(base(std::move(options)));

    write(1200, std::string(64, 'v'));
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_TRUE(settle([&] { return files_on(0) > 0; }));
    advance(Duration(900'000));

    const bool converged = settle([&] { return convergence_failure().empty(); });
    ASSERT_TRUE(converged) << "nothing is at risk here, only cost — and it still has to happen: "
                           << convergence_failure();

    // And the control for the control: suppressed, it must not converge.
    engine().suppress_maintenance_tasks_for_test(
        static_cast<uint32_t>(MaintenanceTask::DurableAgeMigration));
    write(1200, std::string(64, 'v'), 2000);
    ASSERT_EQ(db_->flush(), Status::Ok);
    advance(Duration(900'000));
    EXPECT_FALSE(settle([&] { return convergence_failure().empty(); },
                        std::chrono::milliseconds(500)))
        << "starvable is not the same as never scheduled, and this is the difference";
}

}  // namespace
}  // namespace elysiumkv::test
