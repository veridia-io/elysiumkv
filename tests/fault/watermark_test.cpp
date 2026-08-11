/* The durable watermark: the embedder's resume point, and what happens to it when a transient
 * store loses its contents.
 *
 * The guarantee under test:
 *
 *   If, after opening a store, `recovered_watermark()` returns M, then replaying only the
 *   positions after M onto the recovered database yields the same logical key-value state as
 *   replaying the entire log.
 *
 * **Every safety assertion here is also satisfied by a rule that always returns nullopt**, and
 * that failure is silent — a full replay on every discard looks exactly like the feature working.
 * Too low is wasteful, too high is data loss, so both directions are asserted: the tightness case
 * near the end is what a nullopt-always rule fails, and it is not optional.
 */

#include "db/db_impl.hpp"

#include "sst/compression.hpp"
#include "sst/format.hpp"
#include "support/test_db.hpp"
#include "version/version_edit.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/manifest_catalog.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <future>
#include <memory>
#include <optional>
#include <utility>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

/// Fails the next `put_edit` and passes everything else through.
///
/// This is how "killed between the SST put and the manifest edit" is expressed without killing the
/// process: the SST is on the store, its edit never becomes durable, and the object is left as an
/// orphan — which is byte-for-byte the state a crash in that window leaves behind. The point of
/// running it in-process is that the *same* instance can then be closed and reopened, so what
/// recovery reports is observable.
class EditFailingCatalog final : public ManifestCatalog {
public:
    explicit EditFailingCatalog(std::shared_ptr<ManifestCatalog> below) : below_(std::move(below)) {}

    void fail_next_edit() { fail_next_edit_ = true; }

    Result<std::optional<Entry>> read() override { return below_->read(); }
    Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                 uint64_t generation) override {
        return below_->compare_and_set(std::move(expected), generation);
    }
    std::future<Status> put_snapshot(uint64_t generation, Slice bytes) override {
        return below_->put_snapshot(generation, bytes);
    }
    std::future<GetResult> get_snapshot(uint64_t generation) override {
        return below_->get_snapshot(generation);
    }
    std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) override {
        if (fail_next_edit_) {
            fail_next_edit_ = false;
            // `Io` rather than anything terminal: the engine must treat this as "ask again later",
            // which is what leaves the instance usable and the state observable.
            std::promise<Status> promise;
            promise.set_value(Status::Io);
            return promise.get_future();
        }
        return below_->put_edit(generation, seq, bytes);
    }
    std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) override {
        return below_->get_edit(generation, seq);
    }
    std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) override {
        return below_->list_edits(generation);
    }
    std::future<Status> delete_generation(uint64_t generation) override {
        return below_->delete_generation(generation);
    }

private:
    std::shared_ptr<ManifestCatalog> below_;
    bool fail_next_edit_ = false;
};

class WatermarkTest : public ::testing::Test {
protected:
    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    /// One durable tier. Nothing is ever discarded here, so the `max(w_high)` branch is what runs.
    Options durable_options() {
        Options options = make_options(store_, Compression::None, 64u << 10);
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_; };
        return options;
    }

    /// A transient hot tier over a durable one: the shape the rollback rule exists for.
    Options transient_options() {
        Options options = make_transient_options(store_, Duration(60'000), Duration(120'000));
        options.background = BackgroundMode::Inline;
        options.clock = [this] { return now_; };
        return options;
    }

    std::unique_ptr<DB> open(const Options& options) {
        auto opened = DB::open_with_result(options);
        EXPECT_TRUE(opened.has_value())
            << (opened.has_value() ? "" : status_name(opened.error()));
        if (!opened.has_value()) return nullptr;
        discarded_files_ = opened->discarded_files;
        return std::move(opened->db);
    }

    /// Wipes a store the way a replaced volume does: the directory is still there and empty.
    void wipe(const std::shared_ptr<LocalFileBlobStore>& store) {
        auto names = store->list("").get();
        ASSERT_TRUE(names.has_value());
        for (const std::string& name : *names) {
            ASSERT_EQ(store->remove(name).get(), Status::Ok);
        }
    }

    void write(DB& db, int from, int count, const std::string& tag = "v") {
        for (int i = from; i < from + count; ++i) {
            ASSERT_EQ(db.put(Slice::from(key_at(i)), Slice::from(tag + std::to_string(i))),
                      Status::Ok);
        }
    }

    /// Retires the live memtable so the next one is created at the *current* clock.
    ///
    /// Placement reads a file's `min_write_time_ms`, which a flushed L0 file inherits from its
    /// memtable's creation time — so a memtable that predates a clock advance flushes straight
    /// past the transient tier onto the cold one. Any test that advances the clock to age old data
    /// off the hot tier and then wants a *losable* file has to rotate in between, or the file it
    /// meant to lose is never at risk. The throwaway key is outside `key_at`'s range so it cannot
    /// disturb a later assertion.
    void rotate_memtable(DB& db) {
        ASSERT_EQ(db.put(Slice::from(std::string("~rotate")), Slice::from(std::string("x"))),
                  Status::Ok);
        ASSERT_EQ(db.flush(), Status::Ok);
    }

    std::vector<FileMetadata> files_of(DB& db) {
        return static_cast<DbImpl&>(db).current_version()->all_files();
    }

    TestStore store_{2};
    uint64_t now_ = 1'000'000;
    uint64_t discarded_files_ = 0;
};

// --- the rule itself, in isolation ---------------------------------------------

/* `RecoveryWatermark::resume_after` is a pure function, so the three states and both unsafe
 * alternatives can be pinned directly rather than only through full-store scenarios. That matters
 * more than usual here: this rule has been wrong three times, and two of those versions survived
 * review. A case that states the rule in six lines is easier to check than one that has to be
 * reconstructed from a tiered configuration.
 *
 * **And these are not redundant with the scenario cases below — they are the only cases that
 * discriminate.** Replacing `resume_after` with
 * `coalesce(discarded_lower_bound, surviving_upper_bound)` — the plausible wrong rule, which falls
 * back to the survivors when the discard set yields no bound — fails exactly one test in this file,
 * `ADiscardedFileWithNoLowerBoundCertifiesNothing` here. Every scenario case still passes, including
 * the scenario of the same name.
 *
 * The reason is structural, and it is why the bug is latent rather than immediately visible: a file
 * with no lower bound was sealed before the first `set_watermark`, so it is older than every file
 * that has one. At most *one* file can be `{no low, has high}` — the one live when the first
 * watermark was established — and files older than it have no high either, so they contribute
 * nothing to `surviving_upper_bound`. Being the oldest, that file is also the first to age off a
 * transient tier, so by the time anything else is losable it has usually already migrated to
 * durable storage and is a survivor rather than a casualty. Reaching the discriminating shape
 * through the engine needs the migration that would have rescued it to be suppressed. Which is a
 * good property of the engine and a bad property of a test suite that only drives it end to end.
 */
TEST(RecoveryWatermarkRule, NothingDiscardedReportsTheNewestSurvivingUpperBound) {
    RecoveryWatermark rule;
    rule.observe_survivor({40u, 50u});
    rule.observe_survivor({60u, 100u});
    EXPECT_FALSE(rule.anything_discarded());
    EXPECT_EQ(rule.resume_after(), std::optional<uint64_t>(100))
        // Sound only because nothing was lost: every write ever made is still in some file.
        << "the newest established watermark is fully covered";
}

TEST(RecoveryWatermarkRule, NoFilesAtAllCertifiesNothing) {
    EXPECT_FALSE(RecoveryWatermark().resume_after().has_value());
}

TEST(RecoveryWatermarkRule, ADiscardWithEveryLowerBoundPresentReportsTheirMinimum) {
    RecoveryWatermark rule;
    rule.observe_discarded({80u, 100u});
    rule.observe_discarded({90u, 120u});
    rule.observe_survivor({40u, 50u});
    ASSERT_TRUE(rule.anything_discarded());
    EXPECT_EQ(rule.resume_after(), std::optional<uint64_t>(80))
        << "min over the lows: a write at or below 80 cannot have lived only in a discarded file";
    // The two rules that are wrong here, named so a future change to `resume_after` trips on them.
    EXPECT_NE(rule.resume_after(), std::optional<uint64_t>(100))
        << "max of the discarded highs over-reports, which is data loss";
    EXPECT_NE(rule.resume_after(), rule.surviving_upper_bound())
        << "falling back to the survivors would be safe here but needlessly low";
}

/* **The case `coalesce(discarded_lower_bound, surviving_upper_bound)` gets wrong.**
 *
 * A transient file created before the first `set_watermark` has no lower bound, so nothing can be
 * proven about what it held. Coalescing would see an absent discard bound, fall through to the
 * survivors' `max(high) = 100`, and tell the embedder to resume at 101 — skipping a write that may
 * have existed only in the lost file. The absence of a lower bound is the absence of *evidence*, not
 * evidence that the file is irrelevant.
 */
TEST(RecoveryWatermarkRule, ADiscardedFileWithNoLowerBoundCertifiesNothing) {
    RecoveryWatermark rule;
    rule.observe_discarded({std::nullopt, std::nullopt});   // predates the first watermark
    rule.observe_survivor({40u, 100u});

    ASSERT_TRUE(rule.anything_discarded());
    EXPECT_FALSE(rule.resume_after().has_value()) << "replay from the beginning";
    // The control: the fallback that coalescing would have taken is *present and non-empty*, so
    // this case is distinguishable from "there was nothing to fall back to".
    EXPECT_EQ(rule.surviving_upper_bound(), std::optional<uint64_t>(100))
        << "the unsafe answer is available, and must still not be the one returned";
}

/* And the variant where the absent bound is *mixed in with* present ones, which is what makes
 * "skip the files that have no low" tempting. `min` over only the files that happen to carry a
 * bound ignores exactly the file that might hold the only copy of an earlier write.
 */
TEST(RecoveryWatermarkRule, OneDiscardedFileWithoutALowerBoundPoisonsTheMinimum) {
    RecoveryWatermark rule;
    rule.observe_discarded({80u, 100u});
    rule.observe_discarded({std::nullopt, 120u});

    EXPECT_FALSE(rule.discarded_lower_bound().has_value());
    EXPECT_FALSE(rule.resume_after().has_value());
    EXPECT_NE(rule.resume_after(), std::optional<uint64_t>(80))
        << "80 proves nothing about the file that has no bound at all";
}

// Order of observation must not matter: the absent bound absorbs whether it arrives first or last.
TEST(RecoveryWatermarkRule, TheAbsentLowerBoundAbsorbsInEitherOrder) {
    RecoveryWatermark absent_first;
    absent_first.observe_discarded({std::nullopt, 120u});
    absent_first.observe_discarded({80u, 100u});

    RecoveryWatermark absent_last;
    absent_last.observe_discarded({80u, 100u});
    absent_last.observe_discarded({std::nullopt, 120u});

    EXPECT_EQ(absent_first.resume_after(), absent_last.resume_after());
    EXPECT_FALSE(absent_first.resume_after().has_value());
}

// Zero is a position, and it must survive the rule rather than reading as absence.
TEST(RecoveryWatermarkRule, ZeroIsAReportableDiscardBound) {
    RecoveryWatermark rule;
    rule.observe_discarded({0u, 40u});
    ASSERT_TRUE(rule.resume_after().has_value());
    EXPECT_EQ(*rule.resume_after(), 0u);
}

// --- the basic promise ---------------------------------------------------------

TEST_F(WatermarkTest, ASetWatermarkSurvivesAFlushAndAReopen) {
    Options options = durable_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        EXPECT_FALSE(db->recovered_watermark().has_value()) << "a fresh store certifies nothing";
        write(*db, 0, 50);
        ASSERT_EQ(db->set_watermark(4242), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(reopened->recovered_watermark(), std::optional<uint64_t>(4242));
}

// The control for the case above: without the flush the watermark is not durable. There is no
// write-ahead log, so this is a resume point and not a durability improvement.
//
// **`abandon_unflushed` is what makes this a control again.** Destruction now attempts a flush, so
// simply letting the handle go saves the watermark and the case would assert nothing; the store has
// to be told to drop what a crash would have dropped.
TEST_F(WatermarkTest, AWatermarkSetButNeverFlushedIsNotReported) {
    Options options = durable_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        write(*db, 0, 20);
        ASSERT_EQ(db->set_watermark(10), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);

        write(*db, 20, 20);
        ASSERT_EQ(db->set_watermark(20), Status::Ok);   // deliberately not flushed
        db->abandon_unflushed();                        // ...and deliberately not saved at close
    }
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(reopened->recovered_watermark(), std::optional<uint64_t>(10))
        << "the unflushed watermark covers writes the engine never stored";
}

TEST_F(WatermarkTest, ZeroIsAPositionAndNotAbsence) {
    Options options = durable_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        write(*db, 0, 10);
        ASSERT_EQ(db->set_watermark(0), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_TRUE(reopened->recovered_watermark().has_value())
        << "zero is a valid position — a store at the start of its log";
    EXPECT_EQ(*reopened->recovered_watermark(), 0u);
}

TEST_F(WatermarkTest, ADecreasingWatermarkIsRefused) {
    auto db = open(durable_options());
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(db->set_watermark(100), Status::Ok);
    // Refused rather than clamped: clamping would hide a replay that went backwards, and the
    // whole value of the watermark is that it can be trusted.
    EXPECT_EQ(db->set_watermark(99), Status::Config);
    EXPECT_EQ(db->set_watermark(100), Status::Ok) << "equal is not decreasing";
    EXPECT_EQ(db->set_watermark(101), Status::Ok);
}

TEST_F(WatermarkTest, TheNonDecreasingCheckSpansAReopen) {
    Options options = durable_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        write(*db, 0, 10);
        ASSERT_EQ(db->set_watermark(500), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(reopened->set_watermark(499), Status::Config)
        << "the recovered position is an established watermark, not a clean slate";
    EXPECT_EQ(reopened->set_watermark(500), Status::Ok);
}

// --- where the bounds come from -----------------------------------------------

// `w_low` is captured when the memtable is **created**, not when it is sealed. A memtable open
// across several `set_watermark` calls keeps its original low — which is the whole reason the
// interval can certify anything, because the low is what asserts the file holds nothing at or
// below it.
TEST_F(WatermarkTest, TheLowerBoundReflectsMemtableCreationNotSealing) {
    auto db = open(durable_options());
    ASSERT_NE(db, nullptr);

    ASSERT_EQ(db->set_watermark(80), Status::Ok);   // established before the memtable takes writes
    write(*db, 0, 10);
    ASSERT_EQ(db->set_watermark(90), Status::Ok);   // while the same memtable is live
    write(*db, 10, 10);
    ASSERT_EQ(db->set_watermark(100), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    const std::vector<FileMetadata> files = files_of(*db);
    ASSERT_EQ(files.size(), 1u);
    EXPECT_EQ(files.front().watermark.low, std::optional<uint64_t>(80))
        << "capturing at seal instead would report 100 and lose the writes at 81..100";
    EXPECT_EQ(files.front().watermark.high, std::optional<uint64_t>(100));
}

// A memtable that predates the first `set_watermark` has no lower bound at all, and must not
// acquire one retroactively — it may already hold writes at or below the new position.
TEST_F(WatermarkTest, AMemtableHoldingWritesFromBeforeTheFirstCallHasNoLowerBound) {
    auto db = open(durable_options());
    ASSERT_NE(db, nullptr);

    write(*db, 0, 10);                              // written before any watermark exists
    ASSERT_EQ(db->set_watermark(100), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    const std::vector<FileMetadata> files = files_of(*db);
    ASSERT_EQ(files.size(), 1u);
    EXPECT_FALSE(files.front().watermark.low.has_value())
        << "these writes are at unknown positions; claiming 100 as a lower bound is false";
    EXPECT_EQ(files.front().watermark.high, std::optional<uint64_t>(100));
}

// The two halves come from **different inputs**, asserted separately: taking both from the same
// input is the natural implementation slip, and the `min` is the half the recovery proof rests on.
TEST_F(WatermarkTest, ACompactionOutputTakesTheMinLowAndTheMaxHigh) {
    Options options = durable_options();
    auto db = open(options);
    ASSERT_NE(db, nullptr);

    // **The same key range in both**, so the second compaction genuinely merges the two rather
    // than rewriting them side by side. `compact_level` moves one L0 file at a time, so
    // non-overlapping inputs would each keep their own interval and the composition would never
    // be exercised.
    ASSERT_EQ(db->set_watermark(10), Status::Ok);
    write(*db, 0, 40, "first");
    ASSERT_EQ(db->set_watermark(20), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);            // file A: [10, 20]

    ASSERT_EQ(db->set_watermark(30), Status::Ok);
    write(*db, 0, 40, "second");
    ASSERT_EQ(db->set_watermark(40), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);            // file B: [30, 40]

    ASSERT_EQ(db->compact_level(0), Status::Ok);

    const std::vector<FileMetadata> files = files_of(*db);
    ASSERT_EQ(files.size(), 1u) << "the two inputs must have been merged for this to mean anything";
    EXPECT_EQ(files.front().watermark.low, std::optional<uint64_t>(10))
        << "min over the inputs' lows — the output holds file A's data too";
    EXPECT_EQ(files.front().watermark.high, std::optional<uint64_t>(40))
        << "max over the inputs' highs. Asserted separately from the low because taking both from "
           "the same input is the natural slip, and the min is the half the proof rests on";
}

// One input with no lower bound makes the output have none: there was no bound to inherit.
TEST_F(WatermarkTest, ACompactionInputWithNoLowerBoundLeavesTheOutputWithout) {
    auto db = open(durable_options());
    ASSERT_NE(db, nullptr);

    write(*db, 0, 40, "first");                    // before any watermark: no low
    ASSERT_EQ(db->set_watermark(20), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    ASSERT_EQ(db->set_watermark(30), Status::Ok);
    write(*db, 0, 40, "second");
    ASSERT_EQ(db->set_watermark(40), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    ASSERT_EQ(db->compact_level(0), Status::Ok);
    const std::vector<FileMetadata> files = files_of(*db);
    ASSERT_EQ(files.size(), 1u);
    EXPECT_FALSE(files.front().watermark.low.has_value());
    EXPECT_EQ(files.front().watermark.high, std::optional<uint64_t>(40));
}

// A migration is a byte copy: the file holds exactly the writes it held, so both bounds carry.
TEST_F(WatermarkTest, AMigrationBetweenTiersCarriesTheIntervalUnchanged) {
    Options options = make_tiered_options(store_, Duration(60'000));
    options.background = BackgroundMode::Inline;
    options.clock = [this] { return now_; };

    auto db = open(options);
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(db->set_watermark(70), Status::Ok);
    write(*db, 0, 60);
    ASSERT_EQ(db->set_watermark(90), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);    // out of L0, which never migrates

    const std::string hot = store_.store(0)->id();
    now_ += 120'000;
    ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);

    const std::vector<FileMetadata> files = files_of(*db);
    ASSERT_FALSE(files.empty());
    bool moved = false;
    for (const FileMetadata& file : files) {
        if (file.store_id != hot) moved = true;
        EXPECT_EQ(file.watermark.low, std::optional<uint64_t>(70));
        EXPECT_EQ(file.watermark.high, std::optional<uint64_t>(90));
    }
    EXPECT_TRUE(moved) << "nothing migrated, so the assertion above proved nothing";
}

// --- the rollback -------------------------------------------------------------

// **The counterexample that killed the withdrawn file-number rule**, kept as a regression case.
//
// The withdrawn rule was "report the largest watermark among surviving files whose file_number is
// below the smallest discarded file number", and the construction that breaks it is this one: a
// durable survivor certifies a high watermark while the only copy of a later key lives on the
// transient tier and is discarded. Any rule reading a *single* per-file scalar over-reports here.
// The interval does not, because the discarded file's lower bound is what is reported.
TEST_F(WatermarkTest, ADiscardRollsBackBelowTheLostFilesLowerBound) {
    Options options = transient_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);

        // Durable, older data first, certified up to 50 and compacted down to the durable tier.
        ASSERT_EQ(db->set_watermark(40), Status::Ok);
        write(*db, 0, 60, "old");
        ASSERT_EQ(db->set_watermark(50), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_EQ(db->compact_level(0), Status::Ok);
        now_ += 300'000;                            // age it off the transient tier
        ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);
        rotate_memtable(*db);

        // Now the data that will be lost: a memtable that begins at 80 and is certified to 100.
        ASSERT_EQ(db->set_watermark(80), Status::Ok);
        write(*db, 1000, 40, "new");
        ASSERT_EQ(db->set_watermark(100), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    wipe(store_.store(0));
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_GT(discarded_files_, 0u) << "nothing was discarded, so this proves nothing";

    ASSERT_TRUE(reopened->recovered_watermark().has_value());
    EXPECT_EQ(*reopened->recovered_watermark(), 80u)
        << "the lost file's lower bound. Reporting its high (100) would skip the writes at 81..100; "
           "reporting a surviving file's watermark (50) would be safe but needlessly low";
}

// The `w_high`-is-unsafe case stated on its own, because it is the specific mistake the interval
// exists to prevent and the control is the rule that takes the high.
TEST_F(WatermarkTest, TheLostFilesUpperBoundWouldOverReport) {
    Options options = transient_options();
    std::optional<uint64_t> lost_low;
    std::optional<uint64_t> lost_high;
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        ASSERT_EQ(db->set_watermark(80), Status::Ok);
        write(*db, 0, 40);
        ASSERT_EQ(db->set_watermark(100), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);

        const std::string hot = store_.store(0)->id();
        for (const FileMetadata& file : files_of(*db)) {
            if (file.store_id != hot) continue;
            lost_low = file.watermark.low;
            lost_high = file.watermark.high;
        }
    }
    ASSERT_EQ(lost_low, std::optional<uint64_t>(80));
    ASSERT_EQ(lost_high, std::optional<uint64_t>(100)) << "the control value";

    wipe(store_.store(0));
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_GT(discarded_files_, 0u);
    EXPECT_EQ(reopened->recovered_watermark(), lost_low);
    EXPECT_NE(reopened->recovered_watermark(), lost_high)
        << "resuming at 100 never replays the write at 81";
}

// A lost file with no lower bound certifies nothing at all. Absent, not zero.
TEST_F(WatermarkTest, ADiscardOfAFileWithNoLowerBoundCertifiesNothing) {
    Options options = transient_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        write(*db, 0, 40);                          // written before any watermark exists
        ASSERT_EQ(db->set_watermark(100), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    wipe(store_.store(0));
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_GT(discarded_files_, 0u);
    EXPECT_FALSE(reopened->recovered_watermark().has_value())
        << "replay from the beginning — and that is distinct from a watermark of zero";
}

// The `min` has to survive lineage: a compaction output on the transient tier carries the oldest
// input's lower bound, and losing it rolls back to that, not to the compaction's own newest input.
TEST_F(WatermarkTest, ADiscardedCompactionOutputRollsBackToItsOldestInputsLowerBound) {
    Options options = transient_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);

        ASSERT_EQ(db->set_watermark(30), Status::Ok);
        write(*db, 0, 40);
        ASSERT_EQ(db->set_watermark(40), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);

        ASSERT_EQ(db->set_watermark(60), Status::Ok);
        write(*db, 40, 40);
        ASSERT_EQ(db->set_watermark(70), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);

        // Compact within the transient tier — the clock has not moved, so the output stays hot.
        ASSERT_EQ(db->compact_level(0), Status::Ok);
        const std::string hot = store_.store(0)->id();
        bool output_is_hot = false;
        for (const FileMetadata& file : files_of(*db)) {
            if (file.store_id == hot) output_is_hot = true;
        }
        ASSERT_TRUE(output_is_hot) << "the setup requires the compaction output to be losable";
    }
    wipe(store_.store(0));
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_GT(discarded_files_, 0u);
    EXPECT_EQ(reopened->recovered_watermark(), std::optional<uint64_t>(30))
        << "min over the lineage — 60 would skip the writes the older input held";
}

// **Tightness.** Every safety assertion above is also satisfied by a rule that always returns
// nullopt, and that failure is silent. This is the case that rejects it: nothing was lost, so the
// newest upper bound is available and reporting less would mean a needless full replay.
TEST_F(WatermarkTest, WithNothingDiscardedTheNewestUpperBoundIsReported) {
    Options options = transient_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);
        ASSERT_EQ(db->set_watermark(100), Status::Ok);
        write(*db, 0, 60);
        ASSERT_EQ(db->set_watermark(900), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_EQ(db->compact_level(0), Status::Ok);
    }
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_EQ(discarded_files_, 0u);
    ASSERT_TRUE(reopened->recovered_watermark().has_value())
        << "a rule that always reports nothing would pass every safety case and fail here";
    EXPECT_EQ(*reopened->recovered_watermark(), 900u)
        << "not the lower bound: nothing was lost, so nothing needs replaying twice";
}

// The boundary is **exclusive**, and that is where the proof is tight: the write at the reported
// position is present, and nothing is claimed about the one after it.
TEST_F(WatermarkTest, TheReportedPositionIsInclusiveOfItsOwnWritesAndExclusiveForReplay) {
    Options options = transient_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);

        // Position 80 is established, and everything written before that call is at or below 80.
        write(*db, 0, 60, "at-or-below-80");
        ASSERT_EQ(db->set_watermark(80), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_EQ(db->compact_level(0), Status::Ok);
        now_ += 300'000;
        ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);
        rotate_memtable(*db);

        // Everything after the call is at a position strictly above 80, and is what will be lost.
        write(*db, 1000, 40, "above-80");
        ASSERT_EQ(db->set_watermark(200), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    wipe(store_.store(0));
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_GT(discarded_files_, 0u);
    ASSERT_EQ(reopened->recovered_watermark(), std::optional<uint64_t>(80));

    // At or below the reported position: present, so replay may resume after it.
    auto present = reopened->get_copy(Slice::from(key_at(0)));
    EXPECT_TRUE(present.has_value()) << "a write at a position <= the report must have survived";
    // Above it: not assumed present, which is exactly what "resume at 81" is for.
    EXPECT_EQ(reopened->get_copy(Slice::from(key_at(1000))).error(), Status::NotFound);
}

// **The core guarantee, at the one window where it could be violated.** A flush writes the SST
// first and the manifest edit second, so a crash in between leaves the data on the store with
// nothing referencing it. The watermark rides *in that edit*, so it must not advance — if it did,
// the embedder would be told to skip replaying writes the engine never committed.
TEST_F(WatermarkTest, AKillBetweenTheSstPutAndTheManifestEditDoesNotAdvanceTheWatermark) {
    auto catalog = std::make_shared<EditFailingCatalog>(store_.catalog());

    Options options = durable_options();
    options.manifest_catalog = catalog;
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);

        write(*db, 0, 20, "committed");
        ASSERT_EQ(db->set_watermark(50), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);

        // Now the window: the SST lands, the edit does not.
        write(*db, 100, 20, "lost");
        ASSERT_EQ(db->set_watermark(900), Status::Ok);
        catalog->fail_next_edit();
        EXPECT_NE(db->flush(), Status::Ok) << "the edit was supposed to fail";
        // A kill, so nothing gets a second attempt: destruction would otherwise retry the flush,
        // and the retry would succeed because only the *next* edit was set to fail.
        db->abandon_unflushed();
    }

    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    EXPECT_EQ(reopened->recovered_watermark(), std::optional<uint64_t>(50))
        << "the watermark is only as durable as the edit that carries it";
    // And the data it would have covered is genuinely absent, which is why advancing would be wrong.
    EXPECT_EQ(reopened->get_copy(Slice::from(key_at(100))).error(), Status::NotFound);
    EXPECT_TRUE(reopened->get_copy(Slice::from(key_at(0))).has_value());
}

// **An over-age L0 file on the transient tier**, which is the case that breaks the naive argument
// "a file this old must already be durable". L0 files are never migrated — a fresh file number
// would reorder positional recency — so one can sit on a losable store well past `max_age` waiting
// to be compacted off it. The rollback has to be driven by the file's own lower bound, not by any
// inference from its age.
TEST_F(WatermarkTest, ADiscardedOverAgeLevelZeroFileRollsBackToItsLowerBound) {
    Options options = transient_options();
    {
        auto db = open(options);
        ASSERT_NE(db, nullptr);

        ASSERT_EQ(db->set_watermark(300), Status::Ok);
        write(*db, 0, 40);
        ASSERT_EQ(db->set_watermark(400), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);

        // Age it well past `max_age` **without** letting the L0 escape run: inline mode performs
        // background work only when asked, so this is the state a real store is in between the
        // crossing and the compaction that acts on it.
        now_ += 900'000;

        const std::string hot = store_.store(0)->id();
        bool over_age_and_still_hot = false;
        for (const FileMetadata& file : files_of(*db)) {
            if (file.level != 0 || file.store_id != hot) continue;
            ASSERT_GT(now_ - file.min_write_time_ms, 60'000u);
            over_age_and_still_hot = true;
        }
        ASSERT_TRUE(over_age_and_still_hot) << "the setup requires an over-age L0 file left on the "
                                              "transient tier, which is the whole point";
    }

    wipe(store_.store(0));
    auto reopened = open(options);
    ASSERT_NE(reopened, nullptr);
    ASSERT_GT(discarded_files_, 0u);
    EXPECT_EQ(reopened->recovered_watermark(), std::optional<uint64_t>(300))
        << "age says nothing about where this file's data sits in the log";
}

// --- the live gauge -----------------------------------------------------------

// `Stats::durable_watermark` and `recovered_watermark()` are different quantities and must not be
// confused: the getter is fixed at open, the gauge advances.
TEST_F(WatermarkTest, TheLiveGaugeIsTheTransientLossSurvivableFrontier) {
    Options options = transient_options();
    auto db = open(options);
    ASSERT_NE(db, nullptr);
    EXPECT_FALSE(db->stats().durable_watermark.has_value());

    // Certified to 50 and settled onto the durable tier.
    ASSERT_EQ(db->set_watermark(40), Status::Ok);
    write(*db, 0, 60);
    ASSERT_EQ(db->set_watermark(50), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);
    now_ += 300'000;
    ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);
    rotate_memtable(*db);
    ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);

    const std::string hot = store_.store(0)->id();
    for (const FileMetadata& file : files_of(*db)) {
        ASSERT_NE(file.store_id, hot) << "the setup requires nothing left on the transient tier";
    }
    EXPECT_EQ(db->stats().durable_watermark, std::optional<uint64_t>(50))
        << "no transient files remain, so the frontier is the newest upper bound";

    // Now put something on the transient tier. A flush there advances nothing operationally
    // relevant, so the frontier drops back to what a loss of that tier would leave.
    ASSERT_EQ(db->set_watermark(80), Status::Ok);
    write(*db, 1000, 40);
    ASSERT_EQ(db->set_watermark(100), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    EXPECT_EQ(db->stats().durable_watermark, std::optional<uint64_t>(80))
        << "tier-blind maximum (100) would report progress a loss of the hot tier would undo";
    EXPECT_FALSE(db->recovered_watermark().has_value())
        << "the getter describes the state recovered at open and must not move";
}

// --- format ------------------------------------------------------------------

// A manifest written by format version 1 is refused, and the error names the version rather than
// claiming the bytes are damaged. **Decided: clean break, no dual-read** — so the failure an
// operator sees has to be the honest one.
TEST_F(WatermarkTest, AManifestFromAnOlderFormatIsUnsupportedRatherThanCorrupt) {
    // A version-1 file entry: everything up to `min_write_time_ms`, and no watermark.
    std::string content;
    put_varint32(content, 1u);                       // format_version = 1
    put_varint64(content, 1u);                       // next_file_number
    put_varint64(content, 0u);                       // added_count
    put_varint64(content, 0u);                       // deleted_count
    put_varint64(content, 0u);                       // compaction pointer count
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(content), Compression::None, framed), Status::Ok);

    auto decoded = decode_version_edit(Slice::from(framed));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error(), Status::Unsupported)
        << "the checksum verified, so these are the bytes that were written: an unrecognised "
           "version is a real version, and reporting Corrupt would tell an operator to restore";
    EXPECT_TRUE(is_terminal(decoded.error())) << "a different binary is the remedy, not a retry";
}

}  // namespace
}  // namespace elysiumkv::test
