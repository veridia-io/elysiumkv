#include "db/db_impl.hpp"

#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <memory>
#include <set>
#include <string>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Compaction" end to end, through the engine rather than the picker: what compaction
/// does to the store, not what it decides to do.
class CompactionTest : public ::testing::Test {
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

    void reopen() {
        db_.reset();
        auto opened = DbImpl::open(options_, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
        db_ = std::move(opened->db);
    }

    static std::string key_at(int i) {
        char buf[48];
        std::snprintf(buf, sizeof(buf), "key:%08d", i);
        return buf;
    }

    void put_range(int from, int to, const std::string& tag) {
        for (int i = from; i < to; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(tag + "-" + std::to_string(i))),
                      Status::Ok);
        }
    }

    void expect_reads(int from, int to, const std::string& tag) {
        for (int i = from; i < to; ++i) {
            auto found = db_->get_copy(Slice::from(key_at(i)));
            ASSERT_TRUE(found.has_value()) << key_at(i) << ": " << status_name(found.error());
            EXPECT_EQ(std::string(found->begin(), found->end()), tag + "-" + std::to_string(i));
        }
    }

    size_t files_at(int level) { return engine().current_version()->file_count(level); }
    uint64_t entries_at(int level) {
        uint64_t entries = 0;
        for (const FileMetadata& file : engine().current_version()->files_at(level)) {
            entries += file.num_entries;
        }
        return entries;
    }
    std::set<uint64_t> file_numbers_at(int level) {
        std::set<uint64_t> numbers;
        for (const FileMetadata& file : engine().current_version()->files_at(level)) {
            numbers.insert(file.file_number);
        }
        return numbers;
    }

    TestStore store_{2};
    Options options_;
    std::unique_ptr<DB> db_;
};

TEST_F(CompactionTest, DrainsL0AndKeepsEveryValue) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 2;
    open(options);

    for (int round = 0; round < 12; ++round) {
        put_range(round * 100, round * 100 + 100, "v1");
        ASSERT_EQ(db_->flush(), Status::Ok);
    }

    EXPECT_LE(files_at(0), 2u) << "L0 must be drained to its limit";
    EXPECT_GT(files_at(1) + files_at(2), 0u) << "the data has to have gone somewhere";
    expect_reads(0, 1200, "v1");

    reopen();
    expect_reads(0, 1200, "v1");
}

TEST_F(CompactionTest, OverwritesCollapseWhenTheyMeet) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 2;
    open(options);

    for (int round = 0; round < 8; ++round) {
        put_range(0, 200, "v" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
    }
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    expect_reads(0, 200, "v7");
    // Eight rounds of the same 200 keys must not still be eight copies.
    EXPECT_LT(entries_at(0) + entries_at(1) + entries_at(2), 8u * 200);
}

// ARCHITECTURE.md "Compaction" — tombstones are dropped only when the output lands in the bottommost level
// that could contain the key. No other condition.
TEST_F(CompactionTest, TombstonesSurviveAboveTheBottommostLevel) {
    Options options = make_options(store_, Compression::None, 1u << 20);
    options.levels[0].max_files = 8;  // nothing drains: the tombstones stay put
    open(options);

    put_range(0, 400, "v1");
    ASSERT_EQ(db_->flush(), Status::Ok);
    const uint64_t values = entries_at(0) + entries_at(1) + entries_at(2);

    for (int i = 0; i < 400; ++i) {
        ASSERT_EQ(db_->remove(Slice::from(key_at(i))), Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);

    // A tombstone is an entry like any other while a level below could still
    // hold the key it shadows.
    EXPECT_EQ(entries_at(0) + entries_at(1) + entries_at(2), values + 400);
    for (int i = 0; i < 400; ++i) {
        auto found = db_->get(Slice::from(key_at(i)));
        ASSERT_FALSE(found.has_value());
        EXPECT_EQ(found.error(), Status::NotFound) << key_at(i);
    }
}

// ARCHITECTURE.md "Compaction" — tombstones are dropped when, and only when, the output lands in the
// bottommost level. Score alone leaves up to max_files files at a level — a
// level at exactly its limit is not over it — so the descent here is driven by
// the age trigger (ARCHITECTURE.md "Migration between tiers"), which is what drains a level completely.
TEST_F(CompactionTest, TombstonesAreDroppedWhereTheyAreBottommost) {
    Options options = make_options(store_, Compression::None, 1u << 20);
    options.levels[0].max_files = 1;
    open(options);

    put_range(0, 400, "v1");
    for (int i = 0; i < 400; ++i) {
        ASSERT_EQ(db_->remove(Slice::from(key_at(i))), Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);
    put_range(1000, 1100, "filler");  // a second L0 file, to push the first down
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    // L1 is bottommost for this range — the deeper level is empty — so the
    // tombstones went there and stopped.
    uint64_t tombstones = 0;
    for (const FileMetadata& file : engine().current_version()->all_files()) {
        tombstones += file.num_tombstones;
    }
    EXPECT_EQ(tombstones, 0u)
        << "a tombstone with nothing beneath it shadows nothing, and is dropped";
    for (int i = 0; i < 400; ++i) {
        EXPECT_FALSE(db_->get(Slice::from(key_at(i))).has_value()) << i;
    }
    EXPECT_EQ(engine().check_invariants(), Status::Ok);
}

// ARCHITECTURE.md "Compaction" — **the dynamic condition, end to end.** Deeper levels are configured but
// will never be reached by a store this small; a static last-level rule would
// accumulate deletions forever.
TEST_F(CompactionTest, ASmallStoreStillReclaimsItsDeletions) {
    Options options = make_options(store_, Compression::None, 1u << 20);
    options.levels[0].max_files = 1;
    // Six configured levels, and nowhere near enough data to reach them.
    options.levels[3] = options.levels[2];
    options.levels[4] = options.levels[2];
    options.levels[5] = options.levels[2];
    options.levels[2].max_bytes = 1u << 30;
    options.levels[3].max_bytes = 1u << 30;
    options.levels[4].max_bytes = 1u << 30;
    open(options);

    put_range(0, 200, "v1");
    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(db_->remove(Slice::from(key_at(i))), Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);
    put_range(1000, 1100, "filler");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    uint64_t tombstones = 0;
    for (const FileMetadata& file : engine().current_version()->all_files()) {
        tombstones += file.num_tombstones;
    }
    EXPECT_EQ(tombstones, 0u) << "deletions must not accumulate just because L5 is empty";
    EXPECT_EQ(engine().current_version()->file_count(5), 0u);
}

// ARCHITECTURE.md "Compaction" — within one store a trivial move is a pure manifest operation — the file
// number, and so the object itself, is unchanged.
TEST_F(CompactionTest, TrivialMoveRepointsTheSameObject) {
    Options options = make_options(store_, Compression::None, 1u << 20);
    options.levels[0].max_files = 1;   // two files is over the limit
    options.levels[1].max_bytes = 1;   // and L1 keeps nothing
    open(options);

    // Two flushes over disjoint ranges: nothing to merge with at any level, so
    // every step of the descent is a move rather than a rewrite.
    put_range(0, 50, "v1");
    ASSERT_EQ(db_->flush(), Status::Ok);
    put_range(1000, 1050, "v1");
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    // The first flushed object is file number 1. It is now at the bottom level
    // under that same number: nothing was rewritten, only re-pointed. (L0 keeps
    // the second file — a level at exactly its limit is not over it.)
    EXPECT_EQ(file_numbers_at(2), (std::set<uint64_t>{1}))
        << "the same object, re-pointed";
    EXPECT_EQ(db_->stats().compaction_bytes_written, 0u);
    EXPECT_EQ(engine().pending_deletions(), 0u)
        << "a moved file is not an obsolete object; it must not queue for deletion";
    expect_reads(0, 50, "v1");
    expect_reads(1000, 1050, "v1");
}

TEST_F(CompactionTest, CompactionOutputIsPlacedByAgeNotByLevel) {
    // **An injected clock, because the real one made this intermittent.** The bound below is a
    // millisecond, and whether the data had aged past it by the time the compaction placed its
    // output depended on how loaded the machine was — it failed roughly one run in three under a
    // parallel suite with builds running beside it, and never in isolation. Advancing the clock
    // by hand states the precondition instead of racing it.
    std::atomic<uint64_t> now{1'000'000};
    Options options = make_tiered_options(store_, Duration(1), Compression::Zstd, 16u << 10);
    options.levels[0].max_files = 1;
    options.levels[1].max_bytes = 1;
    options.clock = [&now] { return now.load(std::memory_order_relaxed); };
    open(options);

    put_range(0, 200, "v1");
    ASSERT_EQ(db_->flush(), Status::Ok);
    now.fetch_add(10);   // past the hot tier's bound, deterministically
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    // tier 0 accepts nothing older than 1ms, so output made from data written a
    // moment ago lands directly on the cold tier — it is not written hot and
    // migrated straight back out (ARCHITECTURE.md "A tier is not a level").
    ASSERT_GT(files_at(1) + files_at(2), 0u);
    auto cold = store_.store(1)->list("").get();
    ASSERT_TRUE(cold.has_value());
    EXPECT_FALSE(cold->empty()) << "old output belongs on the cold tier from the start";
    expect_reads(0, 200, "v1");
}

TEST_F(CompactionTest, CompactedAwayObjectsAreDeleted) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 2;
    open(options);

    for (int round = 0; round < 6; ++round) {
        put_range(0, 200, "v" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
    }
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);

    std::set<std::string> referenced;
    for (const FileMetadata& file : engine().current_version()->all_files()) {
        referenced.insert(sst_object_name(file.file_number));
    }
    auto names = store_.store(0)->list("").get();
    ASSERT_TRUE(names.has_value());
    for (const std::string& name : *names) {
        EXPECT_TRUE(referenced.count(name) != 0) << name << " is not referenced by any version";
    }
}

// ARCHITECTURE.md "Versions are immutable snapshots", and the reason VersionSet exists: an iterator holds a Version, so a file
// compaction unlinked stays readable until the iterator is released.
TEST_F(CompactionTest, AnIteratorKeepsCompactedFilesAliveUntilItIsReleased) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 2;
    open(options);

    // Overlapping ranges, so compaction rewrites rather than re-points: only a
    // rewrite makes the input objects obsolete.
    for (int round = 0; round < 4; ++round) {
        put_range(0, 400, "v" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
    }

    auto it = db_->iterator();
    ASSERT_TRUE(it->next());
    const std::string first_key = it->key().to_string();

    for (int round = 4; round < 10; ++round) {
        put_range(0, 400, "v" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
    }
    EXPECT_GT(engine().pending_deletions(), 0u)
        << "files the iterator still holds must not be unlinked";

    // The scan completes over the version it started with.
    int seen = 1;
    while (it->next()) ++seen;
    EXPECT_EQ(it->status(), Status::Ok);
    EXPECT_EQ(seen, 400) << "the iterator sees the version it pinned, whole";
    EXPECT_EQ(first_key, key_at(0));
    // And it reads the values as of when it started, from files that have since
    // been compacted away.
    EXPECT_EQ(db_->get_copy(Slice::from(key_at(0))).has_value(), true);

    it.reset();
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    EXPECT_EQ(engine().pending_deletions(), 0u) << "released, they are collected";
}

TEST_F(CompactionTest, CompactionCountersAreReported) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 2;
    open(options);

    // Overlapping ranges, so the compactions actually merge: a trivial move
    // moves no bytes and would leave the byte counters at zero.
    for (int round = 0; round < 8; ++round) {
        put_range(0, 200, "v" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
    }

    const Stats stats = db_->stats();
    EXPECT_GT(stats.compactions, 0u);
    EXPECT_GT(stats.compaction_bytes_read, 0u);
    EXPECT_GT(stats.compaction_bytes_written, 0u);
}

// ARCHITECTURE.md "Compaction" — L1 and below are non-overlapping by construction. The invariant checker
// enforces it; this is the case that would break it if output cutting were wrong.
TEST_F(CompactionTest, DeeperLevelsStayNonOverlapping) {
    Options options = make_options(store_, Compression::None, 16u << 10);
    options.levels[0].max_files = 2;
    options.levels[1].max_bytes = 64u << 10;
    options.levels[1].target_file_bytes = 8u << 10;
    options.levels[2].target_file_bytes = 8u << 10;
    open(options);

    for (int round = 0; round < 20; ++round) {
        put_range(round * 50, round * 50 + 200, "v" + std::to_string(round));
        ASSERT_EQ(db_->flush(), Status::Ok);
    }
    ASSERT_EQ(engine().compact_until_quiet(), Status::Ok);
    // The invariant checker is what actually enforces non-overlap; this
    // configuration is the one that would break it if output cutting were wrong.
    EXPECT_EQ(engine().check_invariants(), Status::Ok);
    EXPECT_GT(files_at(1) + files_at(2), 1u)
        << "output cutting should have produced several files";
    expect_reads(0, 50, "v0");
}

// ARCHITECTURE.md "Compaction" — the write stall. With stop_at reached and blocking declined, the caller is
// told rather than made to wait.
TEST_F(CompactionTest, StopAtReportsStalledWhenBlockingIsDeclined) {
    TestStore store(1);
    Options options;
    options.manifest_catalog = store.catalog();
    options.memtable_bytes = 4u << 10;
    options.block_bytes = 512;
    options.background = BackgroundMode::Threaded;  // nothing runs inline to relieve it
    options.block_on_stall = false;

    LevelOptions l0;
    l0.max_files = 1;
    l0.stop_at = 1;
    LevelOptions l1;
    options.levels = {{0, l0}, {1, l1}};
    options.tiers = {Tier{.store = store.store(0), .durability = Durability::Durable}};

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);

    // Fill L0 past stop_at faster than the compactor can drain it.
    bool saw_stalled = false;
    for (int i = 0; i < 4000 && !saw_stalled; ++i) {
        const Status status =
            db->put(Slice::from(key_at(i)), Slice::from(std::string(200, 'v')));
        if (status == Status::Stalled) saw_stalled = true;
        else ASSERT_EQ(status, Status::Ok);
        if (i % 20 == 0) (void)db->flush();
    }
    EXPECT_TRUE(saw_stalled) << "stop_at must eventually refuse writes";
    EXPECT_GT(db->stats().stall_count, 0u);
}

}  // namespace
}  // namespace elysiumkv::test

// --- compaction reads its inputs in windows ------------------------------------

namespace elysiumkv::test {
namespace {

/// Counts `get` calls and forwards everything. The count is the whole point: against object
/// storage each one is a round trip, and that — not bandwidth — is what made a production
/// compaction take minutes.
class CountingBlobStore final : public BlobStore {
public:
    explicit CountingBlobStore(std::shared_ptr<BlobStore> delegate)
        : delegate_(std::move(delegate)) {}

    std::string id() const override { return delegate_->id(); }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
        gets_.fetch_add(1, std::memory_order_relaxed);
        return delegate_->get(name, offset, len);
    }
    std::future<Status> put(std::string_view name, Slice bytes) override {
        return delegate_->put(name, bytes);
    }
    std::future<Status> remove(std::string_view name) override { return delegate_->remove(name); }
    std::future<ListResult> list(std::string_view prefix) override {
        return delegate_->list(prefix);
    }

    uint64_t gets() const { return gets_.load(std::memory_order_relaxed); }
    void reset() { gets_.store(0, std::memory_order_relaxed); }

private:
    std::shared_ptr<BlobStore> delegate_;
    std::atomic<uint64_t> gets_{0};
};

}  // namespace

/* **The neuter is the old code**: point `compaction_reader_for` at the store instead of a
 * `WindowedBlobStore` and this fails, because `SstReader` then asks for one block at a time. At a
 * 4 KiB block that is one request per 4 KiB of input; with a 2 MiB window it is one per 2 MiB. The
 * assertion is deliberately loose — it pins the order of magnitude, not an exact count, because
 * the exact count depends on how the picker happens to group files.
 */
TEST(CompactionReadTest, ReadsItsInputsInWindowsRatherThanOneRequestPerBlock) {
    TestStore backing;
    auto counting = std::make_shared<CountingBlobStore>(backing.store());

    Options options;
    options.manifest_catalog = backing.catalog();
    options.tiers.push_back(Tier{counting, Durability::Durable, {}, {}, {}});
    options.levels = make_levels(Compression::None);
    options.memtable_bytes = 1024 * 1024;
    options.block_bytes = 4096;
    options.background = BackgroundMode::Inline;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());

    // Several megabytes, so each L0 file is hundreds of blocks — the scale at which one request
    // per block and one per window differ by orders of magnitude rather than by a few.
    std::string value(512, 'x');
    for (int i = 0; i < 20000; ++i) {
        const std::string key = "key" + std::string(6 - std::to_string(i).size(), '0') +
                                std::to_string(i);
        ASSERT_EQ((*db)->put(Slice::from(key), Slice::from(value)), Status::Ok);
    }
    ASSERT_EQ((*db)->flush(), Status::Ok);

    const uint64_t before = counting->gets();
    ASSERT_EQ((*db)->compact_level(0), Status::Ok);
    const uint64_t during_compaction = counting->gets() - before;

    // 4000 x 512 B is ~2 MiB of values, so block-at-a-time would be several hundred requests;
    // windowed is a handful per input file plus its index and filter.
    EXPECT_LT(during_compaction, 100u)
        << "compaction issued " << during_compaction
        << " reads — that is block-at-a-time, not windowed";
    EXPECT_GT(during_compaction, 0u) << "no reads at all means the compaction did not happen";
}

}  // namespace elysiumkv::test
