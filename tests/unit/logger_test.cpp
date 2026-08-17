#include "db/db_impl.hpp"
#include "fault/fault_injecting_blob_store.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace elysiumkv::test {
namespace {

struct Line {
    LogLevel level;
    LogEvent event;
    std::string message;
    std::thread::id thread;
};

/// Collects what the engine says. Locked because the engine logs from the flush, compaction and
/// maintenance threads as well as the caller's.
class Recorder {
public:
    std::shared_ptr<Logger> sink() {
        auto logger = std::make_shared<Logger>();
        logger->context = this;
        logger->write = [](void* context, LogLevel level, LogEvent event, const char* message,
                           size_t len) {
            auto* self = static_cast<Recorder*>(context);
            std::lock_guard<std::mutex> lock(self->mutex_);
            self->lines_.push_back({level, event, std::string(message, len),
                                    std::this_thread::get_id()});
        };
        return logger;
    }

    std::vector<Line> lines() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return lines_;
    }

    size_t count(LogEvent event) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return static_cast<size_t>(
            std::count_if(lines_.begin(), lines_.end(),
                          [&](const Line& line) { return line.event == event; }));
    }

    bool saw(LogEvent event) const { return count(event) != 0; }

private:
    mutable std::mutex mutex_;
    std::vector<Line> lines_;
};

void put_until_flush(DB& db, int count, const std::string& prefix = "k") {
    for (int i = 0; i < count; ++i) {
        const std::string key = prefix + std::to_string(i);
        ASSERT_EQ(db.put(Slice::from(key), Slice::from(std::string(512, 'v'))), Status::Ok);
    }
}

/// `OrphansReclaimed` is in the published event enum and was emitted from a function nothing
/// called, so it had never once fired. The sweep that does the deleting is the one that has to
/// report it — an operator watching for the event would otherwise conclude reclamation never runs,
/// which is indistinguishable from the store leaking.
TEST(LoggerTest, TheSweepReportsWhatItReclaimed) {
    TestStore store;
    Recorder recorder;
    std::atomic<uint64_t> now{1'000'000};

    Options options = make_options(store);
    // Inline, so the sweeps this test asks for are the only ones that happen.
    options.background = BackgroundMode::Inline;
    options.orphan_sweep_interval = Duration(1);
    options.orphan_retention = Duration(60'000);
    options.clock = [&now] { return now.load(std::memory_order_relaxed); };
    options.logger = recorder.sink();
    options.min_log_level = LogLevel::Debug;

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);
    put_until_flush(*db, 40);
    ASSERT_EQ(db->flush(), Status::Ok);

    ASSERT_EQ(store.store(0)->put("000000004242.sst",
                                  Slice::from(std::string_view("residue"))).get(),
              Status::Ok);

    auto& engine = static_cast<DbImpl&>(*db);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    EXPECT_FALSE(recorder.saw(LogEvent::OrphansReclaimed))
        << "one observation is not a sustained one, so there was nothing to report yet";

    now.fetch_add(120'000);
    ASSERT_EQ(engine.sweep_orphans_for_test(), Status::Ok);
    EXPECT_TRUE(recorder.saw(LogEvent::OrphansReclaimed))
        << "the sweep deleted an object and said nothing about it";
}

TEST(LoggerTest, SaysNothingWhenNoLoggerIsConfigured) {
    TestStore store;
    Options options = make_options(store);
    // No logger. If any call site dereferenced it unconditionally this would crash rather than
    // fail, which is the point of running a whole flush cycle here.
    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    put_until_flush(**db, 200);
    EXPECT_EQ((*db)->flush(), Status::Ok);
}

/// A sink that aborts if it is ever called, so "filtered out" is proven rather than assumed —
/// a recorder that simply stays empty cannot tell "not emitted" from "not formatted".
TEST(LoggerTest, FormatsNothingBelowTheThreshold) {
    TestStore store;
    Options options = make_options(store);
    auto logger = std::make_shared<Logger>();
    logger->context = nullptr;
    logger->write = [](void*, LogLevel, LogEvent, const char*, size_t) {
        ADD_FAILURE() << "the sink was called with min_log_level = Off";
    };
    options.logger = logger;
    options.min_log_level = LogLevel::Off;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    put_until_flush(**db, 200);
    EXPECT_EQ((*db)->flush(), Status::Ok);
}

TEST(LoggerTest, ReportsAFlushAtInfo) {
    TestStore store;
    Recorder recorder;
    Options options = make_options(store);
    options.logger = recorder.sink();
    options.min_log_level = LogLevel::Info;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    put_until_flush(**db, 200);
    ASSERT_EQ((*db)->flush(), Status::Ok);

    EXPECT_TRUE(recorder.saw(LogEvent::FlushComplete));
    for (const Line& line : recorder.lines()) {
        if (line.event == LogEvent::FlushComplete) {
            EXPECT_EQ(line.level, LogLevel::Info);
            EXPECT_NE(line.message.find("entries"), std::string::npos) << line.message;
        }
    }
}

TEST(LoggerTest, WarnKeepsIncidentsAndDropsRoutineCompletions) {
    TestStore store;
    Recorder recorder;
    Options options = make_options(store);
    options.logger = recorder.sink();
    options.min_log_level = LogLevel::Warn;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    put_until_flush(**db, 400);
    ASSERT_EQ((*db)->flush(), Status::Ok);

    EXPECT_FALSE(recorder.saw(LogEvent::FlushComplete));
    EXPECT_FALSE(recorder.saw(LogEvent::CompactionComplete));
}

/// The failure this whole facility exists for: a retryable background failure is currently set and
/// then silently cleared by the next write, so a store retrying its way through a degraded backend
/// is indistinguishable from a healthy one.
TEST(LoggerTest, ARetryableBackgroundFailureIsVisibleAsBothTheFailureAndTheRetry) {
    TestStore store;
    Recorder recorder;
    auto faulty = std::make_shared<FaultInjectingBlobStore>(store.store(0));
    faulty->add_rule({.op = FaultInjectingBlobStore::Op::Put,
                      .name_contains = ".sst",
                      .first_match = 0,
                      .match_count = 1,
                      .status = Status::Io});

    Options options = make_options(store);
    options.tiers = {Tier{.store = faulty, .durability = Durability::Durable}};
    options.logger = recorder.sink();
    options.min_log_level = LogLevel::Info;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    put_until_flush(**db, 200);
    // The first flush fails with Io; the engine keeps the frozen memtable and retries.
    (void)(*db)->flush();
    put_until_flush(**db, 200, "second");
    (void)(*db)->flush();

    EXPECT_TRUE(recorder.saw(LogEvent::BackgroundFailure))
        << "the flush failure reached no counter, no Stats field and now no log line either";
    EXPECT_TRUE(recorder.saw(LogEvent::BackgroundRetry))
        << "the retry that clears bg_error_ is the half that hides a degraded backend";
}

/// The sink runs user code. If it were called under `mem_mutex_` this deadlocks rather than fails,
/// which is why the emit sites capture and log after leaving the critical section.
TEST(LoggerTest, ASinkMayReadTheStoreItIsLoggingAbout) {
    TestStore store;
    Options options = make_options(store);

    struct Reentrant {
        DB* db = nullptr;
        std::atomic<int> calls{0};
    } reentrant;

    auto logger = std::make_shared<Logger>();
    logger->context = &reentrant;
    logger->write = [](void* context, LogLevel, LogEvent, const char*, size_t) {
        auto* self = static_cast<Reentrant*>(context);
        if (self->db == nullptr) return;
        self->calls.fetch_add(1);
        (void)self->db->stats();
    };
    options.logger = logger;
    options.min_log_level = LogLevel::Debug;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    reentrant.db = db->get();

    put_until_flush(**db, 400);
    ASSERT_EQ((*db)->flush(), Status::Ok);
    EXPECT_GT(reentrant.calls.load(), 0);
}

TEST(LoggerTest, CompactionIsReportedFromABackgroundThread) {
    TestStore store;
    Recorder recorder;
    Options options = make_options(store);
    options.logger = recorder.sink();
    options.min_log_level = LogLevel::Info;

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    for (int round = 0; round < 8; ++round) {
        put_until_flush(**db, 200, "round" + std::to_string(round));
        ASSERT_EQ((*db)->flush(), Status::Ok);
    }
    ASSERT_EQ((*db)->compact_level(0), Status::Ok);

    ASSERT_TRUE(recorder.saw(LogEvent::CompactionComplete));
    for (const Line& line : recorder.lines()) {
        if (line.event == LogEvent::CompactionComplete) {
            EXPECT_NE(line.message.find("L0->L1"), std::string::npos) << line.message;
        }
    }
}

/// The counter beside the log line: a log can be filtered or sampled, and an alert cannot fire on
/// one. This is what tells a dashboard the store is working through a degraded backend.
TEST(LoggerTest, ARetryableBackgroundFailureIsCounted) {
    TestStore store;
    auto faulty = std::make_shared<FaultInjectingBlobStore>(store.store(0));
    faulty->add_rule({.op = FaultInjectingBlobStore::Op::Put,
                      .name_contains = ".sst",
                      .first_match = 0,
                      .match_count = 1,
                      .status = Status::Io});

    Options options = make_options(store);
    options.tiers = {Tier{.store = faulty, .durability = Durability::Durable}};

    auto db = DB::open(options);
    ASSERT_TRUE(db.has_value());
    EXPECT_EQ((*db)->stats().background_failures, 0u);

    put_until_flush(**db, 200);
    (void)(*db)->flush();
    EXPECT_EQ((*db)->stats().background_failures, 1u)
        << "the failure the engine then retries past is the one nothing else records";

    put_until_flush(**db, 200, "second");
    ASSERT_EQ((*db)->flush(), Status::Ok);
    EXPECT_EQ((*db)->stats().background_failures, 1u)
        << "a successful flush must not inflate it";
}

}  // namespace
}  // namespace elysiumkv::test
