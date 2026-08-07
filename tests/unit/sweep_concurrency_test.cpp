#include "db/db_impl.hpp"
#include "support/test_db.hpp"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
namespace elysiumkv::test { namespace {
// The orphan sweep must be safe to call while the maintenance executor is also sweeping. The
// executor's own sweep is gated on the injected clock, so the overlap only happens once the clock
// has advanced past its next-sweep deadline -- which is what makes this reproduce only on a machine
// slow enough for the coordinator to tick inside the test.
TEST(SweepConcurrency, ConcurrentSweepsDoNotRaceOnTheObservationTable) {
    TestStore store{1};
    std::atomic<uint64_t> now{1'000'000};
    Options options = make_options(store, Compression::None, 8u << 10);
    options.background = BackgroundMode::Threaded;
    options.maintenance_interval = Duration(1);
    options.clock = [&] { return now.load(); };
    options.orphan_sweep_interval = Duration(1);
    options.orphan_retention = Duration(60'000);

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(*opened);
    auto& engine = static_cast<DbImpl&>(*db);

    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(db->put(Slice::from("k" + std::to_string(i)), Slice::from("v")), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(store.store(0)->put(sst_object_name(900'000 + static_cast<uint64_t>(i)),
                                      Slice::from(std::string(32, 'x'))).get(), Status::Ok);
    }

    // Past the executor's next-sweep deadline, so its gate opens and it sweeps alongside us.
    for (int round = 0; round < 200; ++round) {
        now.fetch_add(1'000);
        (void)engine.sweep_orphans_for_test();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_FALSE(engine.current_version()->all_files().empty()) << "the store is still usable";
}
}}
