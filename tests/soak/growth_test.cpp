#include "support/resident_memory.hpp"
#include "support/sanitizers.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/memory_budget.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

int env_int(const char* name, int fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : std::atoi(value);
}

std::string key_at(int i) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "cluster:%04d:key:%08d", i % 64, i);
    return buf;
}

/// ARCHITECTURE.md "Benchmarks" — bounded growth. A steady-state workload over a bounded keyspace
/// must plateau in resident memory. This is the one suite that deliberately runs
/// in `BackgroundMode::Threaded`: the soak and the TSan runs are what the
/// threaded path exists to exercise.
TEST(Soak, ResidentMemoryPlateausUnderSteadyState) {
    if (resident_bytes() == 0) GTEST_SKIP() << "resident memory is not readable here";
    if (running_under_sanitizer()) {
        // A sanitizer's redzones and quarantine keep RSS climbing on their own
        // account, which says nothing about the engine.
        GTEST_SKIP() << "resident memory is not the engine's under a sanitizer";
    }

    const int ops = env_int("ELYSIUMKV_SOAK_OPS", 120000);
    const int distinct_keys = 4000;  // bounded: the dataset does not grow

    TestStore store;
    Options options = make_options(store, Compression::Lz4, 256u << 10);
    options.background = BackgroundMode::Threaded;

    // ARCHITECTURE.md "A process-wide memory budget" — the claim an embedder actually cares about is that a budget bounds resident
    // memory, and this is the only suite that measures resident memory at all. Until now
    // the budget's evidence was six short targeted tests: it was charged correctly and it
    // shed when asked. That it *holds a process down over time* was untested.
    //
    // Deliberately smaller than the memtable, so shedding is continuous rather than a rare
    // event at the end of a long run.
    auto budget = std::make_shared<MemoryBudget>(128u << 10);
    options.memory_budget = budget;

    // Sized to the budget, or this measures a mismatch rather than the budget. A compaction
    // holds two windows per input and the charge is unconditional, so the default 2 MiB window puts
    // 4 MiB against a 128 KiB budget — thirty-two times the limit from one input, arriving and
    // leaving with a background compaction. That is what made this test flaky: resident memory did
    // not drift upward, the *budget* stepped by 4 MiB whenever the final read landed inside one.
    //
    // Refusing such a pairing at `open` was tried and reverted: five configurations in this suite
    // deliberately pair a tiny budget with the default window to force shedding, and they are not
    // wrong — the engine works, the budget simply reports memory it does not bound.
    options.compaction_window_bytes = 16u << 10;
    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);

    std::mt19937_64 rng(20260802);
    std::vector<size_t> samples;
    const std::string value(120, 'v');

    for (int op = 0; op < ops; ++op) {
        const std::string key = key_at(static_cast<int>(rng() % static_cast<unsigned>(distinct_keys)));
        if (rng() % 8 == 0) {
            ASSERT_EQ(db->remove(Slice::from(key)), Status::Ok);
        } else {
            ASSERT_EQ(db->put(Slice::from(key), Slice::from(value)), Status::Ok);
        }
        if (rng() % 4 == 0) (void)db->get(Slice::from(key));
        if (op % 1000 == 0) samples.push_back(resident_bytes());
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_GE(samples.size(), 30u);

    // Compare the tail against the middle, skipping the first third as warm-up:
    // steady state should not keep climbing.
    const size_t third = samples.size() / 3;
    const size_t middle_peak = *std::max_element(samples.begin() + static_cast<long>(third),
                                                 samples.begin() + static_cast<long>(2 * third));
    const size_t tail_peak = *std::max_element(samples.begin() + static_cast<long>(2 * third),
                                               samples.end());

    // Non-vacuity: the measurement has to move, or the assertion below is
    // asserting nothing.
    //
    // Phrased as spread rather than as "the tail exceeds the first sample", which is what this
    // asked before. That form assumed resident memory climbs from the first sample onward — an
    // assumption the OS is free to break, since it reclaims pages under pressure whatever the
    // process is doing, and one in tension with the very property under test. It failed on a
    // machine where RSS *fell* over the run, reporting a broken measurement when the measurement
    // was working. What the guard needs to rule out is a constant, and a constant has no spread.
    ASSERT_GT(middle_peak, 0u);
    const auto [low, high] = std::minmax_element(samples.begin(), samples.end());
    EXPECT_GT(*high, *low)
        << "resident memory never moved — is it being measured at all?";

    const double growth = static_cast<double>(tail_peak) / static_cast<double>(middle_peak);
    EXPECT_LT(growth, 1.25) << "resident memory grew " << (growth - 1.0) * 100
                            << "% between the middle and the tail of a steady-state run: "
                            << middle_peak / (1u << 20) << " MiB -> " << tail_peak / (1u << 20)
                            << " MiB";

    // The budget did work rather than merely existing — a budget this small under this
    // many writes that never shed would mean the plumbing came undone, and the growth
    // assertion above would then be measuring an engine with no budget at all.
    const Stats stats = db->stats();
    EXPECT_GT(stats.budget_sheds, 0u) << "the budget never shed, so it was not the thing "
                                         "holding this run down";
    EXPECT_EQ(stats.memory_budget_total, 128u << 10);

    // Bounded, not merely non-growing. `used()` may exceed the total transiently — an
    // arena charges for a write already accepted — but it must not run away, or the budget
    // is a counter rather than a limit. A few multiples of the total is the honest bound:
    // the overage is one memtable's worth of arena plus whatever the caches hold between
    // shedding passes.
    EXPECT_LT(budget->used(), 8u * (128u << 10))
        << "the budget was exceeded by " << budget->used() / 1024 << " KiB against a "
        << (128u << 10) / 1024 << " KiB limit";
}

}  // namespace
}  // namespace elysiumkv::test
