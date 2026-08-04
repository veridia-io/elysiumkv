#include "diff/op_stream.hpp"
#include "diff/replay.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

int env_int(const char* name, int fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : std::atoi(value);
}

/// The full profile is a nightly job; a PR runs a quick one under sanitizers. Both replay
/// identically from a seed, because both run in `BackgroundMode::Inline`.
///
/// **Many short streams, not a few long ones — and the reason is cost, not only coverage.**
/// A replay is *superlinear* in stream length: the op mix contains scans, and each scan
/// compares against an oracle that grows with the stream, so the work per operation rises as
/// the run goes on. Measured in release on one machine, one seed of `TransientBand`:
///
///     5k ops → 1.1 s     10k → 3.8 s     20k → 11.7 s     60k → 69 s      (≈ n^1.63)
///
/// ARCHITECTURE.md "The differential oracle" asks for "1M-op runs across at least 100 seeds", and that extrapolates to about
/// **1.9 hours per seed** — 190 hours for one config, and this suite has twelve. It was never
/// a schedulable amount of work, and the nightly had been timing out against the 600 s
/// per-test cap rather than running short.
///
/// So the seed count is kept at the "at least 100" and the **deviation is in stream length
/// alone**: 100 seeds of 5k ops. That is also the better trade for finding things — seed count
/// is what produces distinct starting states, while a long stream mostly re-treads the state
/// space its first few thousand operations already reached. Short streams shrink better too,
/// and shrinking is what makes a failing seed debuggable. Both engine bugs this suite has found
/// surfaced inside the first 2,400 operations of a 3,000-op stream.
///
/// **Size against the most expensive config, not the cheapest.** Per seed at 10k ops in
/// release, `TransientBand` costs 3.8 s and `CachedTightBudget` 10 s — a cache chain whose
/// every hit is re-read for verification, plus budget shedding, plus the continuous invariant
/// checks. Sizing off the cheap one put five configs over the 600 s cap on the first attempt.
/// At 5k ops the heaviest is ~3 s per seed, so ~5 minutes per config here and roughly a quarter
/// hour on a slower runner.
///
/// Override with ELYSIUMKV_DIFF_SEEDS / ELYSIUMKV_DIFF_OPS to go deeper deliberately.
bool full_profile() { return std::getenv("ELYSIUMKV_DIFF_FULL") != nullptr; }
int ops_per_run() { return env_int("ELYSIUMKV_DIFF_OPS", full_profile() ? 5000 : 3000); }
int seed_count() { return env_int("ELYSIUMKV_DIFF_SEEDS", full_profile() ? 100 : 3); }

std::string report(int seed, const ReplayConfig& config, const DiffFailure& failure,
                   size_t original_ops, const std::vector<DiffOp>& minimal,
                   const std::optional<DiffFailure>& minimal_failure) {
    std::string out = "\ndifferential mismatch\n";
    out += "  config:  " + config.name + "\n";
    out += "  seed:    " + std::to_string(seed) + "\n";
    out += "  failed:  operation " + std::to_string(failure.op_index) + " of " +
           std::to_string(original_ops) + "\n";
    out += "  message: " + failure.message + "\n";
    out += "\nshrunk to " + std::to_string(minimal.size()) + " operations";
    if (minimal_failure.has_value()) {
        out += ", failing at operation " + std::to_string(minimal_failure->op_index) + ": " +
               minimal_failure->message;
    } else {
        out += " (which no longer reproduces — the shrinker or the engine is nondeterministic)";
    }
    out += "\n\n" + describe_ops(minimal);
    out += "\nReproduce with:\n  ELYSIUMKV_DIFF_SEED=" + std::to_string(seed) +
           " ELYSIUMKV_DIFF_SEEDS=1 ELYSIUMKV_DIFF_OPS=" + std::to_string(original_ops) +
           " ./elysiumkv_tests --gtest_filter='*Differential*" + config.name + "*'\n";
    return out;
}

class DifferentialTest : public ::testing::TestWithParam<ReplayConfig> {};

TEST_P(DifferentialTest, MatchesTheOracle) {
    const ReplayConfig config = GetParam();
    const int ops_count = ops_per_run();
    const int first_seed = env_int("ELYSIUMKV_DIFF_SEED", 1);

    for (int seed = first_seed; seed < first_seed + seed_count(); ++seed) {
        const std::vector<DiffOp> ops = generate_ops(static_cast<uint64_t>(seed), ops_count);
        auto failure = replay(ops, config);
        if (!failure.has_value()) continue;

        // ARCHITECTURE.md "The differential oracle" — a mismatch deep in a million operations is close to
        // undebuggable, so minimize before reporting.
        const std::vector<DiffOp> minimal = shrink(ops, config);
        const auto minimal_failure = replay(minimal, config);
        FAIL() << report(seed, config, *failure, ops.size(), minimal, minimal_failure);
    }
}

// **Designated initializers, deliberately.** `ReplayConfig` is a plain aggregate and this
// list was written positionally; inserting a field in the middle of the struct then
// silently repurposed two configs — `cached` landed where `threaded` had been, so two cases
// that claimed to test the cache chain ran threaded and uncached instead, in the suite that
// shrinks (where a threaded replay is not reproducible and shrinking is meaningless).
// Naming the fields makes that class of mistake impossible rather than merely unlikely.
INSTANTIATE_TEST_SUITE_P(
    Configs, DifferentialTest,
    ::testing::Values(
        ReplayConfig{.name = "NoCompression", .compression = Compression::None},
        ReplayConfig{.name = "Lz4", .compression = Compression::Lz4},
        ReplayConfig{.name = "Zstd", .compression = Compression::Zstd},
        ReplayConfig{.name = "TwoStores", .compression = Compression::Zstd, .split_stores = true},
        ReplayConfig{
            .name = "TinyMemtable", .compression = Compression::None, .memtable_bytes = 16u << 10},
        ReplayConfig{.name = "TransientBand",
                     .compression = Compression::Zstd,
                     .split_stores = true,
                     .transient_band = true},
        // ARCHITECTURE.md "Caches chain" — the same oracle with a memory-over-disk chain in front of every tier. If a
        // cache is invisible to the engine, this cannot differ from `Zstd`.
        ReplayConfig{.name = "CachedChain", .compression = Compression::Zstd, .cached = true},
        // Two tiers, so migration copies a file between stores that both have caches over
        // them.
        ReplayConfig{.name = "CachedTiered",
                     .compression = Compression::Zstd,
                     .split_stores = true,
                     .memtable_bytes = 64u << 10,
                     .cached = true},
        // Two tiers *and* a small memtable, so migration runs constantly. `TwoStores` uses a
        // 1 MiB memtable, so this pressure had never been generated — and the first run of it
        // found a committed write reverting to its previous value
        // (`compact_l0_file_off_its_tier` choosing by write time rather than by file number).
        ReplayConfig{.name = "TieredHeavyMigration",
                     .compression = Compression::Zstd,
                     .split_stores = true,
                     .memtable_bytes = 64u << 10},
        // ARCHITECTURE.md "A process-wide memory budget" — a budget *below* the memtable size, so the arena crosses it before the
        // memtable is full and the flush is forced for a reason unrelated to the memtable.
        // That is the perturbation worth putting under the oracle.
        //
        // The vacuity check in the harness reported "never exceeded" for three budgets in a
        // row before this was believed — and it was the check that was wrong, reading the
        // shed counter off the instance the replay had just *reopened*. Worth remembering
        // as an argument for the check rather than against it: it was the thing that
        // eventually got looked at, instead of a config silently testing nothing.
        ReplayConfig{.name = "TightMemoryBudget",
                     .compression = Compression::Zstd,
                     .memtable_bytes = 64u << 10,
                     .budget_bytes = 48u << 10},
        // And with the chain, because the blob cache and the block cache compete for the same
        // budget — the substitution warning as an eviction race rather than as advice.
        ReplayConfig{.name = "CachedTightBudget",
                     .compression = Compression::Zstd,
                     .memtable_bytes = 64u << 10,
                     .cached = true,
                     .budget_bytes = 48u << 10}),
    [](const auto& scenario) { return scenario.param.name; });  // not `info`: gtest shadows it

/// ARCHITECTURE.md "The differential oracle" — the nightly randomized pass. Same op streams, same oracle, but the
/// flush and compaction threads run freely: this samples interleavings, which the
/// synchronous pass by construction cannot. A failure here is bisected into a
/// synchronous repro by hand — unpleasant, rare, and the price of finding it.
///
/// Kill points stay out: with a flush possibly in flight, "the oracle as of the
/// last successful flush" is not a single well-defined state. Everything else is
/// exact, because background work never changes logical content.
TEST(ThreadedDifferentialTest, MatchesTheOracleWithThreadsRunning) {
    // Same reshaping as the synchronous pass, and more so: this one samples *interleavings*,
    // which is a property of how often you start rather than of how long you run.
    const int ops = env_int("ELYSIUMKV_DIFF_OPS", full_profile() ? 5000 : 1500);
    const int seeds = env_int("ELYSIUMKV_DIFF_SEEDS", full_profile() ? 40 : 2);
    const int first_seed = env_int("ELYSIUMKV_DIFF_SEED", 1);

    const ReplayConfig configs[] = {
        ReplayConfig{.name = "ThreadedZstd",
                     .compression = Compression::Zstd,
                     .memtable_bytes = 64u << 10,
                     .threaded = true},
        ReplayConfig{.name = "ThreadedTransient",
                     .compression = Compression::Zstd,
                     .split_stores = true,
                     .memtable_bytes = 64u << 10,
                     .transient_band = true,
                     .threaded = true},
        // The caches under real interleaving: their locks are dropped around every fetch
        // from below, so concurrent readers of one range is the ordinary case.
        ReplayConfig{.name = "ThreadedCached",
                     .compression = Compression::Zstd,
                     .memtable_bytes = 64u << 10,
                     .cached = true,
                     .threaded = true},
        // ARCHITECTURE.md "A process-wide memory budget" — **the budget's stall loop is threaded-only code.** In inline mode the
        // writer performs the compaction itself and returns early, so the "wait only while
        // the overage is shrinking" loop never runs there. Its entire exercise until now
        // was one Java test, which hung on the first attempt at it; nothing ran it under
        // TSan, and it interacts with the background flusher and the frozen-memtable
        // handoff.
        ReplayConfig{.name = "ThreadedTightBudget",
                     .compression = Compression::Zstd,
                     .memtable_bytes = 64u << 10,
                     .budget_bytes = 48u << 10,
                     .threaded = true},
    };

    for (const ReplayConfig& config : configs) {
        for (int seed = first_seed; seed < first_seed + seeds; ++seed) {
            const std::vector<DiffOp> ops_list = generate_ops(static_cast<uint64_t>(seed), ops);
            auto failure = replay(ops_list, config);
            if (!failure.has_value()) continue;

            // No shrinking here: without reproducibility, delta-debugging cannot
            // tell a fix from luck.
            FAIL() << "\nthreaded differential mismatch\n  config:  " << config.name
                   << "\n  seed:    " << seed << "\n  failed:  operation "
                   << failure->op_index << " of " << ops_list.size() << "\n  message: "
                   << failure->message
                   << "\n\nThreaded failures are not reproducible by seed alone; bisect into a "
                      "synchronous repro by hand.\n";
        }
    }
}

/// ARCHITECTURE.md "Fault injection" — **non-overlap safety.** Once a tombstone is dropped at a level that
/// was bottommost for its range, no later compaction may place an older value
/// for that key beneath it. The argument is that levels below L0 hold each key
/// in at most one file, so a surviving older value would itself make a deeper
/// level overlap the range and the drop would not have happened; and compaction
/// only ever moves data downward.
///
/// A narrow key range makes deletes, rewrites and compactions collide constantly,
/// which is what turns that argument into a test.
TEST(TombstoneSafetyTest, NoValueEverReappearsBeneathADroppedTombstone) {
    ReplayConfig config{"NarrowRange", Compression::None, false, 16u << 10};
    // Left long on purpose, unlike the two above. This one narrows the keyspace to 20 keys,
    // so the oracle stays tiny and the scans with it: measured at ~29 s for 200k ops, near
    // enough to linear. The whole point here is *depth* — deletes and rewrites colliding on
    // the same keys over and over — which is the one place a long stream earns its cost.
    const int ops = env_int("ELYSIUMKV_DIFF_OPS", full_profile() ? 200000 : 4000);
    const int seeds = env_int("ELYSIUMKV_DIFF_SEEDS", full_profile() ? 20 : 3);

    GeneratorOptions generator;
    generator.distinct_keys = 20;  // every operation lands on top of another

    for (int seed = 1; seed <= seeds; ++seed) {
        const std::vector<DiffOp> stream =
            generate_ops(static_cast<uint64_t>(seed) * 31, ops, generator);
        auto failure = replay(stream, config);
        if (!failure.has_value()) continue;

        const std::vector<DiffOp> minimal = shrink(stream, config);
        FAIL() << "\na deleted key came back\n  seed:    " << seed << "\n  failed:  operation "
               << failure->op_index << "\n  message: " << failure->message << "\n\nshrunk to "
               << minimal.size() << " operations\n\n"
               << describe_ops(minimal);
    }
}

// --- properties of the harness itself -----------------------------------------

/// The whole point of the synchronous mode: the same op list produces the same
/// outcome every time. Without it a failing seed is a lottery ticket.
TEST(DifferentialHarness, ReplayIsDeterministic) {
    const ReplayConfig config{"Determinism", Compression::Zstd, false, 32u << 10};
    const std::vector<DiffOp> ops = generate_ops(99, 800);

    const auto first = replay(ops, config);
    const auto second = replay(ops, config);
    EXPECT_EQ(first.has_value(), second.has_value());
    if (first.has_value() && second.has_value()) {
        EXPECT_EQ(first->op_index, second->op_index);
        EXPECT_EQ(first->message, second->message);
    }
}

TEST(DifferentialHarness, GeneratorIsAFunctionOfItsSeed) {
    const auto a = generate_ops(12345, 500);
    const auto b = generate_ops(12345, 500);
    const auto c = generate_ops(12346, 500);

    ASSERT_EQ(a.size(), b.size());
    EXPECT_EQ(describe_ops(a), describe_ops(b));
    EXPECT_NE(describe_ops(a), describe_ops(c));
}

/// The shrinker is the highest-leverage part of the harness, so it gets a test
/// of its own rather than being trusted to work the day something breaks. The
/// predicate here fails only when two specific operations are both present, so
/// a correct shrinker reduces a long stream to exactly those two.
TEST(DifferentialHarness, ShrinkerMinimizesToTheOperationsThatMatter) {
    std::vector<DiffOp> ops = generate_ops(7, 400);

    DiffOp culprit_one;
    culprit_one.kind = DiffOp::Kind::Put;
    culprit_one.key = "the-culprit";
    culprit_one.value = "one";

    DiffOp culprit_two;
    culprit_two.kind = DiffOp::Kind::Get;
    culprit_two.key = "the-culprit";

    ops.insert(ops.begin() + 120, culprit_one);
    ops.insert(ops.begin() + 300, culprit_two);

    auto contains = [](const std::vector<DiffOp>& list, const std::string& key,
                       DiffOp::Kind kind) {
        for (const DiffOp& op : list) {
            if (op.kind == kind && op.key == key) return true;
        }
        return false;
    };
    auto still_fails = [&](const std::vector<DiffOp>& candidate) {
        return contains(candidate, "the-culprit", DiffOp::Kind::Put) &&
               contains(candidate, "the-culprit", DiffOp::Kind::Get);
    };

    const std::vector<DiffOp> minimal = shrink(ops, still_fails);
    EXPECT_EQ(minimal.size(), 2u) << describe_ops(minimal);
    EXPECT_TRUE(still_fails(minimal));

    // And the report is something a person can read.
    const std::string rendered = describe_ops(minimal);
    EXPECT_NE(rendered.find("the-culprit"), std::string::npos) << rendered;
}

TEST(DifferentialHarness, ShrinkerLeavesAnAlreadyMinimalStreamAlone) {
    std::vector<DiffOp> ops = generate_ops(3, 5);
    const std::vector<DiffOp> minimal = shrink(ops, [](const std::vector<DiffOp>& candidate) {
        return candidate.size() == 5;  // only the full list "fails"
    });
    EXPECT_EQ(minimal.size(), 5u);
}

}  // namespace
}  // namespace elysiumkv::test
