#include "support/temp_dir.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <benchmark/benchmark.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <string>

namespace elysiumkv {
namespace {

/// ARCHITECTURE.md "Benchmarks" — reported, not gating.

/// 100 keys per cluster whatever the store holds, so a prefix scan returns the
/// same number of keys at every store size — which is what makes the scaling
/// question in BM_PrefixScan meaningful.
std::string key_at(int i) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "cluster:%06d:key:%08d", i / 100, i);
    return buf;
}

/// A store that lives for the whole benchmark run, so setup cost is not counted
/// per iteration.
class BenchStore {
public:
    explicit BenchStore(int keys) {
        std::filesystem::create_directories(dir_.path() / "store");
        auto store = std::make_shared<DiskBlobStore>(dir_.path() / "store", "bench");
        store->set_sync_writes(false);

        Options options;
        options.manifest_catalog = std::make_shared<DiskManifestCatalog>(dir_.path());
        options.memtable_bytes = 32u << 20;
        // **No engine threads during measurement.** Every benchmark here is a read against a store
        // that is already built and flushed, so background work has nothing to do — but the
        // maintenance coordinator still wakes on its interval, and whether a tick lands inside a
        // given repetition is luck. That turns into a few percent of spread between repetitions,
        // which is noise about the scheduler rather than about the engine.
        options.background = BackgroundMode::Inline;

        LevelOptions l0;
        l0.max_files = 100;
        LevelOptions l1;
        options.levels = {{0, l0}, {1, l1}};
        options.tiers = {Tier{.store = store, .durability = Durability::Durable}};

        db_ = std::move(*DB::open(options));
        const std::string value(100, 'v');
        for (int i = 0; i < keys; ++i) {
            (void)db_->put(Slice::from(key_at(i)), Slice::from(value));
        }
        (void)db_->flush();
    }

    DB& db() { return *db_; }

private:
    test::TempDir dir_;
    std::unique_ptr<DB> db_;
};

void BM_PointLookupHot(benchmark::State& state) {
    static BenchStore store(100000);
    const std::string key = key_at(42);
    for (auto _ : state) {
        auto found = store.db().get(Slice::from(key));
        benchmark::DoNotOptimize(found);
    }
}
BENCHMARK(BM_PointLookupHot);

void BM_PointLookupRandom(benchmark::State& state) {
    static BenchStore store(100000);
    std::mt19937_64 rng(7);
    for (auto _ : state) {
        const std::string key = key_at(static_cast<int>(rng() % 100000));
        auto found = store.db().get(Slice::from(key));
        benchmark::DoNotOptimize(found);
    }
}
BENCHMARK(BM_PointLookupRandom);

/// The negative lookup the bloom filter exists to make cheap.
void BM_PointLookupBloomRejected(benchmark::State& state) {
    static BenchStore store(100000);
    std::mt19937_64 rng(7);
    for (auto _ : state) {
        const std::string key = "absent:" + std::to_string(rng());
        auto found = store.db().get(Slice::from(key));
        benchmark::DoNotOptimize(found);
    }
}
BENCHMARK(BM_PointLookupBloomRejected);

/// ARCHITECTURE.md "Absence is an answer, not an error" and ARCHITECTURE.md "Benchmarks" — **a prefix scan must not scale with total key count.** The same
/// 100-key prefix is scanned in a 100k-key store and a 1M-key store; ten times
/// the keyspace must cost about the same, or SST pruning is not working.
/// Built once per size and kept, like the point-lookup stores.
///
/// **A local would be rebuilt several times per reported number**, not once: the framework calls a
/// benchmark repeatedly to find an iteration count and then once more per repetition, so a million
/// keys were being written and flushed on each of those calls. That is most of the runtime, and it
/// leaves the page cache and allocator in a different state each time — measurable as spread in the
/// thing being measured.
BenchStore& prefix_store(int keys) {
    if (keys == 100000) {
        static BenchStore small(100000);
        return small;
    }
    static BenchStore large(1000000);
    return large;
}

void BM_PrefixScan(benchmark::State& state) {
    const int keys = static_cast<int>(state.range(0));
    BenchStore& store = prefix_store(keys);

    const std::string prefix = "cluster:000007:";
    int64_t scanned = 0;
    for (auto _ : state) {
        auto it = store.db().prefix_iterator(Slice::from(prefix));
        while (it->next()) {
            benchmark::DoNotOptimize(it->value());
            ++scanned;
        }
    }
    state.counters["keys_scanned"] =
        benchmark::Counter(static_cast<double>(scanned), benchmark::Counter::kAvgIterations);
}
BENCHMARK(BM_PrefixScan)->Arg(100000)->Arg(1000000);

/// ARCHITECTURE.md "Negative controls" — the subject of the ratchet's negative control, and nothing else.
///
/// The gate under test is `check_regression.py`, not the engine, so the engine
/// is not the thing to slow down: this benchmark's duration comes from the
/// environment. Running it fast, recording a baseline, then running it slow must
/// trip the threshold *and name this benchmark* — which is what distinguishes a
/// working ratchet from one whose filter matches nothing.
void BM_RatchetControl(benchmark::State& state) {
    const char* configured = std::getenv("ELYSIUMKV_RATCHET_SPIN_NS");
    const auto spin_ns = configured == nullptr ? 1000 : std::atoi(configured);
    for (auto _ : state) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::nanoseconds(spin_ns);
        auto now = std::chrono::steady_clock::now();
        while (now < deadline) {
            now = std::chrono::steady_clock::now();
            benchmark::DoNotOptimize(now);
        }
    }
}
BENCHMARK(BM_RatchetControl);

}  // namespace
}  // namespace elysiumkv

BENCHMARK_MAIN();
