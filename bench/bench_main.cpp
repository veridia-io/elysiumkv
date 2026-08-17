#include "fault/fault_injecting_blob_store.hpp"
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

// --- the remote path ------------------------------------------------------------
//
// **Every benchmark above measures local disk, and the design target is object storage.** That gap
// is not a nicety: a change that triples the number of round trips a read costs passes all of them,
// because on local disk a round trip is a syscall. The engine's read cost against S3 is round
// trips, so that is what these count.
//
// `gets_per_op` is the number to watch, and it is deterministic — a timing under injected latency
// is the same statement with noise added, and is here so the cost is legible in wall-clock terms
// too. Latency is an argument in microseconds rather than a fixed 30 ms because a realistic S3
// figure makes each iteration so slow that the harness gathers no samples.

/// Local disk behind a latency injector, with the caches sized so reads actually reach it: a
/// benchmark whose working set fits in the block cache measures the cache.
class RemoteishStore {
public:
    RemoteishStore(int keys, std::chrono::microseconds latency) {
        std::filesystem::create_directories(dir_.path() / "store");
        auto disk = std::make_shared<DiskBlobStore>(dir_.path() / "store", "bench");
        disk->set_sync_writes(false);
        store_ = std::make_shared<test::FaultInjectingBlobStore>(disk);

        Options options;
        options.manifest_catalog = std::make_shared<DiskManifestCatalog>(dir_.path());
        options.memtable_bytes = 32u << 20;
        options.background = BackgroundMode::Inline;
        // Small enough that a reader is evicted between lookups, so the cost of *opening* one is
        // in the measurement — which is the cost C2 is about.
        options.reader_cache_bytes = 64u << 10;

        LevelOptions l0;
        l0.max_files = 100;
        LevelOptions l1;
        options.levels = {{0, l0}, {1, l1}};
        options.tiers = {Tier{.store = store_, .durability = Durability::Durable}};

        db_ = std::move(*DB::open(options));
        const std::string value(100, 'v');
        for (int i = 0; i < keys; ++i) {
            (void)db_->put(Slice::from(key_at(i)), Slice::from(value));
        }
        (void)db_->flush();

        // Injected only once the store is built: charging the build would measure nothing useful
        // and would take minutes.
        store_->set_latency(latency);
    }

    DB& db() { return *db_; }
    uint64_t gets() const {
        return store_->call_count(test::FaultInjectingBlobStore::Op::Get);
    }

private:
    test::TempDir dir_;
    std::shared_ptr<test::FaultInjectingBlobStore> store_;
    std::unique_ptr<DB> db_;
};

void BM_RemotePointLookup(benchmark::State& state) {
    constexpr int kKeys = 200000;
    RemoteishStore store(kKeys, std::chrono::microseconds(state.range(0)));

    std::mt19937 rng(1234);
    std::uniform_int_distribution<int> pick(0, kKeys - 1);

    const uint64_t before = store.gets();
    const uint64_t opens_before = store.db().stats().reader_cache_misses;
    int64_t lookups = 0;
    for (auto _ : state) {
        auto found = store.db().get(Slice::from(key_at(pick(rng))));
        benchmark::DoNotOptimize(found);
        ++lookups;
    }
    const uint64_t spent = store.gets() - before;
    const uint64_t opens = store.db().stats().reader_cache_misses - opens_before;
    const auto per = static_cast<double>(lookups ? lookups : 1);

    // **The gate.** Round trips per lookup, independent of the machine and of the injected
    // latency. Reported beside the number of readers opened, because the two together are what
    // make the figure explicable: a lookup costs one read per reader it has to open plus one per
    // data block it has to fetch, and a configuration where the reader cache absorbs the opens is
    // measuring the cache rather than the read path.
    state.counters["gets_per_op"] = benchmark::Counter(static_cast<double>(spent) / per);
    state.counters["opens_per_op"] = benchmark::Counter(static_cast<double>(opens) / per);
}
BENCHMARK(BM_RemotePointLookup)->Arg(0)->Arg(200)->Unit(benchmark::kMicrosecond);

void BM_RemotePrefixScan(benchmark::State& state) {
    constexpr int kKeys = 200000;
    RemoteishStore store(kKeys, std::chrono::microseconds(state.range(0)));

    const uint64_t before = store.gets();
    int64_t scans = 0;
    for (auto _ : state) {
        auto it = store.db().prefix_iterator(Slice::from(std::string("cluster:000123:")));
        int64_t seen = 0;
        while (it->next()) ++seen;
        benchmark::DoNotOptimize(seen);
        ++scans;
    }
    const uint64_t spent = store.gets() - before;

    // A scan opens every candidate file up front (C3), so this counter is where that shows.
    state.counters["gets_per_scan"] =
        benchmark::Counter(static_cast<double>(spent) / static_cast<double>(scans ? scans : 1));
}
BENCHMARK(BM_RemotePrefixScan)->Arg(0)->Arg(200)->Unit(benchmark::kMicrosecond);

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
