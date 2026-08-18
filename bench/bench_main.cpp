#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "db/db_impl.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
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

// --- the write path --------------------------------------------------------------
//
// **Every benchmark above is a read against a store that was already built.** Nothing measured the
// cost of building it, so nothing gated write amplification, flush cost or compaction throughput —
// and write amplification is the number the level configuration exists to control. A change that
// doubles the bytes compaction writes per byte the application writes passes every other gate here.
//
// Reported as a ratio rather than a duration because that is the part which is a property of the
// engine rather than of the disk under it: `compaction_bytes_written / bytes_the_caller_wrote`.

/// Writes a fixed volume into a fresh store and compacts to quiescence, so the numbers describe one
/// complete cycle rather than wherever the background happened to be when time ran out.
void BM_WriteAmplification(benchmark::State& state) {
    const int keys = static_cast<int>(state.range(0));

    double amplification = 0;
    double flushes = 0;
    double compactions = 0;
    for (auto _ : state) {
        state.PauseTiming();
        test::TempDir dir;
        std::filesystem::create_directories(dir.path() / "store");
        auto store = std::make_shared<DiskBlobStore>(dir.path() / "store", "bench");
        store->set_sync_writes(false);

        Options options;
        options.manifest_catalog = std::make_shared<DiskManifestCatalog>(dir.path());
        options.memtable_bytes = 1u << 20;
        options.background = BackgroundMode::Inline;
        LevelOptions l0;
        l0.max_files = 4;
        LevelOptions l1;
        l1.max_bytes = 4u << 20;
        LevelOptions l2;
        options.levels = {{0, l0}, {1, l1}, {2, l2}};
        options.tiers = {Tier{.store = store, .durability = Durability::Durable}};

        auto db = std::move(*DB::open(options));
        const std::string value(200, 'v');
        state.ResumeTiming();

        // **Shuffled, not ascending.** Written in key order every L0 file holds a disjoint range,
        // so each compaction is a trivial move and the amplification is zero — a true number about
        // a workload nothing has, and a benchmark that could not see a regression in the merge.
        std::vector<int> order(static_cast<size_t>(keys));
        for (int i = 0; i < keys; ++i) order[static_cast<size_t>(i)] = i;
        std::mt19937 shuffle(7);
        std::shuffle(order.begin(), order.end(), shuffle);

        uint64_t written = 0;
        for (const int i : order) {
            const std::string key = key_at(i);
            (void)db->put(Slice::from(key), Slice::from(value));
            written += key.size() + value.size();
        }
        (void)db->flush();
        (void)static_cast<DbImpl&>(*db).compact_until_quiet();

        state.PauseTiming();
        const Stats stats = db->stats();
        amplification = written == 0 ? 0
                                     : static_cast<double>(stats.compaction_bytes_written) /
                                           static_cast<double>(written);
        flushes = static_cast<double>(stats.flushes);
        compactions = static_cast<double>(stats.compactions);
        state.ResumeTiming();
    }

    // **The gate.** Bytes compaction rewrote per byte the caller wrote, for one full cycle. It is a
    // property of the level configuration and the picker, not of the machine, so it moves only when
    // one of those changes.
    state.counters["write_amp"] = benchmark::Counter(amplification);
    state.counters["flushes"] = benchmark::Counter(flushes);
    state.counters["compactions"] = benchmark::Counter(compactions);
}
BENCHMARK(BM_WriteAmplification)->Arg(50000)->Unit(benchmark::kMillisecond);

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
