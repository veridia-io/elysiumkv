# ElysiumKV

An embedded LSM key-value store in C++, with a C ABI and a Java binding.

ElysiumKV is built around one idea that most embedded stores leave to the operator:
**where a file lives is decided per file, by its age, and it is decided
continuously.** Hot data sits on fast storage, cold data migrates to cheap
storage, and the store keeps working while it happens. Everything else — leveled
compaction, bloom filters, a block cache, prefix scans — is there to make that
useful rather than to be novel.

> **Status: early.** The engine, the C ABI and the Java binding are
> complete and heavily tested, but nothing has run in production. The API is not
> stable, and there has been no security review. Treat it as something to read,
> build on and break — not yet as something to store your data in.

---

## Contents

- [What it does](#what-it-does)
- [Storage tiers](#storage-tiers)
- [Remote storage](#remote-storage)
- [Quick start (C++)](#quick-start-c)
- [Quick start (Java)](#quick-start-java)
- [Memory](#memory)
- [Concurrency](#concurrency)
- [Reading: which path to use](#reading-which-path-to-use)
- [Limits](#limits)
- [Building](#building)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [What is not implemented](#what-is-not-implemented)
- [Contributing](#contributing)
- [License](#license)

## What it does

- **Ordered key-value storage** — put, delete, point lookup, range scan, prefix
  scan, atomic batches.
- **Leveled compaction** with a background thread, write stalls and tombstone
  reclamation.
- **Storage tiers**: several object stores per database, with files migrating
  between them by age (see below).
- **Concurrent readers**: any number of processes may open a store read-only while
  one writes it, with no registration and no coordination — see
  [Concurrency](#concurrency).
- **A durable watermark**: tell the store where you have reached in the log you
  are replaying, and it hands the position back at the next open — including
  rolled back to what a lost transient tier could not have held. This is the
  resume point for a changelog-backed store; see
  [ARCHITECTURE.md](ARCHITECTURE.md#the-watermark-is-an-interval-and-only-its-lower-bound-is-load-bearing).
- **Zero-copy reads**: a lookup can hand back a pointer into the block cache,
  pinned until you release it, with no copy at any layer — including through the
  Java binding.
- **Pluggable storage**: the object store and the manifest catalog are interfaces.
  A local-directory implementation of each ships, and the C ABI exposes them as
  function-pointer vtables so a binding can supply its own.
- **Bindings**: a stable C ABI (52 functions, C99) and a Java binding over JNI
  needing only Java 11, plus a Kafka Streams state store in
  `bindings/kafka-streams-v3`.

Keys are ordered as unsigned bytes. There are no column families, no snapshots and
no sequence numbers — recency is positional, decided by which level and which file
an entry lives in. That is a deliberate reduction, and much of the engine's
simplicity follows from it.

## Storage tiers

A **level** is LSM structure: overlap, capacity, compression, what compacts into
what. A **tier** is storage: which object store physically holds a file. They are
independent axes, and a single level routinely spans several tiers.

Placement is a pure function of a file's age, evaluated continuously, and it only
ever moves a file colder:

```cpp
options.tiers = {
    {.store = nvme, .durability = Durability::Durable, .max_age = hours(6)},
    {.store = ssd,  .durability = Durability::Durable, .max_age = days(7)},
    {.store = bulk, .durability = Durability::Durable},   // the last tier is unbounded
};
```

Migration is a byte-for-byte copy — no merge, no decode — which is why compression
is a property of the *level* rather than the tier: a file keeps its codec when it
moves.

A tier may be declared `Transient`, meaning its store is allowed to vanish — a
local NVMe cache, an ephemeral instance disk. If it does, that store is discarded
whole at open and reported. This is not the same as "missing recent writes":
dropping newer files uncovers older values underneath, so reads afterwards can
return **stale** data rather than no data. `DB::open` therefore refuses any
configuration containing a transient tier outright; `open_with_result` accepts it
and tells you what was lost, and the flag it sets stays until you acknowledge it.

## Remote storage

The cold tier can be S3, and the manifest can live in S3 or DynamoDB. This is what
makes a tier actually cold rather than a second directory on the same disk.

```java
try (S3BlobStore cold = S3BlobStore.builder("my-bucket").prefix("cold").open();
     DynamoManifestCatalog catalog =
             DynamoManifestCatalog.builder("elysiumkv-manifest", "orders").open()) {
    // credentials come from the SDK's own chain: environment, profile, instance role
}
```

Two things worth knowing before choosing between the catalogs:

- **The manifest pointer swap is the commit point, and it sits on every flush and
  every compaction.** DynamoDB pays single-digit milliseconds for it; S3 pays around
  50 ms. Prefer `DynamoManifestCatalog` unless having one fewer service matters more,
  which is what `S3ManifestCatalog` is for.
- **The pointer swap is a compare-and-swap, and a lost one means this process has
  been fenced** — another writer installed first, its own view is stale, and the
  correct response is to reopen rather than retry. That is reported as
  `FencedException`, distinct from the retryable I/O class.

Reads use two timeout profiles, not one: compaction reads whole files while a point
lookup reads a footer, and a single budget cannot serve both. Objects at or above
`multipart_threshold_bytes` are uploaded in parts on the bulk profile, so a failure costs
one part rather than the whole object — and the completion carries `If-None-Match: *`, so
write-once holds for a multipart upload exactly as it does for a single PUT.

**Put a cache in front of it.** Anything faster than an authoritative store is a
decorator over the same interface, so they compose and the engine never learns they
exist:

```java
S3BlobStore remote = S3BlobStore.builder("my-bucket").prefix("cold").open();
DiskCacheBlobStore cold = new DiskCacheBlobStore("/var/cache/elysiumkv", remote, 20L << 30, true);
```

`cache_on_write` populates on write — write-through, never write-back, so a cache is
never authoritative even briefly. That matters most for L0, whose files are read almost
immediately by the next L0→L1 compaction.

Two things worth knowing before stacking more:

- **A memory cache earns its place over a *remote* delegate and mostly not otherwise.**
  Over local files it duplicates the OS page cache, which does the same job with better
  eviction for free; spend the memory on a larger block cache instead, since that one
  holds decoded blocks.
- **Over hot data the block cache and a memory blob cache are substitutes, not
  complements.** The block cache intercepts first, so a range held in both is stored
  twice and read once.

Caches need no fsync and no crash-consistency protocol, and are wiped at startup: the
authoritative store is acknowledged before anything is cached, so a lost entry costs one
refetch. A cache may never be the innermost store of a tier — it holds only copies —
and `open` rejects that configuration.

These are an **optional native component** — the AWS SDK is by far the heaviest
dependency in the build, and an embedder with no remote tier should not pay for it.
Build with `-DELYSIUMKV_BUILD_AWS=ON`. The constructors exist either way and fail with
a configuration error naming the missing option when they are absent, because an ABI
whose shape depends on how it was compiled cannot be checked by a binding;
`ElysiumKV.hasAwsSupport()` answers the question up front.

## Quick start (C++)

```cpp
#include "elysiumkv/db.hpp"
#include "elysiumkv/file_manifest_catalog.hpp"
#include "elysiumkv/local_file_blob_store.hpp"

elysiumkv::Options options;
options.manifest_catalog = std::make_shared<elysiumkv::FileManifestCatalog>("/data");
options.tiers = {{.store = std::make_shared<elysiumkv::LocalFileBlobStore>("/data/store", "hot"),
                  .durability = elysiumkv::Durability::Durable}};
options.levels = {{0, {.compression = elysiumkv::Compression::None, .max_files = 4}},
                  {1, {.compression = elysiumkv::Compression::Zstd}}};

auto opened = elysiumkv::DB::open(options);
if (!opened) return handle(opened.error());
auto db = std::move(*opened);

db->put(key, value);

if (auto found = db->get(key)) {
    consume(found->value());          // borrowed from the block cache; no copy
} else if (found.error() == elysiumkv::Status::NotFound) {
    // absent — and only NotFound means absent
}

auto it = db->prefix_iterator(prefix);
while (it->next()) consume(it->key(), it->value());
it->status();                         // exhaustion and failure look alike otherwise

auto down = db->reverse_iterator(lo, hi);   // next() still advances — downwards
while (down->next()) consume(down->key(), down->value());
```

Every scan has a descending twin (`reverse_iterator`, `reverse_prefix_iterator`,
and `reverseIterator`/`reversePrefixIterator` in Java). **Bounds keep their
meaning in both directions** — `lo` inclusive, `hi` exclusive — so the two
describe the same set of keys and differ only in delivery order. A scan chooses a
direction once: there is no `prev()`, because turning around mid-scan would cost a
re-seek of every underlying source at each turn.

Errors are values: the public API returns `std::expected`, and exceptions never
cross it. **Absence is a distinct status, and nothing else is folded into it** — an
unreachable store reports `Status::Io`, never "not found", because an API that
confuses the two turns an outage into apparent data loss.

## Quick start (Java)

```java
try (LocalFileBlobStore store = new LocalFileBlobStore("/data/store", "hot");
     FileManifestCatalog catalog = new FileManifestCatalog("/data");
     ElysiumKVOptions options = new ElysiumKVOptions()
             .manifestCatalog(catalog)
             .addTier(store, Durability.DURABLE, 0, 0, 0, 0)
             .level(0, Compression.NONE, 0, 4, 8, 12, 0)
             .level(1, Compression.ZSTD, 0, 0, 0, 0, 0);
     ElysiumKV db = ElysiumKV.open(options)) {

    db.put(key, value);

    try (Pinned pinned = db.get(key)) {        // null, and only null, means absent
        if (pinned != null) consume(pinned.value());
    }

    try (BatchedIterator scan = db.batchedPrefixIterator(prefix)) {
        byte[] k = new byte[64], v = new byte[256];
        while (scan.next()) handle(k, scan.keyInto(k), v, scan.valueInto(v));
    }
}
```

Build the native library first, then the jar:

```sh
cmake --preset release && cmake --build --preset release --target elysiumkv_jni
mvn -f bindings/java verify
```

The jar carries `native/{os}-{arch}/libelysiumkv_jni.{so,dylib}` and extracts it at
first load, so there is nothing to install. Nothing is published to a package
repository yet.

Failures arrive as exceptions whose type says whether retrying makes sense:
`RetryableException` for an unreachable store or a write held back by the stall
valve, distinct from `CorruptException` and `UnusableException`.

Applications on **JDK 24 or newer** should pass `--enable-native-access=ALL-UNNAMED`
or the equivalent `module-info` entry; without it every JNI load prints a warning.
Older JVMs reject the flag outright, so it cannot simply be set unconditionally.

## Memory

Four caches, one budget. The budget is **per process, not per instance** — many embedders
run one instance per shard or tenant, so per-instance sizing multiplies by the instance
count and is the wrong unit:

```cpp
auto budget = std::make_shared<elysiumkv::MemoryBudget>(4ull << 30);   // 4 GiB, this process
for (auto& shard : shards) options.memory_budget = budget;           // every instance
```

| What | Holds | Bounded by |
| --- | --- | --- |
| Block cache | decompressed blocks, above the whole store chain | `block_cache_bytes` |
| SST readers | each open file's index block and bloom filter | `reader_cache_bytes` |
| `DiskCacheBlobStore` | raw byte ranges on local disk | its own size |
| `MemoryCacheBlobStore` | raw byte ranges in memory | its own size |

Memtable arenas charge the budget too, and are usually the largest consumer. When the
budget is exceeded the engine sheds in this order: **evict the block cache, flush
memtables, then slow writes down.** Each step costs the application more than the last,
and only the third is visible to it. **No write ever fails because of the budget** —
refusing a `put` because a different instance is using memory would be a surprising and
unhelpful failure mode — and writes are slowed only while shedding is making progress, so
a budget set too low costs throughput rather than wedging the process. `budget_sheds` in
the stats is what tells you it is set too low.

The bloom filter is the reason the reader cache needs a bound at all: at 10 bits per key it
is around 1.25 MB for a million-entry file, so a thousand open files is over a gigabyte.
Size it generously anyway — evicting a reader costs three reads to reopen it, which against
a remote tier is three round trips.

## Concurrency

**One writer per store.** ElysiumKV is single-writer by protocol, and the protocol is enforced by
the manifest: installing a new generation is a compare-and-swap, so a second writer that gets
there first leaves the other fenced — `FencedException` in Java, `Status::Fenced` in C++ —
meaning its view is stale and it must be reopened rather than retried.

What that does **not** protect is the window before the fence fires. `open` takes no lock and
performs no compare-and-swap; it reads the manifest and gets on with it. So two processes on one
store can both believe they own it until the loser's next manifest write, which may be many
operations later. So arrange for exactly one writer: a lease, a singleton, a lock around your
deploy, or distinct prefixes per instance.

**Readers are a supported configuration, not a tolerated one.** `DB::open_read_only` — or
`ElysiumKV.openReadOnly`, returning the `ReadOnlyStore` type, which has no write methods — opens
without writing the manifest at all, starts no background threads, and performs no compare-and-set.
Any number may run alongside the writer. That works because objects are write-once, so a block a
reader has cached can never become wrong, and because a reader is outside the ownership protocol
entirely: it can neither fence the writer nor be fenced.

One thing the writer must configure for it: **`obsolete_retention`**. The writer's collector deletes
an object once nothing in *its own process* references it, and a reader elsewhere is invisible to
that — so without a retention window, a compaction on the writer deletes files a reader is still
reading. Set it comfortably above how often your readers call `refresh()`. A reader that falls
behind is told `Status::Stale` and recovers by refreshing; it is never told the store is corrupt,
because it is not.

Freshness is explicit: a reader sees the version it opened until it calls `refresh()`. Two reads in
one logical operation therefore see one version, and a long scan is unaffected by a refresh under it.

**Reclamation of unreferenced objects happens on a sweep, not at open.** Deleting at open rested on
a single instantaneous observation, which cannot tell a dead writer's residue from a live writer's
just-committed file; the sweep requires an object to be *continuously* unreferenced for
`orphan_retention`, and re-reads the manifest each pass so a file whose edit has since landed drops
out on its own. It is off unless `orphan_sweep_interval` is set, and leaving it off costs storage and
nothing else — the engine steps the file-number counter over whatever the stores already hold, which
is what makes not deleting safe. On S3 a lifecycle expiry rule on the prefix is the other answer.

## Reading: which path to use

Two read paths exist because their costs differ. Measured on an Apple M-series
laptop — a 1000-key prefix scan and a hot point lookup — so read them as ratios
rather than absolutes:

| scan                                        | ns per entry | boundary crossings |
| ------------------------------------------- | ------------ | ------------------ |
| `prefixIterator` with `key()`/`value()`     | 419          | 3 per entry        |
| `prefixIterator` with `keyInto`/`valueInto` | 251          | 3 per entry        |
| **`batchedPrefixIterator`**                 | **58**       | 3 per batch        |
| the same scan in C++                        | 36           | —                  |

| point lookup         | ns/op |
| -------------------- | ----- |
| C++, block cache hit | 386   |
| Java `getCopy`       | 711   |
| Java `get` (pinned)  | 785   |

**For scans, use the batched iterator.** Only ~37 ns of the unbatched cost is the
advance itself; the rest is two accessor crossings per entry, and removing the
per-entry allocation recovers under half of it. Batching copies each entry instead
of borrowing it, which is the right trade for a scan and the wrong one for a
lookup.

**For point lookups, pin large values and copy small ones.** The pinned path avoids
copying the value but adds a crossing and a pin-table entry, so on an 8-byte value
it loses. Its advantage is that allocation does not scale with value size. The
crossover sits well above a kilobyte; measure against your own values.

**Close what you pin.** A leaked pin holds a block-cache entry that can never be
evicted. Both bindings track outstanding pins and report a non-zero count at close.

## Limits

| Constraint    | Value                                                  |
| ------------- | ------------------------------------------------------ |
| Maximum value | 1 MiB                                                  |
| Maximum key   | 64 KiB                                                 |
| Concurrency   | one writer, any number of readers; iterators and pins belong to one thread |

Oversized entries are refused at `put`, rather than accepted and found unreadable
later. Enabling `paranoid_checks` turns the threading rule from documentation into
an exception and switches on continuous internal verification.

## Building

Requires **CMake 3.25+**, **Ninja**, and a **C++23** compiler (`std::expected` is
in the public API). Dependencies — zstd, lz4, GoogleTest, Google Benchmark — come
from [vcpkg](https://github.com/microsoft/vcpkg) in manifest mode, pinned by
`builtin-baseline` in `vcpkg.json`.

```sh
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg   # full clone; versioning needs the history
~/vcpkg/bootstrap-vcpkg.sh -disableMetrics
export VCPKG_ROOT=~/vcpkg

cmake --preset debug && cmake --build --preset debug && ctest --preset debug
```

| Preset       | Purpose                                                |
| ------------ | ------------------------------------------------------ |
| `debug`      | Day-to-day, with internal invariant checks compiled in |
| `asan-ubsan` | Address and undefined-behaviour sanitizers             |
| `tsan`       | Thread sanitizer, for the flush and compaction paths   |
| `release`    | Optimised, plus the benchmarks                         |

Two library artifacts come out of the same objects: `libelysiumkv.a`, which exports
the C++ API and is safe only under the same toolchain, and
`libelysiumkv.{so,dylib}`, which exports the C ABI **and nothing else**. That export
restriction is a correctness requirement rather than hygiene — default visibility
re-exports statically linked zstd, which can then interpose on a different copy
already loaded in the host process, and the failure appears as corruption or a
crash depending on load order. The Java binding's glue is a third, separate shared
object.

Supported: **linux-x86_64**, **linux-arm64**, **macos-arm64**. Windows is not
supported today — nothing in the design prevents it, but nothing tests it either.

## Testing

Sanitizers are a build gate, not an option. CI runs every preset on all three
platforms, and the Java binding on JDK 11 and 25.

| Suite                    | Covers                                                                                                           |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| `tests/unit`             | Block format, checksums, bloom filter, SST reader and writer, memtable, block cache, versions, manifest encoding |
| `tests/contract`         | The object-store and catalog interfaces, run against every implementation                                        |
| `tests/diff`             | A `std::map` oracle compared against the engine over seeded random operation streams                             |
| `tests/fault`            | Injected failures: torn flushes, truncated manifest edits, corrupted blocks, unreachable stores, kill points     |
| `tests/invariant`        | Continuous internal consistency checks, and controls proving they fire                                           |
| `tests/perf`             | Allocations per operation — zero on a cache-hit lookup and per iterator step                                     |
| `tests/soak`             | Resident memory plateaus under a steady-state threaded workload                                                  |
| `tests/capi`             | The C ABI, including a smoke test compiled as C99                                                                |
| `bindings/java/src/test` | The binding, including a `TreeMap` oracle driven through the Java API                                            |

Two ideas run through the suite and are worth knowing before contributing.

**The differential oracle is the primary correctness argument.** A seeded stream of
operations is applied to both the engine and a `std::map`, and every read is
compared. It runs in a synchronous mode where the entire run is a function of the
seed — so a failure reproduces exactly and shrinks to a minimal case — and in a
threaded mode that samples real interleavings. Most of the serious bugs found in
this codebase came from it rather than from unit tests.

**Every automated gate has a negative control.** A gate that can silently become a
no-op reports green for two different states: nothing is wrong, and the check did
not run. Each one is therefore paired with a test that deliberately breaks what it
watches and asserts it fails *for that specific reason* — a stray exported symbol
must be named, each internal invariant has its own injection, and a synthetic
benchmark is slowed on purpose to prove the performance ratchet trips.

## Benchmarks

Benchmarks are gating rather than decorative: a correctness oracle cannot tell a
prefix scan served by file pruning from one that walks the whole keyspace. Both are
correct; one is ten times slower.

```sh
cmake --build --preset release --target elysiumkv_bench
./build/release/bench/elysiumkv_bench --benchmark_repetitions=7 \
    --benchmark_report_aggregates_only=true --benchmark_format=json > results.json
python3 bench/check_regression.py results.json           # fails on a >10% regression
python3 bench/check_regression.py results.json --update  # ratchet the baseline down
```

Baselines are **per machine and not committed**: 73 ns on one arm64 machine is not
a 73 ns threshold on another, and treating it as one reports a hardware difference
as a regression. Your first run records your own and passes with a notice; CI keeps
its own in a cache so runners compare against their own history.

A baseline also expires. Measured 20 hours apart on one laptop under sustained
build load, a memory-bound benchmark drifted 24% with no code change at all. Before
believing a regression, rebuild the old code and measure it *now*.

The load-bearing benchmark is the prefix scan: the same 100-key prefix in a 100k-
and a 1M-key store must cost about the same, or file pruning is not working.

## What is not implemented

- **Range deletes.** The entry encoding reserves room for them; the compaction
  interaction is not designed.
- **A write-ahead log.** Deliberate: the changelog you are already replaying is
  the log, so duplicating it would double every write. The watermark is what
  makes that trade workable — it tells you where to resume, and an unflushed
  memtable is still lost.
- **Windows.**

## Contributing

Issues and pull requests are welcome. Practical expectations:

- `ctest` passes on `debug`, `asan-ubsan` and `tsan` before review.
- Behaviour changes need a test that fails without them. For anything touching
  compaction, recovery or the read path, prefer extending the differential or fault
  suites over adding an isolated unit test.
- New automated gates need a negative control, for the reason given above.
- Comments should explain why, not what. Several in the source read as
  over-explained; they are usually recording a mistake that was expensive to find,
  and they earn their space.

## License

MIT — see [LICENSE](LICENSE).

The shared library and the Java jar statically link zstd and lz4, so their notices
travel with the binary. [NOTICE.md](NOTICE.md) records what is bundled and what it
requires of you if you redistribute a build.
