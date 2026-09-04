# ElysiumKV

An embedded LSM key-value store in C++, with a C ABI and a Java binding.

ElysiumKV is built around one idea that most embedded stores leave to the operator:
**where a file lives is decided per file, by its age, and it is decided
continuously.** Hot data sits on fast storage, cold data migrates to cheap
storage, and the store keeps working while it happens. Everything else — leveled
compaction, bloom filters, a block cache, prefix scans — is there to make that
useful rather than to be novel.

> Used in production, and under active development. Releases are versioned; a break in the C
> ABI or an on-disk format is named in the release notes.

---

## Contents

- [What it does](#what-it-does)
- [Storage tiers](#storage-tiers)
- [Remote storage](#remote-storage)
- [Encryption at rest](#encryption-at-rest)
- [Quick start (C++)](#quick-start-c)
- [Quick start (Java)](#quick-start-java)
- [Partitioned stores](#partitioned-stores)
- [Memory](#memory)
- [Concurrency](#concurrency)
- [Reading: which path to use](#reading-which-path-to-use)
- [Limits](#limits)
- [Building](#building)
- [Operator CLI](#operator-cli)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [What is not implemented](#what-is-not-implemented)
- [Contributing](#contributing)
- [License](#license)

## What it does

- **Ordered key-value storage** — put, delete, point lookup, range and prefix
  scans in either direction, atomic batches.
- **Leveled compaction** with a background thread, write stalls and tombstone
  reclamation. A level compacts when it passes its file or byte budget — and,
  optionally, when a single file passes `tombstone_density_trigger`, which is the
  case those budgets cannot express: a delete-heavy store that stays inside them
  never compacts, so its tombstones accumulate and every scan across the deleted
  region pays to skip them. Off by default; a store deleting in bulk usually wants
  `truncate_below` instead, which reclaims without rewriting anything.
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
- **Range deletes**: `delete_range(lower, upper)` removes a band from anywhere in
  the keyspace with one record rather than one per key — a tenant in the middle
  of a keyspace is the case that needs it. Unlike a truncation the range stays
  writable afterwards, and a later write wins. A file the range covers entirely
  is unlinked whole; the rest comes back as the covered files are rewritten.
- **Time-based expiry**: set `ttl` and a file whose newest write has outlived it is
  dropped whole on a maintenance pass, unread — but only when no older file still
  holds keys in its range, since recency here is positional and dropping out of order
  would resurrect a superseded value.
- **Prefix truncation**: `truncate_below(key)` drops everything under a key by
  moving one value in the manifest, rather than writing a tombstone per key. A
  file entirely below the point is unlinked whole — nothing read, nothing
  rewritten — which is what makes it cheap enough to run continuously against an
  ageing keyspace. **The floor is permanent**: a later write below it is refused
  rather than accepted and hidden, because recency here is positional and the
  engine cannot tell a key written before the truncation from one written after.
- **Zero-copy reads**: a lookup can hand back a pointer into the block cache,
  pinned until you release it, with no copy at any layer — including through the
  Java binding.
- **Pluggable storage**: the object store and the manifest catalog are interfaces.
  A local-directory implementation of each ships, and the C ABI exposes them as
  function-pointer vtables so a binding can supply its own.
- **Encryption at rest**, covering SST contents and manifest payloads alike:
  AES-256-GCM under envelope encryption, with a fresh data key per object. Key custody
  is yours — a static master key and AWS KMS both ship, and the key manager is an
  interface if neither fits (see [below](#encryption-at-rest)).
- **Bindings**: a C ABI (76 functions, C99) and a Java binding over JNI needing only
  Java 11, plus Kafka Streams state stores in `bindings/kafka-streams-v3` — key-value,
  window and session stores in both plain and timestamped form, and a versioned store
  (KIP-889). The windowed kinds keep every window in one store rather than one store per
  segment, so retention is a single `truncateBelow` rather than a segment directory to
  open and close.
- **Partitioned stores**: one engine instance per partition behind a single handle, with
  staging that makes a local write and a log commit agree on their outcome — see
  [below](#partitioned-stores).

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

**Give it a fetch granularity if the delegate is remote.** A cache stores the ranges
it was asked for, so a cold scan costs one request per block however large the cache
is. Passing a granularity rounds each miss out to a chunk and caches the whole chunk,
which turned a 256 KiB sequential read from 64 requests into 4 in the test that pins
it. Amplification is bounded by the chunk rather than the object — a small read
against a large file pulls one chunk, never the file — and it needs no notion of a
scan, so a point lookup whose neighbour is read later is served from what the first
one pulled. It does nothing for data already cached by `cache_on_write`; the case it
answers is a file this process did not write, which is a read-only replica or a cold
start.

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

## Encryption at rest

SST contents are encrypted with AES-256-GCM under envelope encryption: a **fresh data
key per object**, wrapped by a key manager you choose and recorded beside the file in
the manifest.

```java
options.encryptWith("v1", StaticEncryptionKeyManager.fromHex(masterKeyHex), 0);

// or, with the wrapping key held outside the process
options.encryptWith("v1", AwsKmsEncryptionKeyManager.builder(keyArn).build(), 0);
```

```cpp
auto keys = elysiumkv::StaticEncryptionKeyManager::from_hex(master_key_hex);
auto cipher = elysiumkv::Aes256GcmEncryptionProvider::open(*keys);
options.encryption.providers["v1"] = *cipher;
options.encryption.primary_provider = "v1";
```

**The per-object key is what makes the nonce safe**, not a detail of the
implementation. Nonce reuse under one key breaks GCM completely, and the nonce here is
a function of the chunk index with nothing persisted and nothing coordinated. A fresh
key per object confines the nonce space to that object — and since objects are
write-once and file numbers are never reused, an object is encrypted exactly once and
no path could reuse one. That is why "one key for the whole store" is not offered.

### What is and is not covered

**SST contents and manifest payloads are both encrypted**, so no user byte — value, key,
or the key bounds each file record carries — is stored in the clear. That last one is
the easiest to overlook and the reason the manifest needed its own seam: an engine that
sealed every SST and left `FileMetadata` readable would still leak the shape of the
keyspace to anyone holding the bucket.

What stays plaintext is what carries no user data and could not work encrypted: object
**names**, which are file numbers, and the **manifest pointer**, whose generation and
token are what `compare_and_swap` arbitrates ownership on.

The rest follows from where the two boundaries sit. For SSTs that is directly above the
object store:

- **Caches hold ciphertext.** A `DiskCacheBlobStore` in front of S3 stores encrypted
  bytes, because it is below the boundary. The block cache is above it and holds
  plaintext, as any in-memory read path must.
- **Compression is unaffected**, and still worth having: blocks are compressed before
  the object is sealed, so ZSTD sees the real data.
- **Migration between tiers is a byte-for-byte copy**, which stays true for an
  encrypted file. A migrated copy is renumbered, so the identity a chunk is
  authenticated against is recorded at creation rather than read from the file's
  current number.

A manifest payload has no ranged read to preserve, so it is sealed whole — compressed
first, because ciphertext does not compress and encrypting first would inflate every
manifest write. Each payload is bound to its own address, so an edit cannot be replayed
at another sequence number, and **a provider the manifest names but you have not
registered fails at `open`** rather than at whichever read reached an encrypted file
first.

### The id is persisted, and that is what makes rotation work

Every object records **which provider wrote it**, so reads route on the file rather
than on the current configuration. Rotating to a new construction or a new key is
therefore: register the new one as primary, keep the old one registered for reading,
and let compaction rewrite files under the new one over time.

```java
options.encryptWith("v2", AwsKmsEncryptionKeyManager.builder(newKeyArn).build(), 0);
options.alsoDecryptWith("v1", AwsKmsEncryptionKeyManager.builder(oldKeyArn).build(), 0);
```

Then turn on the pass that finishes it:

```java
options.rewriteToPrimaryEncryptionProvider(true);
```

**Changing the primary is not the rotation.** It governs what is written next; every file already
on disk keeps the provider it was written under, and a cold file may never be compacted — which is
exactly the file a rotation was performed to stop depending on. The flag turns on a background pass
that re-seals them one at a time, behind everything else the engine has to do.

`filesPendingReencryption()` reaches zero when it has converged, **and that is the moment `v1` may
be dropped** — the manifest is re-sealed as part of it, so the store then opens with `v1` not
registered at all. Non-zero with the flag off means a rotation was started and abandoned, which is
a store still depending on a key someone believes they retired.

**Renaming an id orphans every file written under the old name** — the id is data, not
configuration.

Rotating the KMS *key* underneath one id needs none of this: a wrapped data key names
the key that produced it, so `Decrypt` resolves it and files written under the
previous key keep opening.

### Key custody

Three options, in the order most embedders want them:

- **`StaticEncryptionKeyManager`** — one 32-byte master key held in this process,
  wrapping each object's data key with AES-256-GCM. For a key that arrives from a
  secrets manager at startup.
- **`AwsKmsEncryptionKeyManager`** — `GenerateDataKey` and `Decrypt`, so the wrapping
  key never enters the process. Needs `-DELYSIUMKV_BUILD_AWS=ON`; without it,
  registering one fails with a configuration error naming the missing option, as the
  remote seams do.
- **Your own** — implement `EncryptionKeyManager` (a Java interface, a C ABI vtable, a
  C++ virtual) and the engine keeps the cryptography. Supplying a whole
  `EncryptionProvider` is also possible and is for an organisation that must use a
  specific construction; almost nobody needs it.

**A KMS call is a network round trip**, so it is worth knowing when they happen: once
per object written, and once per object whose reader is not resident. A reader holds
its unwrapped key for as long as the cache keeps it, which is what stops this from
being per-block — but a reader cache well below the working set turns evictions into
KMS traffic.

### Cost, and the unencrypted case

Chunks default to 4096 bytes with a 16-byte tag each, so an encrypted object is about
0.4% larger. Reads stay ranged: a lookup fetches only the chunks its range covers.

**An unencrypted store is not a special case.** A passthrough provider is registered
under the reserved empty id and is primary unless you name another, so the code path
is the same one and a file written before encryption existed records the same thing as
one written with it off. There is no branch to get wrong.

## Quick start (C++)

```cpp
#include "elysiumkv/db.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/disk_blob_store.hpp"

elysiumkv::Options options;
options.manifest_catalog = std::make_shared<elysiumkv::DiskManifestCatalog>("/data");
options.tiers = {{.store = std::make_shared<elysiumkv::DiskBlobStore>("/data/store", "hot"),
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

db->truncate_below(cutoff);           // everything under `cutoff` is gone
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
try (DiskBlobStore store = new DiskBlobStore("/data/store", "hot");
     DiskManifestCatalog catalog = new DiskManifestCatalog("/data");
     ElysiumKVOptions options = new ElysiumKVOptions()
             .manifestCatalog(catalog)
             .addTier(store, Durability.DURABLE, 0, 0, 0)
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

The jar carries `native/{os}-{arch}/libelysiumkv_jni.{so,dylib}` and extracts it at first
load, so there is no separate native install. Releases are published to Maven Central by the
release workflow, as `io.veridia:elysiumkv` and `io.veridia:elysiumkv-kafka-streams-v3` — see
the release notes for the current version. Building locally is only needed to work on the
binding itself.

Failures arrive as exceptions whose type says whether retrying makes sense:
`RetryableException` for an unreachable store or a write held back by the stall
valve, distinct from `CorruptException` and `UnusableException`.

Applications on **JDK 24 or newer** should pass `--enable-native-access=ALL-UNNAMED`
or the equivalent `module-info` entry; without it every JNI load prints a warning.
Older JVMs reject the flag outright, so it cannot simply be set unconditionally.

## Partitioned stores

`io.veridia.elysiumkv.partitioned.PartitionedStore` runs one engine instance per partition
behind one handle: `assign`, `revoke` and `lost` follow a consumer group's rebalances, and
each partition keeps its own tiers, its own watermark and its own directory.

What it exists for is the ordering problem underneath a transactional consumer. A local write
and a log commit cannot be made atomic, so writes are **staged** — `stage` holds them,
`applyCommitted` lands them once the transaction commits, and `discard` drops them when it
did not. The watermark rides in the same memtable as the writes it covers, so a crash cannot
leave a watermark ahead of the state it claims.

The part worth reading before wiring it up is the failure taxonomy. Each outcome licenses a
different action, and they are separate types because a correct belief that permits an illegal
call is worse than no classification at all:

| Outcome                 | What the caller may do                              |
| ----------------------- | --------------------------------------------------- |
| `AbortableNotCommitted` | discard, then abort the transaction                  |
| `OutcomeUnknown`        | discard, then close the producer; aborting is forbidden |
| `ProducerDead`          | discard, then close; aborting would throw            |
| `ApplyFailed`           | it committed — repair the partitions it names        |

A partition whose outcome could not be established is marked **behind**, and both
`getCommittedBatch` and `stage` then throw `PartitionBehindException` until `repair` replays
it — which is what stops a fold from deriving a value from state the log has already moved
past. `begin()` closes the one hazard a two-hook transaction manager
leaves open: anything still staged at a transaction boundary belonged to a commit nobody
resolved, and is dropped rather than trusted.

`bindings/java/src/test/.../partitioned/kafka` carries a worked Kafka integration —
`KafkaTransaction` maps Kafka's exceptions onto the outcomes above, and
`ExampleStateRestore` replays a state topic. Both are test sources on purpose: the core takes
no Kafka dependency. Copy them.

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

### When something is refused

`Status` is small and closed, so one value covers many causes — `Config` most of all. **Every way
`open` can refuse leaves a sentence behind** naming the option that was wrong, which is the call
where guessing costs the most:

```cpp
auto db = elysiumkv::DB::open(options);
if (!db) {
    log("open failed: {} — {}", elysiumkv::status_name(db.error()), elysiumkv::last_error());
}
```

Java puts it in the exception message and C callers read `elysiumkv_last_error()`. It is advisory
and thread-local: read it immediately after the call that failed, and treat empty as "no more than
the status says" rather than as "nothing failed". Other calls set it where they have something to
add; the status is what you branch on.

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
in the public API). The library itself needs only zstd, lz4 and OpenSSL; GoogleTest,
Google Benchmark, the AWS SDK and the CLI's CLI11 / nlohmann-json / tabulate arrive
through vcpkg features, so a build with the tests and the CLI off pulls none of them.
Everything comes from [vcpkg](https://github.com/microsoft/vcpkg) in manifest mode,
pinned by `builtin-baseline` in `vcpkg.json`. OpenSSL is pinned to the 3.5 LTS
line and is what the built-in AES-256-GCM provider uses.

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

The jar carries one library per platform under `native/{os}-{arch}/`, and those directory names are
what the loader computes from `os.name` and `os.arch` — so they are the spellings that matter when a
load fails:

| Key | Built | Notes |
| --- | --- | --- |
| `linux-x86_64` | yes | glibc |
| `linux-aarch64` | yes | glibc |
| `darwin-aarch64` | yes | Apple Silicon |
| `darwin-x86_64` | **no** | Intel Macs — see below |
| `linux-x86_64-musl` | yes | Alpine and other musl distributions |
| `linux-aarch64-musl` | yes | as above |
| `windows-x86_64` | **no** | see below |

**musl.** A glibc build does not run on musl, and `{os}-{arch}` cannot say which libc it was built
against — so the loader appends `-musl` when `/lib/ld-musl-{arch}.so.1` is present. Without that the
key matches, the wrong library is extracted, and the failure is a relocation error inside `dlopen`
naming the library rather than the distribution.

The musl artifacts are built on Alpine, and not the way the others are. Alpine packages the AWS SDK
but not usably: 1.11.205 has no `PutObjectRequest::SetIfNoneMatch`, which is what the S3 manifest's
compare-and-set *is*, and it is compiled with AWS memory management on, so `Aws::String` is not
`std::string` and none of `aws/` builds against it. vcpkg is not the alternative — its lz4 port fails
under musl. So that job takes zstd, lz4 and OpenSSL from Alpine and builds the SDK from source with
`USE_AWS_MEMORY_MANAGEMENT=OFF`. The engine needed no changes at all; it compiles clean against musl.

Building on Alpine yourself: Maven cannot detect a libc, so pass
`-Delysiumkv.platform=linux-x86_64-musl` to package it under the key the loader will ask for, or
point `-Delysiumkv.library.path` straight at the build and skip extraction.

**Intel Macs.** Not built. The catch is that this is not only about Intel hardware: an x86_64 JDK
reports `os.arch=x86_64` even on Apple Silicon under Rosetta, so the loader asks for
`darwin-x86_64` on a machine whose `darwin-aarch64` library is sitting unused in the same jar. If a
load fails on a Mac, check `java -XshowSettings:properties -version 2>&1 | grep os.arch` before
anything else; an arm64 JDK is the fix.

**Windows.** Not supported, and further off than "untested": `disk_blob_store.cpp`,
`disk_manifest_catalog.cpp`, `disk_cache_blob_store.cpp` and `open_file_cache.hpp` use `unistd.h`,
`sys/mman.h`, `dirent.h` and `pthread.h` with no alternative path. The C ABI already has its
`_WIN32` export macro and the Java loader already names `elysiumkv_jni.dll`, so the seams are in
place; the storage layer is the work.

## Operator CLI

`ELYSIUMKV_BUILD_CLI` is on by default and produces `elysiumkv`, which reads a store without
opening it for writing — so it is safe to point at one a live process owns.

```sh
elysiumkv manifest --catalog disk --dir /data       # files, levels, tiers, what a reopen loads
elysiumkv stats --catalog disk --dir /data --json   # per level and per tier, machine-readable
```

`--catalog` selects the backend — `disk`, `dynamo` or `s3` — and the rest of the flags belong
to whichever one was named. `--generation` reads a generation other than the one the pointer
names. `--json` may be written on either side of the subcommand. Both commands are read-only;
there is no repair verb yet.

An encrypted store needs the provider its manifest payloads were sealed under. Routing is by the id
recorded in each payload, so the id given here has to be the one the writer registered:

```sh
elysiumkv stats --catalog disk --dir /data --encryption-provider kms-gcm kms:alias/elysiumkv
elysiumkv manifest --catalog disk --dir /data --encryption-provider aes-gcm env:ELYSIUMKV_KEY
```

The key is `hex:<hex>`, `file:<path>`, `env:<VAR>` or `kms:<key-id>`, all of them 32-byte master
keys wrapping a data key per object. The flag repeats, because a rotation changes the primary
provider without starting a new generation and one generation can hold payloads under two ids. Run
without it first if you do not know which: a payload this process cannot route reports the id it
needs.

## Testing

Sanitizers are a build gate, not an option. CI runs every preset on all three
glibc platforms, plus the release preset on both Alpine architectures — which
answers whether the engine works when libc is not glibc, without repeating the
sanitiser and thread runs. The Java binding runs on JDK 11 and 25.

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
| `fuzz`                   | libFuzzer over the four decoders, seeded from the encoders so the corpus cannot drift out of format             |

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

- **A write-ahead log.** Deliberate: the changelog you are already replaying is
  the log, so duplicating it would double every write. The watermark is what
  makes that trade workable — it tells you where to resume, and an unflushed
  memtable is still lost.
- **Column families, snapshots and sequence numbers** — recency is positional, and
  much of the engine's simplicity follows from that reduction.
- **More than one writer per store.** Ownership is arbitrated at the manifest and a
  loser is fenced, but see [Concurrency](#concurrency) for the window before that
  fires.
- **A partitioned index.** One flat index block per file, which is what bounds how
  large a single file is worth making.
- **Windows.**

## Contributing

Issues and pull requests are welcome. Practical expectations:

- `ctest` passes on `debug`, `asan-ubsan` and `tsan` before review.
- Behaviour changes need a test that fails without them. For anything touching
  compaction, recovery or the read path, prefer extending the differential or fault
  suites over adding an isolated unit test.
- New automated gates need a negative control, for the reason given above.
- Comments record what a reader cannot recover from the code: invariants, ownership,
  concurrency assumptions, wire-format constraints, and the reason behind a surprising
  choice. Not what the code does, and not how it came to look this way — a comment that
  narrates a change is stale the moment the next one lands. Detailed rationale belongs in
  [ARCHITECTURE.md](ARCHITECTURE.md).

## License

MIT — see [LICENSE](LICENSE).

The shared library and the Java jar statically link zstd, lz4 and OpenSSL, so their notices
travel with the binary. [NOTICE.md](NOTICE.md) records what is bundled and what it
requires of you if you redistribute a build.
