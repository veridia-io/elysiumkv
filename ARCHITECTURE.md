# ElysiumKV — Architecture

What this engine is, the decisions that define it, and what those decisions cost. Organised by
question rather than by component: if you want to know *why* something is the way it is, the answer
should be findable without reading the source first.

## What it is

An embedded LSM key–value store with a stable C ABI and a Java binding. A memtable absorbs writes,
flushes to level 0, and leveled compaction moves data downward. One writer per store.

The distinguishing feature is that **storage location is a separate axis from LSM structure**: the
same engine addresses a local disk and an object store, and a file's home is decided by its age and
size rather than by which level it happens to live in. That is what makes a local-disk deployment
and a "hot locally, cold in S3" deployment the same engine with different configuration.

## How it works

The life of data, end to end. Everything in the next section is a justification for something here.

```
   put/delete
       │
       ▼
   ┌─────────────┐  size OR  ┌──────────────┐
   │  memtable   │  age ────▶│  immutable   │
   │ (skip list) │           │   memtable   │
   └─────────────┘           └──────┬───────┘
       ▲                            │ flush
       │ read (first)               ▼
       │                     ┌──────────────┐
       │                     │  L0  SSTs    │  overlapping; newest = highest file number
       │                     └──────┬───────┘
       │                            │ compaction
       │                     ┌──────▼───────┐
       └──── read (then) ────│  L1 … Ln     │  non-overlapping within a level
                             └──────────────┘
                                    │
                     placement(age) decides which tier holds each file
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
        tier 0 (local)        tier 1 (local/…)      tier N (S3, durable)
                     ──── migration, cold-only ────▶
```

### A write

`put` and `delete` go into the memtable — a skip list over an arena — and a `WriteBatch` is applied
to it as one unit. A delete stores a tombstone; nothing is erased in place anywhere in the system.
The arena is what lets the memtable's footprint be charged to the memory budget as a single number.

**Two independent triggers rotate the memtable**, and whichever comes first wins: it has reached
`memtable_bytes`, or it has been open longer than `flush_interval`. When either fires, the memtable
becomes immutable and a fresh one takes over, so writes keep being accepted while the full one is
flushed.

Size alone is not enough, because a tier's age bound can only act on a *file*. Under a trickle of
writes that never fills a memtable, data would sit in memory for the life of the process and no
tier configuration could reach it — the durability story would have a hole at the front. The
interval bounds how long a write can sit somewhere a crash would lose it, independent of write
rate. It costs write amplification in exchange: a short interval on a quiet store produces small L0
files, and small files mean more compaction to merge them away, so it should be chosen from how
much recent data you are willing to lose rather than from a latency target.

The age trigger is evaluated by the maintenance coordinator, not by the write path — the situation it
exists for is precisely one where no write is arriving (*Maintenance asks; it is not told*). Both
triggers are one predicate in the coordinator's table rather than a timer of their own, because
`flush_interval` was the first thing in this engine to be driven by time and the last thing that
should be on separate machinery. In inline mode there is no coordinator, so age is evaluated on the
next write; with no writes at all, nothing flushes.

### Flush: memtable → L0 SST

The immutable memtable is iterated in key order and written as one SST: data blocks, a bloom filter,
an index block, and the footer. A file number is allocated, and the object is written **once** to
whichever tier `placement` selects — for a brand-new file that is age zero, so the hottest tier that
accepts its size.

Then a manifest edit adds the file at level 0 and a new version is installed. **The order matters:**
the object exists before anything references it, so a crash in between leaves an unreferenced object
— an orphan — and never a manifest pointing at bytes that do not exist. Recovery steps the file
number counter past it rather than deleting it.

### Inside an SST

A file is sorted key–value blocks, then a bloom filter, an index block, and the footer.

Blocks use restart points with prefix compression between them, and carry a checksum each. Restart
points bound how far a seek has to scan; the per-block checksum means corruption is detected at the
block that carries it, rather than surfacing as a nonsense key somewhere upstream.

**Compression is a level property, not a global one**, because the trade genuinely differs by level.
Level 0 is written constantly and read soon after; the deepest level is written rarely and holds most
of the bytes. Leaving L0 uncompressed and compressing deep levels spends CPU where the data actually
accumulates. It also means migration never recompresses — a file's codec follows its level, and
migration does not change a file's level.

The bloom filter is per file and sized in bits per key. It only helps point lookups — it does nothing
for iteration or compaction — so it earns its space on a read-heavy keyspace and very little on a
write-mostly one.

### The manifest is snapshots plus edits

The file list lives in numbered, fixed-width-named objects. A generation is one snapshot plus a
sequence of edits: the snapshot bounds how much must be replayed at open, and the edits keep the
common case — one flush, one compaction — a small append instead of a rewrite of the whole list.
Rolling to a new generation is where the next snapshot is written.

File numbers come from a single monotonic counter and are **never reused**, which is what lets object
identity be the number alone rather than a store-and-number pair.

### A read

`get` walks candidates newest-first and stops at the first hit, tombstone included:

1. the active memtable, then the immutable one if a flush is in flight;
2. L0 files in descending file-number order — they overlap, so all of them may need consulting,
   which is why L0's file count is a direct read-amplification term;
3. each deeper level in turn, one file at most per level, found by binary search because files in a
   level do not overlap.

Within a file: consult the bloom filter, then the index block, then the data block. Blocks come from
the block cache as pins — no copy on a hit. On a miss the cache chain is walked outward (memory,
then disk, then the authoritative store), and each layer fills on the way back.

A key's tier affects only *where the bytes come from*, never the order candidates are tried. That is
why a fresh local L0 entry correctly shadows a stale copy of the same key sitting in S3.

### Compaction

A background thread scores every level by how far it exceeds its configured budget and compacts the
worst offender. The chosen input files plus every overlapping file in the next level are merged by a
heap of iterators; where the same key appears more than once, position decides the winner — newest
wins — and superseded values and dead tombstones are dropped. That is the *only* mechanism that
reclaims space.

New files get fresh numbers, one manifest edit adds them and removes the inputs, and the old objects
are deleted once no live version references them. Compaction picks by structure alone: a file's age
and its tier play no part, so a compaction may read inputs from a cold tier and write outputs to a
hot one.

### Migration between tiers

Separately from compaction, a background pass recomputes `placement` for each file. If the answer is
colder than where the file sits, it is copied to the new store — read through the bulk path so the
caches are not polluted with bytes nobody will read again — written under a **fresh file number**,
swapped in by a manifest edit, and the old object is deleted.

The copy is byte-for-byte: nothing is decoded or recompressed, because compression is a property of
the *level* and migration does not change a file's level. So a migration costs exactly the bytes
moved — which is what makes it safe to treat as a background cost optimisation rather than as work
that competes with compaction.

Three priorities, in order: rescuing files off a *transient* tier that may lose them; evicting to
respect a tier's capacity; then age. The last is a cost optimisation, so starving it wastes money
rather than risking data. Level 0 never participates — a new file number would reorder recency — so
an L0 file whose age has outgrown its tier leaves by being compacted instead.

### Maintenance: who decides that any of this is due

One coordinator thread reconciles on a fixed interval. It evaluates every background policy —
flush, compaction, migration, capacity eviction, obsolete-object collection — against current state
and the clock, and hands what is due to one of two executors: flush on its own thread, and one
*deleting* task at a time on the other. It performs no long-running work itself, so its next
evaluation is never blocked by work it dispatched.

**Scheduling pulls; it does not wait to be pushed** — see *Maintenance asks; it is not told*. An idle
tick is two comparisons and no version scan, which is what makes a one-second default affordable
across many instances in one process.

### The watermark

The embedder may tell the store where it has reached in whatever log it is replaying —
`set_watermark(position)`, typically a changelog offset. The engine orders it, carries it with the
data, and hands it back at the next open; it never invents or interprets one. See *Absence is an
answer, not an error* for what it promises and what it does not.

### Open and recovery

Read the manifest pointer, load that generation's snapshot, replay its edits to rebuild the version,
then list the stores to confirm every referenced file exists and to raise the file-number counter
above everything present. Nothing is deleted unless reclamation is explicitly enabled.

## The decisions

### Positional recency

Most LSM engines put a monotonic sequence number on every entry and resolve conflicts by comparing
them. We don't. Newer means "in a file with a higher number, in level 0" or "further up the level
stack" — a lookup walks levels in order and stops at the first hit.

**Why:** a sequence number is a second source of truth about ordering, one that has to be kept
consistent with a structure that already encodes it. Removing it removes a class of bug where the
two disagree, and it removes a field from every entry.

**What it costs:** one rule has to hold absolutely everywhere — *a file number is never reused*,
including when a file moves between stores. Anything that renumbers files rewrites history. That
single constraint explains several things that otherwise look arbitrary: migration allocates a fresh
number instead of carrying one across stores (so identity stays the number alone, and no component
needs to key on a store/number pair), and level 0 files are never migrated at all, because a new
number would move them in the recency order. An L0 file that has outgrown its tier leaves by being
compacted instead.

### A tier is not a level

A *level* is structure: how much data, how much overlap, when to compact. A *tier* is storage: which
blob store holds the bytes. They are orthogonal, and placement is a pure function:

```
placement(age) -> tier
```

It consults no keys and does not know whether anything in the file is still current.

**Age is the only input, and narrowing it to that was a correction.** A per-file size bound used to
be a second predicate, and it broke the property the rest of this section rests on: a file's age only
ever increases, so placement only ever moves it colder, but a *size* bound could place a brand-new
file on a cold tier the day it was written. One axis that only moves one way is what makes the design
stable; two axes with different dynamics is what makes files thrash. Capping a tier's footprint is
`Tier::max_bytes`, evicted oldest-first — which is what anyone reaching for a size bound actually
wanted — and keeping large files off a fast tier is a matter of the level's `target_file_bytes`.

**Why:** the tempting design is "level 3 lives on S3". That makes every compaction decision a
storage decision and every storage change a rewrite of the LSM shape. Keeping the axes separate
means tuning read amplification and tuning storage cost are independent exercises.

**What it costs**, and these bite in practice:

- Placement only moves colder, because age only increases. Updating a key never pulls the file
  holding its old value back from a cold tier — the stale copy stays there until compaction
  reclaims it.
- A migrated file is still an ordinary file. Compaction picks by structure alone, so a file you
  moved to S3 can be **read back over the network** when compaction reaches its level. Migration
  buys storage cost, not immunity from compaction I/O. If a tier's age bound is short relative to
  how long compaction takes to reach that level, you pay for the round trip.

The last tier must be durable and must not bound age, so a file always has somewhere to
land. Open rejects a configuration that breaks this instead of documenting it as a precondition.

**Every tier knob is an expression of one underlying parameter: durability lag** — how long a write
may sit somewhere it could still be lost. Configuring that as an age bound per tier states it
directly, instead of leaving it to emerge from a combination of sizes and rates that nobody can
compute. A transient tier's stall valve exists for the same reason: when migration cannot keep up,
the lag would otherwise grow without bound, so writes are slowed rather than allowed to outrun
their own durability.

**`max_age` is not the exposure window, and it is not the largest term in it.** The window is four
terms:

```
max_age                                    the bound you configure
  + the maintenance interval               how late the crossing is noticed
  + queueing behind an in-flight compaction
  + the migration itself                   read, upload, commit the manifest edit
```

The third term is the one to size against, not the first: it is bounded by `max_compaction_bytes`
over throughput, which at the 400 MiB default and 50 MB/s is roughly sixteen seconds. Note what that
conversion assumes — **a byte bound turned into a time bound using an assumption about your
storage** — so it is a typical-case figure and not a guarantee. Against a remote tier throughput is
network-bound and variable, and a throttling episode with retries stretches it further.

**And no tier setting bounds the worst case.** During a storage outage, migration is what is failing,
so data already written stays exposed for the duration. The stall valve stops accepting *new* writes
past `stall_age`, which bounds the **volume** of exposed data and not its **duration**. The backstop
is that your log still holds the data — which is what the watermark is for. A tier configuration
cannot make an object store's outage shorter.

Read the stall flag accordingly: from the moment it engages, the log is expiring while the durable
position stops advancing, so **recovery capability is what degrades, on a deadline**, and the action
is to extend log retention.

### Maintenance asks; it is not told

Every background policy is a predicate over current state plus the clock, evaluated by one
coordinator on a fixed tick — not a message a caller remembered to send.

**Why:** it used to be push-based, and three age-bounded behaviours shipped with a write as their
only trigger. Each failure was identical: the only thing that could have noticed was the thing that
had stopped happening. A store that went quiet with a file on a transient tier left it there
indefinitely, whatever the tiers said. The remaining `schedule_compaction()` calls on the write path
are an optimisation now — they keep a stalled writer from waiting a tick — not the mechanism.

**A fixed interval rather than a computed deadline.** The interval is the *smallest* term in the
exposure window (*A tier is not a level*), so precision there buys nothing, and a deadline
computation's failure mode is a silent indefinite sleep — the exact class of bug this exists to
remove. The rejected alternative, recorded so it is not re-proposed: sleep until
`oldest min_write_time_ms + max_age`.

**But an idle tick must be nearly free**, or a one-second interval across dozens of instances is not
affordable — and it is not free naively, because picking a migration walks every file in the version.
So a gate: a *maintenance epoch* that changes on any predicate-relevant non-clock event, plus the
earliest future time at which a time-driven predicate could change. Unchanged epoch and no transition
passed means nothing can have become due, and the tick costs two comparisons.

**The gate is itself a push dependency**, and honesty about that matters more than the mechanism. It
is much narrower than what it replaced — every predicate must declare what invalidates it, checked in
one place instead of at scattered call sites — but a predicate whose invalidation is mis-wired can be
hidden by it. Two things keep that bounded: O(1) predicates such as memtable size are evaluated
*ahead* of the gate, and every sixtieth tick bypasses it entirely. So a mis-wired predicate runs
late rather than never. A task nobody wrote a predicate for is not covered by anything.

**One coordinator, two executors.** The coordinator dispatches and never performs long-running work,
so its next evaluation is not blocked by what it dispatched. Flush has its own thread because one
immutable memtable is allowed in flight, so sharing a worker with compaction would stall writes for
the length of a compaction. Everything that *deletes* — compaction, migration, capacity eviction —
shares the other, and that is load-bearing rather than incidental: `VersionSet::apply` does not
validate that the files an edit removes are still live, so two deleting tasks picking from the same
version snapshot could both commit. Flush is exempt because it only adds. A second deleting worker
needs file-level exclusion or optimistic validation *first*; an assertion under
`ELYSIUMKV_PARANOID` fails on the first test run if one is added without it.

**The stall predicate has exactly one evaluator.** The coordinator decides whether a transient tier is
past `stall_age` and publishes a flag; the write path reads it and does not compute it. The same
predicate in two places is how a valve ends up engaged by one and not the other. The cost is that
engaging lags by up to one tick — the same `+ interval` the exposure window already carries — and
open reconciles synchronously so the flag is never stale on the first write.

### Immutable named objects

The storage interface is deliberately tiny: put a named object, get a byte range, list a prefix,
remove. No append, no rename, no in-place update — the intersection of what a filesystem and an
object store both do well.

Two rules carry the weight:

**Objects are write-once.** Putting to an occupied name fails rather than overwriting. Immutability
is what makes a file's bytes cacheable forever, safely readable by concurrent readers, and safe to
copy between tiers.

**"Not found" is positive evidence of absence. An I/O error is evidence of nothing.** This
asymmetry is the most important rule in the system. Recovery may draw conclusions from *not found* —
that a file was never written, that an edit never landed. It may never draw a conclusion from *I/O
error*, which means only "could not determine". A storage implementation that reports absence for a
network failure will cause data loss, and nothing upstream can compensate. Every layer, up to and
including the C ABI, keeps those two answers distinct.

A corollary: a failed write is never retried under the same name, because the first attempt may have
partially landed. The engine allocates a new number and leaves the partial object as an orphan.

### Ownership is one compare-and-set

The manifest catalog stores the file list and provides a compare-and-set on the pointer to its
current generation. That CAS is the engine's entire ownership story, because it is the only atomic
test-and-set in the system. A writer that loses it has been overtaken; it reports *fenced* and must
be reopened. It never retries — retrying means writing over the winner's history.

**Why one place:** ownership arbitrated in several places is ownership arbitrated nowhere. This is
why a name collision on an object is explicitly *not* an ownership signal — it is a numbering
accident, resolved by renumbering. Only the manifest decides who owns the store.

**What it costs:** the fence protects the manifest, not the object store. Two processes writing to
one prefix will not corrupt each other's committed history, but they can still interfere in ways the
CAS cannot see — which is why open performs no destructive action (below), and why storage
reclamation is opt-in.

### Open never destroys anything

Recovery reads the pointer, replays edits, and reconciles against what the stores hold. Crash
residue — an object written by a flush that died before its manifest edit was durable — is handled
by **stepping the file-number counter over everything already present**, not by deleting it.

Deleting was the original design and it was wrong. Open takes no lock and performs no CAS, so an
unreferenced object is indistinguishable from a concurrent writer's in-flight file, or from one whose
edit became durable between the moment the manifest was read and the moment the store was listed. A
rolling deploy with two pods briefly overlapping on one bucket would silently destroy committed
data.

So reclamation is a flag, off by default, and turning it on asserts something the engine cannot
check: that nothing else has the store open. Leaving it off costs disk and nothing else; on S3 a
lifecycle rule on the prefix is the other answer.

### Caches chain

A cache is a blob store wrapping another blob store. Memory over disk over S3 is three decorators,
and the engine above cannot tell the difference. Three rules keep it honest:

- **Write-through, never write-back.** A cache that acknowledges a write it hasn't passed on has
  become the authority for data it is allowed to evict.
- **A cache is never innermost.** It holds copies; making one the only home for a file leaves
  eviction with nothing to fall back on. The configuration is rejected, not documented.
- **Bulk reads bypass the chain.** Compaction streams whole files it will never re-read; caching
  them would evict everything useful.

### A process-wide memory budget

The budget is an object you create once and hand to every database and cache — not a size on
options.

**Why:** embedders run one instance per shard, partition or tenant, so per-instance sizing
multiplies by a count the library cannot see. Measured here with six instances replaying 120k
records each: peak charged memory was **476 MiB** on per-instance defaults versus **72 MiB** tuned,
and only the first number grows with instance count.

When the ceiling is crossed the engine **sheds** rather than failing — evict the block cache, flush
memtables, then stall writes. A write is never refused because a *different* instance is using
memory; that would wire a failure to something the caller cannot see. The budget shapes behaviour
instead of rejecting work, and a shed counter is how you learn it is too small.

The stall valve cannot be disabled, but it can be made non-blocking, in which case a write that
would stall is refused with a retryable status rather than parking the caller's thread. That is the
right choice for a non-critical writer and the wrong one for a system of record — the data is simply
not stored.

### Absence is an answer, not an error

Fallible operations return a result type; a missing key is a *not found* answer rather than an error
handled alongside I/O trouble. The distinction is carried by the type system at every layer, and it
survives the C ABI, where absence is a distinct status rather than a null with a message.

**Why it is worth the API weight:** folding "the store is unreachable" into "there is no such key"
turns an outage into apparent data loss, and a caller that cannot tell them apart will happily write
a replacement for data that still exists. This is the same rule as *Immutable named objects*, seen
from the top of the stack instead of the bottom.

The same distinction governs the watermark. `recovered_watermark()` returns a position or *nothing*,
and nothing is not zero — zero is a valid position, a store at the start of its log. A single
integer could not express "nothing can be certified", which is the answer after a transient store
loses data written before the first watermark existed.

### The watermark is an interval, and only its lower bound is load-bearing

`set_watermark(M)` asserts that every write completed so far is at a position at or before M in the
embedder's log. What it buys: **if `recovered_watermark()` returns M after an open, replaying only
the positions after M yields the same logical state as replaying everything.** Exclusive — `80` means
resume at `81`. Stated as *state* equivalence rather than record retention, because compaction drops
superseded values and dead tombstones, so no physical-retention claim would be true, and state
equivalence is what a changelog consumer actually needs.

**This is a resume point, not a durability improvement.** There is no write-ahead log, so an
unflushed memtable is still lost; the watermark tells you where to resume, it does not reduce what
you lost. `flush_interval` is what bounds the lag on a quiet store, and `flush()` promotes it
immediately.

Each file carries **two** values, not one:

- `low` — the last watermark established when the source memtable was *created*. Because every write
  accepted after `set_watermark(M)` is at a position above M, this is a strict *lower* bound: the
  file contains no write at or below its own `low`.
- `high` — the last established before that memtable was sealed. An upper bound.

Compaction takes `min` of the lows and `max` of the highs; migration carries both. Recovery reports
`max(high)` when nothing was lost, and `min(low)` over the *discarded* files when a transient store
did lose data.

**Why a single scalar cannot do this**, and it is worth being concrete because two earlier designs
were wrong here. Reporting a lost file's upper bound over-reports: a memtable that begins at 80 and
takes writes up to 100 before `set_watermark(100)` has `low = 80, high = 100`, and losing it costs
the write at 81. And inferring a temporal fact from *placement* or from *file numbers* does not work
either — the position ordering and the file-number ordering are independent, so a durable survivor
can certify a high position while the only copy of an earlier write sits on the tier that was lost.

The lower-bound rule needs no such inference: a write at or below `min(low)` over the discarded set
cannot have lived only in a discarded file, so it survives. The argument mentions no tier, age,
placement or file number, and holds for an arbitrary discard set — which is exactly why it is the
one that is correct.

`Stats::durable_watermark` is a **different quantity** with a different name on purpose: the live
transient-loss-survivable frontier, which advances as data settles onto durable storage, while
`recovered_watermark()` describes the state recovered at open and never moves. Sharing one name would
silently change the getter's meaning after the first write.

### Statistics are a buffer, not a struct

Statistics cross the ABI as an encoded buffer with self-describing record sizes, not as a struct. A
binding that does not know about a newer field skips it by its declared length instead of misreading
everything after it, so adding a statistic is not an ABI break.

They are also reported per level and per tier rather than as derived globals. Those are the two
independent axes (*A tier is not a level*), and a single global number would silently average over a
distinction the engine spends real effort maintaining. One call returns the whole snapshot, because a
snapshot assembled from per-field accessors is torn.

Two of them are worth calling out as the ones an operator acts on. `flushes` is the first place a
`flush_interval` set too short shows up, and the only way to see the interval fire at all on a quiet
partition — `memtable_age` is a gauge read at scrape time, and a counter cannot be derived from a
gauge. `durable_watermark` is the numerator of the only margin there is: the distance between your
log's earliest retained offset and that position is how much recovery capability is left, and when
migration is failing it stops advancing while the log keeps expiring.

Every counter here is **per instance and in memory**, so they return to zero on reopen — which for a
partition store means on every rebalance. Either accumulate across instances before exporting, or
scope each series per instance and alert on rates only; an absolute threshold silently re-arms
otherwise.

### Versions are immutable snapshots

A version is an immutable view of the file list; applying an edit produces a new version rather than
mutating the old one. A reader holds a reference and is unaffected by concurrent compaction, which is
what makes a consistent iterator possible without locking the engine — and what makes it safe to
delete a compacted-away object only once no live version still names it.

The version set also owns the file-number counter and the fenced flag, because both are facts about
*which history is being written* rather than about any one file.

### Reads don't copy

Block cache values are handed out as pins into cached, decompressed, immutable blocks. A cache-hit
lookup allocates nothing. A pin holds its entry alive, so a leaked pin leaks a cache entry — pin
accounting is a checked invariant, with a leak check at close rather than a comment asking callers
to be careful.

### The ABI boundary

Bindings target the C ABI, and every rule there exists because the failure modes on that boundary
are silent ones.

- Opaque handles, so layout is never part of the contract.
- Every entry point is wrapped, because an exception crossing the boundary is undefined behaviour.
  What escapes becomes a status and a message — reported as *unusable*, not *I/O*, since an escaped
  exception is not "try again later" but a state the engine does not model.
- The error slot is thread-local, so one thread's failure is never read as another's. That is also
  why the whole ABI is a single translation unit: a second one would get a second thread-local slot,
  and a failure reported through one would be invisible through the other.
- **The set of exported symbols does not change with build configuration.** Optional features
  (S3, DynamoDB) are absent as *behaviour*, reporting a configuration error, never as absent
  symbols — so a binding can verify the ABI with a set comparison.
- **Symbol visibility is correctness, not hygiene.** The shared library exports the C ABI and
  nothing else. Default visibility re-exports statically linked dependencies, which can interpose
  against a different copy already loaded in the host process — likely inside a JVM, where other
  native libraries bundle their own compression libraries. The failure presents as corruption or a
  crash depending on load order.
- The version string is generated from the build, so it cannot disagree with the artifact carrying
  it.

The Java binding is JNI rather than Panama FFM, for reach: FFM requires Java 22, which excludes most
production deployments. The floor is Java 11. Methods bind via explicit registration rather than name
mangling, so a package rename fails loudly instead of silently failing to bind. The native library
is extracted from the jar per platform, which means a released jar must carry every platform —
otherwise resolution succeeds and the failure appears at first use.

### The invariant trailer

An SST's last bytes are an invariant trailer: magic, then format version. A reader seeks to the end,
validates the magic, and only then dispatches on the version to learn the rest of the footer's
width.

The ordering is the design. If footer width depended on a version stored inside the footer, a reader
could not know how much to read before knowing the version, and no future format change would be
parseable. Because the magic is checked before anything else is trusted, it is frozen — changing it
makes every existing file report as corrupt.

## How we know it works

The testing strategy assumes the interesting bugs are not the ones a unit test finds.

### The differential oracle

Replays random operation streams against both the engine and a
`std::map`, comparing every result. The gating profile is many seeds of moderate length rather than
a few long streams — replay cost is superlinear in stream length (scans compare against an oracle
that grows with the stream), while distinct seeds explore distinct starting states. Both engine bugs
this suite has found surfaced within the first few thousand operations. A threaded variant runs
nightly, where failures are real but no longer shrinkable.

### Fault injection

Wraps storage to fail, corrupt or stall chosen operations, and kill points abort
between a write and its manifest edit. Without this the recovery rules above could not be tested at
all — crash residue is hard to produce on purpose.

### Negative controls

**Every gate has one.** For each invariant enforced, a paired test asserts it *fails
for the intended reason* when the mechanism is removed. A test that passes for the wrong reason is
worse than no test, because it reports safety it never checked — several tests here were found to be
vacuous exactly that way, and the controls are what caught them.

### Contract suites

One suite per seam, run against **every implementation** of it, including the remote ones. "A write
to an existing name never overwrites" is asserted against the local filesystem and against S3 by the
same code, so a divergence is a test failure rather than a production surprise.

### Invariants and sanitizers

Continuous internal consistency checks are compiled in under a build flag and
gated at runtime, and **sanitizers are part of the gate rather than an occasional exercise**. The whole suite runs under
ASan/UBSan and TSan, because the bugs this design can have — a pin outliving its entry, a reader
racing a compaction — are the ones only a sanitizer sees.

### Benchmarks

**They gate structure, not speed.** Allocation counts are asserted (zero on a cache-hit
lookup, zero per iterator step). Throughput is recorded but never gated: a wall-clock threshold on a
shared runner fails for reasons unrelated to the change under test.

## Dependencies and artifacts

Dependencies are pinned by a vcpkg baseline, and CI's checkout of vcpkg is pinned to the same commit.
The two are different things: the baseline selects port *versions*, the checkout supplies the port
*scripts* that do the building. Letting the second float means a build that worked yesterday breaks
today for reasons that live outside this repository — which has happened here.

Two library artifacts come out of the same position-independent objects. A static archive exports the
C++ API, safe only under one toolchain, which static linking already implies. A shared library exports
the C ABI and nothing else (*The ABI boundary*).

Build order is engine, then bindings, and the C ABI's smoke test is compiled **as C** — the only way
the C99 claim about the public header is tested rather than asserted.

## What we deliberately did not build

Listed so their absence isn't mistaken for an oversight. Each would change the contract rather than
extend it, and the reasoning above depends on their absence.

- **Multiple writers.** Ownership is one compare-and-set; concurrent writers would need a different
  ownership model entirely, not a bigger lock.
- **Snapshots as of a point in time.** There are no sequence numbers to name a point with.
- **Transactions** beyond an atomic write batch.
- **Secondary indexes.** A key–value store that also maintains derived state is two systems.
- **A write-ahead log.** The changelog the embedder is already replaying is the log; duplicating it
  would double every write for a durability guarantee the embedder can make more cheaply itself. The
  watermark exists because of this decision, not despite it: it says where to resume, and it does not
  reduce what an unflushed memtable loses.
- **Any interpretation of the watermark.** It is a position in someone else's log. The engine orders
  it and hands it back; it does not derive a time from it, relate it to `min_write_time_ms`, or claim
  anything across two stores. Two partitions' watermarks are unrelated and there is no global
  consistency point.

## Consequences worth planning for

The short list of things that surprise people, all consequences of decisions above rather than
defects:

| You do this | This happens | Because |
| --- | --- | --- |
| Set a short age bound on a hot tier | Files migrate to S3 and are read back to be compacted | Migration and compaction are independent axes |
| Leave `flush_interval` unset on a low-traffic store | Writes stay in memory indefinitely; no tier bound applies | Tier age acts on files, and a memtable is not one |
| Set a short `flush_interval` | Many small L0 files, so more compaction | Every flush produces a file regardless of how full it was |
| Run one instance per partition on defaults | Memory scales with partition count | Sizing is per instance unless a shared budget is given |
| Make stalls non-blocking | Writes are silently refused under pressure | The valve cannot be disabled, only redirected |
| Enable orphan reclamation | Another process's in-flight files may be deleted | Open cannot detect a concurrent writer |
| Run two writers on one prefix | The loser is fenced at its next manifest write, not immediately | Fencing happens at the CAS, which is not on every write |
| Set a watermark and crash before a flush | The previous watermark is what comes back | A watermark becomes durable with the memtable holding it |
| Lose a transient store | The recovered position rolls back to before anything that store held | Only the discarded files' lower bounds can be trusted |
| Upgrade across a manifest format change | The store will not open, and says `unsupported` rather than `corrupt` | A `0.x` format change is a clean break; rebuild from the log |
| Shorten `maintenance_interval` to tighten exposure | Almost nothing changes | The interval is the smallest of four terms in the window |
| Alert on a counter with an absolute threshold | The alarm silently re-arms on every rebalance | Counters are per instance and in memory |
