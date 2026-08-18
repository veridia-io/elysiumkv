/* elysiumkv.h — the C ABI (ARCHITECTURE.md "The ABI boundary").
 *
 * The contract with every binding, not just Java: designed as if four languages
 * will bind it, because the point of a native core is that they can.
 *
 * C99-compatible. Opaque handles only — no struct layout crosses this boundary,
 * so adding a field to any engine type cannot break a compiled binding.
 *
 * Rules that hold at every entry point:
 *
 *   - Status codes, never exceptions. Every function is wrapped in a catch-all;
 *     a C++ exception escaping this boundary would be undefined behaviour.
 *   - ELYSIUMKV_IO is the retryable class, and nothing else is. The ABI must never
 *     invite a binding to reinterpret an I/O failure as absence — the
 *     positive-evidence rule lives on the C++ side and cannot be delegated.
 *   - Borrowed pointers with explicit pins. elysiumkv_get hands back a pointer
 *     into a cached block plus a pin the caller releases. A leaked pin holds a
 *     block-cache entry forever, so pin accounting is a first-class invariant
 *     with a debug-build leak check at close.
 *   - Iterators are single-threaded and non-copyable.
 */

#ifndef ELYSIUMKV_H
#define ELYSIUMKV_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* The library is built with hidden visibility so that statically linked
 * dependencies are not re-exported — a copy of zstd interposing against another
 * one already loaded in the host process is a corruption bug, not a hygiene
 * issue. That makes marking the ABI explicit, not optional: without this the
 * shared library would export nothing at all. */
#if defined(_WIN32)
#  if defined(ELYSIUMKV_BUILDING_SHARED)
#    define ELYSIUMKV_API __declspec(dllexport)
#  else
#    define ELYSIUMKV_API __declspec(dllimport)
#  endif
#else
#  define ELYSIUMKV_API __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct elysiumkv_db elysiumkv_db;
typedef struct elysiumkv_iter elysiumkv_iter;
typedef struct elysiumkv_options elysiumkv_options;
typedef struct elysiumkv_batch elysiumkv_batch;

typedef enum {
    ELYSIUMKV_OK = 0,
    ELYSIUMKV_NOT_FOUND,
    ELYSIUMKV_CORRUPT,
    ELYSIUMKV_UNUSABLE,
    ELYSIUMKV_FENCED,
    ELYSIUMKV_CONFIG,
    ELYSIUMKV_IO,
    ELYSIUMKV_STALLED,
    ELYSIUMKV_UNSUPPORTED,
    ELYSIUMKV_STALE,
    /* A transient store lost its contents, so reads are refused until the embedder replays the
     * gap and calls elysiumkv_mark_recovery_complete. Writes are not refused: the replay is made
     * of them. */
    ELYSIUMKV_RECOVERY_REQUIRED
} elysiumkv_status;

typedef enum { ELYSIUMKV_COMPRESSION_NONE = 0, ELYSIUMKV_COMPRESSION_LZ4, ELYSIUMKV_COMPRESSION_ZSTD }
    elysiumkv_compression;

typedef enum { ELYSIUMKV_DURABLE = 0, ELYSIUMKV_TRANSIENT } elysiumkv_durability;

/* Thread-local detail for the last failure on this thread. Valid until the next
 * failing call on the same thread. Never NULL. */
ELYSIUMKV_API const char* elysiumkv_last_error(void);

/* Compiled-in library version, so a binding can check what it loaded. */
ELYSIUMKV_API const char* elysiumkv_version(void);

/* What this build can actually do. The remote implementations are an optional
 * component — aws-sdk-cpp is by far the heaviest dependency here and an embedder
 * with no cold tier should not pay for it — but **the ABI shape must not vary
 * with a build flag**: a binding that resolves symbols at load time would then
 * fail to load rather than fail to find a feature, and its coverage tests could
 * no longer be a plain set comparison. So the remote constructors are always
 * present, and this is how a binding asks whether they will work. */
#define ELYSIUMKV_FEATURE_AWS 1u

ELYSIUMKV_API uint32_t elysiumkv_features(void);

/* --- configuration ---------------------------------------------------------
 *
 * Built by calls rather than by filling in a struct, because a struct layout
 * crossing the boundary is exactly what this ABI is designed to avoid.
 */

ELYSIUMKV_API elysiumkv_options* elysiumkv_options_create(void);
ELYSIUMKV_API void elysiumkv_options_destroy(elysiumkv_options*);

/* Tiers (ARCHITECTURE.md "A tier is not a level"), appended hot to cold. `store` is a handle from one of the
 * blob-store constructors below. A negative or zero bound means "unset".
 *
 * The last tier must not bound age, and must be durable; violations are reported
 * by elysiumkv_open as ELYSIUMKV_CONFIG.
 *
 * `max_bytes` is the *tier's* capacity, evicted oldest-first. There is no
 * per-file size bound: this function used to take one, and it was removed because
 * size gave a second, independent route to a colder tier and placement has to be
 * monotone in age alone. To keep large files off a fast tier, lower that level's
 * target_file_bytes so large files are not produced. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_add_tier(elysiumkv_options*, void* store,
                                        elysiumkv_durability durability, int64_t max_age_ms,
                                        int64_t max_bytes, int64_t stall_age_ms);

/* Levels (ARCHITECTURE.md "Compaction"), LSM structure only — no storage decisions. `level` may skip
 * numbers; gaps inherit the nearest shallower entry. A negative bound is
 * "unset". */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_level(elysiumkv_options*, int level,
                                         elysiumkv_compression compression, int64_t max_bytes,
                                         int max_files, int slowdown_at, int stop_at,
                                         size_t target_file_bytes);

/* Everything on Options that is neither a tier nor a level, in one call. ARCHITECTURE.md "The ABI boundary"
 * forbids per-field setters on an aggregate: with nine of them a half-built
 * Options is representable, and the binding that forgets one gets a silently
 * different engine rather than a compile error.
 *
 * A zero numeric field means "leave the engine default". The two flags are
 * tri-state because `false` is a meaningful setting that zero cannot tell apart
 * from "unset": negative keeps the default, zero disables, positive enables.
 * `block_on_stall` defaults to enabled, so passing 0 there is a real change —
 * a write that would stall then returns ELYSIUMKV_STALLED instead of blocking.
 * The stall valve itself is not configurable off.
 *
 * `obsolete_retention_ms` defers deleting an object this instance superseded, so a
 * *read-only* instance in another process holding an older version can still read
 * it. The collector cannot see that reader — liveness is tracked per process — so
 * this delay is the only thing protecting it. Zero deletes immediately, which is
 * correct when nothing else has the store open.
 *
 * `orphan_retention_ms` is how long an object must be *continuously observed*
 * unreferenced before the sweep deletes it, and it protects a concurrently-writing
 * process. Zero leaves the engine default of 24 hours; there is no configuration in
 * which deleting an object seen unreferenced once is correct, which is why the way
 * to switch the sweep off is orphan_sweep_interval_ms rather than this. Must be at
 * least obsolete_retention_ms — elysiumkv_open reports ELYSIUMKV_CONFIG otherwise,
 * because a crash empties the pending queue and a superseded object comes back as
 * an orphan protected by this window alone.
 *
 * `orphan_sweep_interval_ms` is how often to list the stores looking for orphans.
 * Zero disables the sweep, which costs storage and nothing else: correctness never
 * depends on reclamation happening.
 *
 * `flush_interval_ms` is the second, independent flush trigger: the memtable is flushed once it
 * has been open this long even if it never reaches `memtable_bytes`. Zero leaves it unset, so
 * size alone decides. It bounds how long a write can sit in memory under a trickle of traffic,
 * which no tier age bound can do — those act on files, and an unflushed memtable is not one.
 *
 * `maintenance_interval_ms` is how often the maintenance coordinator reconciles: it evaluates
 * every background policy — flush, compaction, migration off a transient tier, capacity eviction,
 * obsolete-object collection — against current state and the clock, and dispatches what is due.
 * It exists because a policy driven by time needs a trigger that is not a write. Zero leaves the
 * default of one second. Not a latency knob: the interval is the smallest term in the exposure
 * window `max_age + interval + queueing behind an in-flight compaction + the migration itself`.
 * An idle tick performs no version scan, which is what makes a short default affordable across
 * many instances in one process.
 *
 * `compaction_window_bytes` is how much of a compaction input is read at a time; zero leaves the
 * default. Total requests are `input bytes / this`, which against object storage is what a
 * compaction costs — and it is traded directly against memory, because every input of a compaction
 * holds two of these at once and they are all live together. Charged to `memory_budget`.
 *
 * `allow_reads_before_recovery` is tri-state like the flags beside it: negative leaves it, zero
 * refuses reads until `elysiumkv_mark_recovery_complete`, positive serves them. Off by default,
 * because what survives a discard is *wrong* rather than merely incomplete and a flag an embedder
 * has to know to check makes noticing it opt-in. Writes are never refused either way. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_configure(elysiumkv_options*, void* manifest_catalog,
                                                     void* memory_budget,
                                                     size_t memtable_bytes, size_t block_bytes,
                                                     size_t block_cache_bytes,
                                                     size_t reader_cache_bytes,
                                                     int bloom_bits_per_key,
                                                     size_t max_compaction_bytes,
                                                     size_t compaction_window_bytes,
                                                     int manifest_edits_per_generation,
                                                     int paranoid_checks, int block_on_stall,
                                                     int allow_reads_before_recovery,
                                                     uint64_t flush_interval_ms,
                                                     uint64_t maintenance_interval_ms,
                                                     uint64_t obsolete_retention_ms,
                                                     uint64_t orphan_retention_ms,
                                                     uint64_t orphan_sweep_interval_ms);

/* Compaction tuning that is a workload judgement rather than a capacity one.
 *
 * A separate call rather than two more positions on the fifteen-argument one above: appending to a
 * shipped signature breaks every existing caller, and a new symbol breaks none.
 *
 * `tombstone_density_trigger` compacts a file once that fraction of its entries are tombstones.
 * Zero, the default, leaves it off. It exists because the size ratios cannot express it: a
 * delete-heavy store whose levels stay inside their byte and file budgets never trips a compaction,
 * so the tombstones accumulate and every scan over the deleted region pays to skip them — visible
 * only as scans getting slower.
 *
 * `tombstone_density_min_entries` is the floor a file must reach before its density counts. Zero
 * leaves the default. Without it a file of two entries, one a tombstone, scores 0.5 and fires a
 * compaction that rewrites nothing — then does it again on the output. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_configure_compaction(elysiumkv_options*,
                                                     double tombstone_density_trigger,
                                                     uint64_t tombstone_density_min_entries);

/* Spreads the two age triggers so instances that reached the same state at the same moment do not
 * act on it at the same moment. Both are fractions in [0, 1]; zero, the default, keeps the trigger
 * exact. Anything outside that range is ELYSIUMKV_CONFIG.
 *
 * Stores drift apart on their own, and this is for the times they do not: a rebuild stamps every
 * file it replays within the same few minutes, so the whole store crosses `max_age` together and
 * migrates as one burst instead of a trickle. For an embedder that rebuilds on assignment that
 * repeats after every rebalance.
 *
 * `age_jitter` applies per file, and only ever fires it **earlier** — a transient tier's `max_age`
 * is an exposure bound the engine promises, so a file may cross early but never late. The offset is
 * derived from the file's number and write time rather than rolled, so a reopen recomputes it
 * instead of re-clustering what it just spread. A tier's `stall_age` is deliberately left exact:
 * it is an alarm, and blurring it only makes the alarm harder to read.
 *
 * `flush_interval_jitter` applies per memtable and fires either way, since a late flush costs
 * replay on restart and breaks no promise. What it smooths is compaction queue depth — instances
 * opened together flush together, and their L0 files reach the compactor as one wave. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_configure_jitter(elysiumkv_options*,
                                                     double age_jitter,
                                                     double flush_interval_jitter);

/* --- diagnostics -----------------------------------------------------------
 *
 * The engine has no logger of its own and nothing here is persisted; `write` is the only way it
 * says anything. `event` is a stable code (elysiumkv_log_event) so a binding can route and count
 * without parsing `message`.
 *
 * **Called on engine threads — flush, compaction and maintenance — synchronously, with no engine
 * lock held.** A sink that blocks applies backpressure to the operation that produced the line, so
 * hand it to an async appender rather than doing work in it. It must not call back into the store.
 *
 * `message` is not NUL-terminated and is valid only for the duration of the call; copy to keep it.
 *
 * `min_level` follows elysiumkv_log_level: 0 debug, 1 info, 2 warn, 3 error, 4 off. A NULL vtable
 * turns logging off, which is the default and costs one comparison per candidate line. */
typedef enum {
    ELYSIUMKV_LOG_DEBUG = 0,
    ELYSIUMKV_LOG_INFO = 1,
    ELYSIUMKV_LOG_WARN = 2,
    ELYSIUMKV_LOG_ERROR = 3,
    ELYSIUMKV_LOG_OFF = 4
} elysiumkv_log_level;

/* Append, never renumber: a binding maps these to its own names. */
typedef enum {
    ELYSIUMKV_EVENT_FLUSH_COMPLETE = 0,
    ELYSIUMKV_EVENT_COMPACTION_COMPLETE = 1,
    ELYSIUMKV_EVENT_COMPACTION_FAILED = 2,
    ELYSIUMKV_EVENT_MIGRATION_COMPLETE = 3,
    ELYSIUMKV_EVENT_BACKGROUND_FAILURE = 4,
    ELYSIUMKV_EVENT_BACKGROUND_RETRY = 5,
    ELYSIUMKV_EVENT_STALL_ENTERED = 6,
    ELYSIUMKV_EVENT_STALL_LEFT = 7,
    ELYSIUMKV_EVENT_STORES_DISCARDED = 8,
    ELYSIUMKV_EVENT_FENCED = 9,
    ELYSIUMKV_EVENT_GENERATION_ROLLED = 10,
    ELYSIUMKV_EVENT_ORPHANS_RECLAIMED = 11
} elysiumkv_log_event;

typedef struct {
    void* context;
    void (*write)(void* context, int level, int event, const char* message, size_t len);
} elysiumkv_logger_vtable;

ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_logger(elysiumkv_options*,
                                                     const elysiumkv_logger_vtable*,
                                                     int min_level);

/* --- seams -----------------------------------------------------------------
 *
 * The built-in on-disk implementations, plus function-pointer vtables so a
 * binding can supply a store or catalog written in its own language without
 * touching C++. Handles are owned by the caller and must outlive the DB.
 */

ELYSIUMKV_API void* elysiumkv_disk_blob_store_create(const char* root_directory, const char* store_id);
ELYSIUMKV_API void elysiumkv_blob_store_destroy(void*);

ELYSIUMKV_API void* elysiumkv_disk_manifest_catalog_create(const char* directory);
ELYSIUMKV_API void elysiumkv_manifest_catalog_destroy(void*);

/* A store supplied by the binding. Every callback receives `context`.
 *
 * `get` writes at most `len` bytes from `offset` into `out` and sets `*out_len`
 * to what it wrote; a read past the end is truncated rather than an error.
 * `list` reports names through `emit`, once per name.
 *
 * The failure rules of ARCHITECTURE.md "Immutable named objects" are the implementation's to honour, and the engine
 * depends on them: ELYSIUMKV_NOT_FOUND means the object is *definitely* absent,
 * ELYSIUMKV_IO means the store could not determine anything. Returning NOT_FOUND
 * where IO is meant is how a store loses data that was never lost. */
typedef struct {
    void* context;
    const char* (*id)(void* context);
    elysiumkv_status (*get)(void* context, const char* name, uint64_t offset, size_t len,
                          uint8_t* out, size_t* out_len);
    elysiumkv_status (*put)(void* context, const char* name, const uint8_t* bytes, size_t len);
    elysiumkv_status (*remove)(void* context, const char* name);
    /* Optional. NULL means "loop over `remove`", which is what the engine did for
     * every store before batching existed.
     *
     * It is here because the engine collects obsolete objects in one call per
     * store, and without this a binding-supplied store is the only kind that
     * cannot see that: a remote store written in the binding's own language would
     * be back to one round trip per file after every compaction — the exact cost
     * `remove_many` exists to remove. */
    elysiumkv_status (*remove_many)(void* context, const char* const* names, size_t count);
    elysiumkv_status (*list)(void* context, const char* prefix,
                           void (*emit)(void* emit_context, const char* name), void* emit_context);
    /* Size of the largest object this store can hold, used to bound a read
     * buffer. Zero means the default of 64 MiB. */
    size_t max_object_bytes;
} elysiumkv_blob_store_vtable;

ELYSIUMKV_API void* elysiumkv_blob_store_from_vtable(const elysiumkv_blob_store_vtable*);

/* --- the shared memory budget (ARCHITECTURE.md "A process-wide memory budget") -----------------------------------------
 *
 * **Per process, not per instance**, which is the entire reason it is a separate handle
 * rather than a number on elysiumkv_options. Many embedders run one instance per shard,
 * partition or tenant, so memtable and cache sizing multiplies by instance count; a
 * per-instance constant is the wrong unit. One budget is created and passed to every
 * instance and every in-memory cache in the process.
 *
 * When the budget is exceeded the engine sheds, in this order: evict the block cache,
 * flush memtables, then stall writes. **No write ever fails because of it** — refusing a
 * put because another instance is using memory would be a surprising failure mode.
 *
 * Must outlive every options object, database and cache it was given to. */
ELYSIUMKV_API void* elysiumkv_memory_budget_create(size_t total_bytes);
ELYSIUMKV_API void elysiumkv_memory_budget_destroy(void*);
/* Bytes currently charged. May exceed the total — see elysiumkv_stats_snapshot. */
ELYSIUMKV_API size_t elysiumkv_memory_budget_used(const void*);

/* --- cache layers (ARCHITECTURE.md "Caches chain") -----------------------------------------------------
 *
 * Anything faster in front of an authoritative store, as a decorator over the same
 * seam. Because a cache is itself a store, they chain: memory over disk over S3 is
 * two calls. The handle they return is a store handle like any other — pass it to
 * elysiumkv_options_add_tier, destroy it with elysiumkv_blob_store_destroy.
 *
 * `delegate` is a store handle, and **must outlive the cache built over it**: the
 * cache holds a reference to the store, not a copy of the handle.
 *
 * A cache may never be the innermost store of a tier — a cache holds only copies, so
 * making one the only home for a file is the one arrangement discard has nothing to
 * fall back on. elysiumkv_open reports ELYSIUMKV_CONFIG for it.
 *
 * These report a status for the same reason the remote constructors do: a bad
 * argument is ELYSIUMKV_CONFIG and a cache directory that cannot be created is
 * ELYSIUMKV_IO, and a NULL return cannot say which. */

/* `cache_on_write` populates on put, write-through and never write-back. It pays
 * mostly for L0, whose files are read almost immediately by the next L0-to-L1
 * compaction; a fresh deep-level file will not be read for a long time. */
ELYSIUMKV_API elysiumkv_status elysiumkv_disk_cache_blob_store_create(void* delegate,
                                                                const char* directory,
                                                                size_t max_cache_bytes,
                                                                int cache_on_write, void** out);

/* Worth having over a *remote* delegate and mostly not otherwise: over local files it
 * duplicates the OS page cache, which does the same job with better eviction for
 * free. Its non-overlapping role is buffering large sequential reads against a remote
 * store, which the block cache is bypassed for by design. */
/* `budget` may be NULL, which bounds this cache by `max_cache_bytes` alone —
 * appropriate for a single-instance process and nothing else. */
ELYSIUMKV_API elysiumkv_status elysiumkv_memory_cache_blob_store_create(void* delegate, void* budget,
                                                                  size_t max_cache_bytes,
                                                                  int cache_on_write, void** out);

/* The same two caches, with a fetch granularity.
 *
 * A miss is rounded out to a chunk of `fetch_granularity` bytes and the whole chunk is cached, so a
 * sequential read costs one request per chunk rather than one per block. Against a remote store
 * that is the difference between one round trip per block and one per chunk, and unlike a readahead
 * inside the iterator it needs no notion of a scan: a point lookup whose neighbour is read later is
 * served from what the first one pulled.
 *
 * Amplification is bounded by the chunk, not by the object: a small read against a large file pulls
 * one chunk, never the file. Zero fetches exactly what was asked, which is what the constructors
 * above do.
 *
 * Separate symbols rather than two more arguments, for the same reason as the compaction call. */
ELYSIUMKV_API elysiumkv_status elysiumkv_memory_cache_blob_store_create_chunked(void* delegate,
                                                                  void* budget,
                                                                  size_t max_cache_bytes,
                                                                  int cache_on_write,
                                                                  size_t fetch_granularity,
                                                                  void** out);
ELYSIUMKV_API elysiumkv_status elysiumkv_disk_cache_blob_store_create_chunked(void* delegate,
                                                                const char* directory,
                                                                size_t max_cache_bytes,
                                                                int cache_on_write,
                                                                size_t fetch_granularity,
                                                                void** out);

/* Hits and misses of a caching blob store — one made by any of the four calls above.
 *
 * The hit rate is what says whether a cache in front of a remote store is earning its space: a
 * miss there is a round trip, so this is read latency rather than a curiosity. The engine's own
 * block-cache counters in elysiumkv_stats are a different layer and answer a different question.
 *
 * ELYSIUMKV_CONFIG when the handle is not a caching store. */
ELYSIUMKV_API elysiumkv_status elysiumkv_blob_cache_stats(void* store, uint64_t* hits,
                                                    uint64_t* misses);

/* --- remote seams (ARCHITECTURE.md "The ABI boundary", ARCHITECTURE.md "Ownership is one compare-and-set") --------------------------------------------
 *
 * S3 and DynamoDB, which is what makes a cold tier actually cold. Absent unless
 * the library was built with them; ELYSIUMKV_FEATURE_AWS says which, and these
 * return ELYSIUMKV_CONFIG naming the missing build option otherwise.
 *
 * **These report a status and hand back the handle, unlike the local
 * constructors that just return a pointer.** They can fail two ways that must
 * not be conflated: a bad bucket or table name is ELYSIUMKV_CONFIG and retrying
 * is pointless, while `create_table_if_missing` against an unreachable DynamoDB
 * is ELYSIUMKV_IO and retrying is the correct response. A NULL return cannot say
 * which, and ELYSIUMKV_IO is the retryable class and nothing else is.
 *
 * NULL for `endpoint` means the real service; anything else points at
 * LocalStack or a compatible endpoint. NULL credentials mean the SDK's own
 * credential chain — environment, profile, instance role — which is what a
 * deployed process should use. Zero for any timeout means the built-in default.
 *
 * Handles are destroyed with elysiumkv_blob_store_destroy /
 * elysiumkv_manifest_catalog_destroy, like every other store and catalog. */

/* `prefix` separates stores sharing a bucket; `store_id` overrides the derived
 * `s3://bucket/prefix` identity, which the manifest records — pass NULL unless
 * a store is being renamed. The two timeouts are separate because compaction
 * reads whole files while a point lookup reads a footer: one budget cannot
 * serve both. */
ELYSIUMKV_API elysiumkv_status elysiumkv_s3_blob_store_create(
    const char* bucket, const char* prefix, const char* region, const char* endpoint,
    const char* access_key, const char* secret_key, int64_t point_timeout_ms,
    int64_t bulk_timeout_ms, const char* store_id, void** out);

/* The prefix should differ from any blob store's on the same bucket, or manifest
 * objects and SSTs share a namespace. */
ELYSIUMKV_API elysiumkv_status elysiumkv_s3_manifest_catalog_create(
    const char* bucket, const char* prefix, const char* region, const char* endpoint,
    const char* access_key, const char* secret_key, int64_t point_timeout_ms,
    int64_t bulk_timeout_ms, void** out);

/* `store_id` is the partition key value: one store's manifest state, kept apart
 * from any other sharing the table. `create_table_if_missing` is the only
 * argument here that performs I/O, and should be off outside tests — a
 * production table belongs to whatever provisions infrastructure, and creating
 * one silently would hide a misconfigured name behind a working store. */
ELYSIUMKV_API elysiumkv_status elysiumkv_dynamo_manifest_catalog_create(
    const char* table, const char* store_id, const char* region, const char* endpoint,
    const char* access_key, const char* secret_key, int64_t timeout_ms,
    int create_table_if_missing, void** out);

/* --- open and close --------------------------------------------------------- */

/* Refuses a configuration containing any Transient tier with ELYSIUMKV_CONFIG —
 * a check, not a documented precondition, because adding one later must not
 * leave existing call sites silently serving stale values. */
ELYSIUMKV_API elysiumkv_status elysiumkv_open(const elysiumkv_options*, elysiumkv_db** out);

/* Opens any configuration and reports discard state. `discarded_stores` is
 * filled with up to `*n_stores` store-id pointers owned by the DB and valid
 * until it is closed; `*n_stores` is set to how many there were, which may
 * exceed what was written. Any out parameter may be NULL. */
ELYSIUMKV_API elysiumkv_status elysiumkv_open_with_result(const elysiumkv_options*, elysiumkv_db** out,
                                        const char** discarded_stores, size_t* n_stores,
                                        uint64_t* discarded_files, bool* requires_recovery);

/* Closing with pins or iterators outstanding is a programming error, and the
 * count is returned so a binding's tests can fail on it — a leaked pin holds a
 * block-cache entry forever. Zero means a clean close.
 *
 * Closing releases every outstanding pin and detaches every live iterator: a
 * detached iterator yields nothing further, and destroying it afterwards is
 * safe. A pin handle, by contrast, is only meaningful together with its db, so
 * elysiumkv_unpin after close is a use-after-free like any other.
 *
 * Closing also *attempts* a flush, because there is no write-ahead log and a
 * memtable dropped on a clean shutdown is lost for no reason. The attempt is
 * best-effort and its failure is not reported here — elysiumkv_flush is still
 * the only way to know. Use elysiumkv_close_without_flush to skip it. */
ELYSIUMKV_API uint64_t elysiumkv_close(elysiumkv_db*);

/* Closes without attempting the flush that elysiumkv_close performs, discarding
 * whatever the memtable still holds. That is what a crash leaves behind, and the
 * two callers who want it are a test that means to lose the writes and an
 * embedder that has decided they are not worth the shutdown latency. Returns the
 * outstanding pin and iterator count exactly as elysiumkv_close does. */
ELYSIUMKV_API uint64_t elysiumkv_close_without_flush(elysiumkv_db*);

/* --- reads ------------------------------------------------------------------ */

/* Zero-copy. On ELYSIUMKV_OK, `*value` points into a pinned block and stays valid
 * until elysiumkv_unpin(db, *pin). On any other status nothing is pinned. */
ELYSIUMKV_API elysiumkv_status elysiumkv_get(elysiumkv_db*, const uint8_t* key, size_t key_len,
                           const uint8_t** value, size_t* value_len, uint64_t* pin);
ELYSIUMKV_API void elysiumkv_unpin(elysiumkv_db*, uint64_t pin);

/* Copies into a caller buffer instead of pinning. Sets `*value_len` to the full
 * value length even when the buffer is too small, in which case the status is
 * ELYSIUMKV_OK and only `capacity` bytes were written — check the length. */
ELYSIUMKV_API elysiumkv_status elysiumkv_get_copy(elysiumkv_db*, const uint8_t* key, size_t key_len, uint8_t* value,
                                size_t capacity, size_t* value_len);

/* Outstanding pins, for the leak check a binding can run itself. */
ELYSIUMKV_API uint64_t elysiumkv_pins_outstanding(const elysiumkv_db*);

/* --- writes ----------------------------------------------------------------- */

/* A value may be at most 1 MiB and a key at most 64 KiB; anything larger is
 * ELYSIUMKV_CONFIG. The limit is refused here rather than at flush, because a
 * memtable holding an entry no SST can represent could never be drained — and
 * because a write that reports success and cannot be read back is the worst
 * failure this API has. */
ELYSIUMKV_API elysiumkv_status elysiumkv_put(elysiumkv_db*, const uint8_t* key, size_t key_len, const uint8_t* value,
                           size_t value_len);
ELYSIUMKV_API elysiumkv_status elysiumkv_delete(elysiumkv_db*, const uint8_t* key, size_t key_len);
/* Drops every key below `key` in one manifest edit, rather than one tombstone
 * per key.
 *
 * Monotone: a call at or below the current point is a no-op returning OK, so
 * this is safe to drive from a loop that does not track what it already asked
 * for. There is no un-truncate.
 *
 * Visibility changes at once; space returns over time. A file whose keys are all
 * below the point is unlinked whole; one straddling it is narrowed by the next
 * compaction. An open iterator is unaffected — it holds the version it started
 * on, exactly as it does across a compaction. */
ELYSIUMKV_API elysiumkv_status elysiumkv_truncate_below(elysiumkv_db*, const uint8_t* key,
                                             size_t key_len);

/* Deletes every key in [lower, upper) — the counterpart to elysiumkv_truncate_below
 * for a range that is not a prefix of the keyspace. A tenant sitting in the
 * middle of a keyspace is the case that needs it.
 *
 * Bounds keep their meaning rather than their role: lower is included, upper is
 * not, the same convention the iterators use. An empty or inverted range deletes
 * nothing and returns OK, exactly as an iterator over those bounds yields
 * nothing.
 *
 * Unlike a truncation point this is not permanent: the range may be written to
 * again straight away, and a later write wins. Nor is it as cheap — a truncation
 * moves one value in the manifest, while this writes a tombstone that reads in
 * the range consult until compaction resolves it. Space returns as the covered
 * files are rewritten, or at once for a file the range covers whole. */
ELYSIUMKV_API elysiumkv_status elysiumkv_delete_range(elysiumkv_db*, const uint8_t* lower,
                                           size_t lower_len, const uint8_t* upper,
                                           size_t upper_len);

/* Whether a `delete_range(lower, upper)` has finished travelling through the tree: no file at any
 * level still holds data in the band. Writes 1 or 0 to `erased`.
 *
 * Deletion in an LSM is a promise about the future — the tombstone is recorded now and the bytes go
 * when compaction reaches them — so "has it actually gone?" normally has no answer. This one costs
 * no reads: every file's key range is in the manifest already.
 *
 * **Conservative.** A recorded range is a hull, so a file can overlap the band while holding no key
 * in it: 0 means "possibly still present" and carries no information, while 1 means every file that
 * could have held one is gone. A band hidden by a truncation point is *not* erased — the objects
 * are there until the reclaim collects them, and unreadable is not the same as gone.
 *
 * Answers about files, so a write made into the band after the deletion is a new write rather than
 * a survival, and sits in the memtable until it is flushed. Available on a read-only handle. */
ELYSIUMKV_API elysiumkv_status elysiumkv_range_is_erased(elysiumkv_db*, const uint8_t* lower,
                                           size_t lower_len, const uint8_t* upper,
                                           size_t upper_len, int* erased);

ELYSIUMKV_API elysiumkv_batch* elysiumkv_batch_create(void);
ELYSIUMKV_API void elysiumkv_batch_destroy(elysiumkv_batch*);
ELYSIUMKV_API void elysiumkv_batch_put(elysiumkv_batch*, const uint8_t* key, size_t key_len, const uint8_t* value,
                       size_t value_len);
ELYSIUMKV_API void elysiumkv_batch_delete(elysiumkv_batch*, const uint8_t* key, size_t key_len);

/* Deletes [lower, upper) as part of the batch. Order within the batch decides
 * what survives: a put after this one lands on top of the range, a put before it
 * is covered. */
ELYSIUMKV_API void elysiumkv_batch_delete_range(elysiumkv_batch*, const uint8_t* lower,
                                      size_t lower_len, const uint8_t* upper, size_t upper_len);
ELYSIUMKV_API size_t elysiumkv_batch_size(const elysiumkv_batch*);
/* Applied as a unit: the whole batch lands in one memtable. */
ELYSIUMKV_API elysiumkv_status elysiumkv_write(elysiumkv_db*, elysiumkv_batch*);

ELYSIUMKV_API elysiumkv_status elysiumkv_flush(elysiumkv_db*);
/* Rewrites every file at `level` under current compression and placement. One
 * pass, so it terminates; a second call does nothing. */
ELYSIUMKV_API elysiumkv_status elysiumkv_compact_level(elysiumkv_db*, int level);

/* --- iteration -------------------------------------------------------------- */

/* `lo` NULL means unbounded below, `hi` NULL unbounded above. */
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_create(elysiumkv_db*, const uint8_t* lo, size_t lo_len,
                                   const uint8_t* hi, size_t hi_len, elysiumkv_iter** out);
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_prefix(elysiumkv_db*, const uint8_t* prefix, size_t prefix_len,
                                   elysiumkv_iter** out);

/* The same two scans, descending: elysiumkv_iter_next still means "advance", and
 * it advances towards smaller keys. The first call yields the largest key in
 * range.
 *
 * Bounds mean what they mean forward — `lo` inclusive, `hi` exclusive — so both
 * directions describe the same set and only the delivery order differs.
 *
 * Separate entry points rather than a flag on elysiumkv_iter_create, because adding
 * a parameter to a shipped signature is an ABI break and a new symbol is not. */
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_create_reverse(elysiumkv_db*, const uint8_t* lo,
                                                   size_t lo_len, const uint8_t* hi,
                                                   size_t hi_len, elysiumkv_iter** out);
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_prefix_reverse(elysiumkv_db*, const uint8_t* prefix,
                                                   size_t prefix_len, elysiumkv_iter** out);
/* The first call positions at the first key in range. Key and value are valid
 * only after it returns true, and only until the next call. */
ELYSIUMKV_API bool elysiumkv_iter_next(elysiumkv_iter*);
ELYSIUMKV_API void elysiumkv_iter_key(elysiumkv_iter*, const uint8_t** key, size_t* key_len);
ELYSIUMKV_API void elysiumkv_iter_value(elysiumkv_iter*, const uint8_t** value, size_t* value_len);
/* Advances up to `cap` bytes' worth of entries in one call, packing them into
 * `buf` as `u32 key_len | key | u32 value_len | value`, little-endian, repeated
 * `*out_count` times over `*out_bytes` bytes.
 *
 * This exists because of a measurement, not a hunch (ARCHITECTURE.md "The ABI boundary"). Per entry, a scan
 * through next/key/value costs about 272ns from Java against roughly 36ns in
 * C++ — and only ~37ns of that is the advance. The rest is three crossings per
 * entry on the path ARCHITECTURE.md "Absence is an answer, not an error" calls the dominant read pattern. Batching amortises them.
 *
 * It trades zero-copy for crossings, deliberately: the bytes are copied into the
 * caller's buffer rather than borrowed from the block. That is the right trade
 * for a scan and the wrong one for a point lookup, which is why elysiumkv_get
 * still pins.
 *
 * `*out_count == 0` with ELYSIUMKV_OK means the iteration is exhausted — unless
 * `*out_bytes` is non-zero, which means the next entry needs a buffer that
 * large and none was made. Check elysiumkv_iter_status afterwards, as with
 * next(). */
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_next_batch(elysiumkv_iter*, uint8_t* buf, size_t cap,
                                                   size_t* out_count, size_t* out_bytes);

/* Check after next() returns false: exhaustion and failure look alike otherwise. */
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_status(elysiumkv_iter*);
ELYSIUMKV_API void elysiumkv_iter_destroy(elysiumkv_iter*);

/* --- statistics -------------------------------------------------------------
 *
 * One call for the whole aggregate, serialized into a caller buffer. ARCHITECTURE.md "The ABI boundary" — a
 * snapshot assembled from per-field accessors is *torn* — each call observes a
 * different instant of a live engine, so the compaction counters will not match
 * the level file counts that are supposed to explain them. Reading it in one
 * call is the only way the cross-field relationships hold, and they do: every
 * file sits in exactly one level and exactly one tier, so summing `bytes` over
 * levels and over tiers must give the same total.
 *
 * Serializing rather than filling a struct keeps every binding's type shapes out
 * of the glue and survives a field being added.
 *
 * Layout, little-endian throughout, all offsets from the start of the buffer:
 *
 *   header
 *     u32 format_version            currently 1
 *     u32 header_bytes              offset of the first level record
 *     u32 level_record_bytes
 *     u32 tier_record_bytes
 *     u32 level_count
 *     u32 tier_count
 *     u8  requires_recovery
 *     u8  reserved[7]
 *     u64 memtable_bytes, memtable_age_ms,
 *         compactions, compaction_bytes_read, compaction_bytes_written,
 *         migrations, migration_bytes,
 *         stalled_total_ms, stall_count,
 *         block_cache_hits, block_cache_misses, block_cache_bytes,
 *         pins_outstanding,
 *         reader_cache_hits, reader_cache_misses, reader_cache_bytes, open_readers,
 *         memory_budget_used, memory_budget_total, budget_sheds,
 *         flushes                                     offset 192
 *     u64 durable_watermark                            offset 200
 *     u8  watermark_present                            offset 208, 0 when unset
 *     u8  reserved[7]                                  offset 209
 *                                                      header_bytes = 216
 *
 * `watermark_present` exists because **zero is a valid watermark** — a store at the
 * start of its log — so the value alone cannot express absence. An exporter omits
 * the series entirely when the flag is zero rather than publishing zero.
 *   level record, level_count of them
 *     i32 level, i32 file_count, u64 bytes, u64 oldest_file_age_ms,
 *     i32 files_stale_codec, u8 age_triggered, u8 stalling, u8 reserved[2]
 *   tier record, tier_count of them
 *     i32 tier, i32 file_count, u64 bytes, u64 oldest_file_age_ms,
 *     i32 files_pending_migration, u8 stalling, u8 reserved[3]
 *
 * **Decode by the declared sizes, not by sizeof.** A reader that starts records
 * at `header_bytes` and steps by `*_record_bytes`, ignoring trailing bytes it
 * does not recognise, keeps working when a field is appended. One that hardcodes
 * the offsets in this comment does not.
 *
 * Pass buf = NULL, cap = 0 to ask for the size. When `cap` is too small nothing
 * is written, `*out_bytes` is set to what was needed, and the status is still
 * ELYSIUMKV_OK — check the length, as with elysiumkv_get_copy. */
ELYSIUMKV_API elysiumkv_status elysiumkv_stats_snapshot(const elysiumkv_db*, uint8_t* buf, size_t cap,
                                                  size_t* out_bytes);

/* Clears requires_recovery after a discard (ARCHITECTURE.md "A tier is not a level"). The only way to clear it. */
ELYSIUMKV_API elysiumkv_status elysiumkv_mark_recovery_complete(elysiumkv_db*);

/* --- watermark --------------------------------------------------------------
 *
 * Records that every write completed so far is at a position at or before
 * `position` in whatever log the embedder replays — a changelog offset, typically.
 * The engine orders it, carries it with the data and hands it back at the next
 * open; it never invents, interpolates or interprets one.
 *
 * It is a *position*, not a time, and unrelated to a tier's max_age. Positions
 * must be non-decreasing; a decreasing one returns ELYSIUMKV_CONFIG rather than
 * being clamped, because clamping would hide a replay that went backwards.
 *
 * Cheap: one store under the lock the write path already takes. It forces no
 * flush and writes no manifest, so it can be called as often as the embedder
 * commits — the value becomes durable when the memtable holding it is flushed,
 * which is why elysiumkv_flush promotes it immediately. */
/* --- read-only -------------------------------------------------------------
 *
 * Opens without taking ownership: no manifest write of any kind, no background
 * threads, no reclamation, no compare-and-set. Several may be open at once,
 * alongside a writer, and there is no registration and so no limit on how many.
 *
 * Refuses a store with no manifest (ELYSIUMKV_NOT_FOUND) rather than creating one,
 * and refuses a store whose Transient tier has lost files, because repairing that
 * is a manifest write and serving a version with holes presents stale values as
 * current.
 *
 * **The C ABI cannot express the C++ split**, where a read-only handle is a
 * different type and passing it somewhere that writes is a compile error. Here
 * there is one handle type and the write entry points — put, remove, write, flush,
 * compact_level, set_watermark — return ELYSIUMKV_CONFIG on a read-only handle.
 *
 * The writer must set obsolete_retention_ms for any of this to be safe: its
 * collector cannot see a reader in another process, so that delay is the only
 * thing standing between a compaction there and a vanished file here. A reader
 * that falls behind the window is told ELYSIUMKV_STALE, never ELYSIUMKV_CORRUPT. */
ELYSIUMKV_API elysiumkv_status elysiumkv_open_read_only(const elysiumkv_options*, elysiumkv_db** out);

/* Re-reads the manifest and installs the newest version. Explicit, never
 * automatic: two reads in one logical operation must be able to see one version.
 * Open iterators are unaffected — an iterator holds the version it started on.
 * A no-op returning ELYSIUMKV_OK on a writable handle. */
ELYSIUMKV_API elysiumkv_status elysiumkv_refresh(elysiumkv_db*);

ELYSIUMKV_API elysiumkv_status elysiumkv_set_watermark(elysiumkv_db*, uint64_t position);

/* The last position whose effect on the store is known to have survived, as
 * established at *open*. Replaying only the positions **after** it yields the
 * same logical key-value state as replaying the entire log — exclusive, so `80`
 * means resume at `81`.
 *
 * A getter on the database rather than another out-parameter on
 * elysiumkv_open_with_result, which already carries five: the recovered watermark
 * is a property of the opened store, not of the open event.
 *
 * `*present` is set to 0 when nothing can be certified — no watermark was ever
 * set, or a lost transient store held data predating the first one — and the
 * embedder should replay from the beginning. Distinct from a watermark of zero.
 * `*out` is untouched when `*present` is 0.
 *
 * A restore must use this value, never one that has been through a metrics
 * pipeline: the stats buffer carries the *live* frontier for observation, and
 * many metrics systems round a uint64 through a double. */
ELYSIUMKV_API elysiumkv_status elysiumkv_watermark(elysiumkv_db*, uint64_t* out, bool* present);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* ELYSIUMKV_H */
