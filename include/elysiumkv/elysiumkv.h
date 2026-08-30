/* elysiumkv.h — the C ABI (ARCHITECTURE.md "The ABI boundary").
 *
 * C99-compatible. Opaque handles only: no struct layout crosses this boundary, so adding a field
 * to an engine type cannot break a compiled binding.
 *
 * Invariants at every entry point:
 *
 *   - Status codes, never exceptions. An exception crossing this boundary is undefined behaviour,
 *     so every function is wrapped in a catch-all.
 *   - ELYSIUMKV_IO is the only retryable class. Absence and failure-to-look stay distinct.
 *   - elysiumkv_get borrows a pointer into a cached block and returns a pin the caller must
 *     release; a leaked pin holds that block-cache entry for the life of the database.
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

/* Which optional components this build contains.
 *
 * The exported symbol set does not vary with build configuration: the remote constructors are
 * always present and report ELYSIUMKV_CONFIG when absent, so a binding resolving symbols at load
 * time cannot fail to load. This is how it asks whether they will work. */
#define ELYSIUMKV_FEATURE_AWS 1u

ELYSIUMKV_API uint32_t elysiumkv_features(void);

/* --- configuration ---------------------------------------------------------
 *
 * Built by calls rather than by filling in a struct, because a struct layout
 * crossing the boundary is exactly what this ABI is designed to avoid.
 */

ELYSIUMKV_API elysiumkv_options* elysiumkv_options_create(void);
ELYSIUMKV_API void elysiumkv_options_destroy(elysiumkv_options*);

/* Tiers (ARCHITECTURE.md "A tier is not a level"), appended hot to cold. `store` is a handle from
 * one of the blob-store constructors below. A negative or zero bound means "unset".
 *
 * The last tier must not bound age and must be durable; elysiumkv_open reports violations as
 * ELYSIUMKV_CONFIG.
 *
 * `max_bytes` is the tier's capacity, evicted oldest-first. Placement is monotone in age alone, so
 * there is no per-file size bound; to keep large files off a fast tier, lower that level's
 * target_file_bytes. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_add_tier(elysiumkv_options*, void* store,
                                        elysiumkv_durability durability, int64_t max_age_ms,
                                        int64_t max_bytes, int64_t stall_age_ms);

/* Levels (ARCHITECTURE.md "Compaction"), LSM structure only — no storage decisions. `level` may skip
 * numbers; gaps inherit the nearest shallower entry. A negative bound is
 * "unset". */
/* Replaces the level map with the geometric layout: L0 bounded by file count, each deeper level
 * `multiplier` times the capacity above it, and the last carrying none because it absorbs
 * everything. Choose `count` against expected total size; configured levels sitting empty cost
 * nothing. */
/* How long data lives before the engine drops it, measured from when it was written. Zero — the
 * default — never expires anything.
 *
 * Expiry by manifest edit: a file whose every write has outlived this is unlinked whole, nothing read
 * and nothing rewritten. Three limits follow from that and none of them is a detail. The granularity
 * is the file, not the key, so this buys "data older than X disappears" and not "this key expires at
 * X". A file is dropped when the sweep next finds it expired, so data may outlive the limit by up to
 * one `orphan_sweep_interval`. And a file expires only once no older file overlaps its range —
 * dropping one that shadows an older version of the same key would uncover that version rather than
 * remove the key. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_ttl(elysiumkv_options*, uint64_t ttl_ms);

ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_geometric_levels(elysiumkv_options*,
                                                                     uint64_t base,
                                                                     int multiplier, int count);

ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_level(elysiumkv_options*, int level,
                                         elysiumkv_compression compression, int64_t max_bytes,
                                         int max_files, int slowdown_at, int stop_at,
                                         size_t target_file_bytes);

/* Everything on Options that is neither a tier nor a level, in one call. One call rather than
 * per-field setters so a half-built Options is not representable.
 *
 * A zero numeric field leaves the engine default. The three flags are tri-state, because `false`
 * is a meaningful setting that zero cannot distinguish from unset: negative keeps the default,
 * zero disables, positive enables. `block_on_stall` defaults to enabled, so passing zero makes a
 * stalling write return ELYSIUMKV_STALLED rather than block. The stall valve cannot be disabled.
 *
 * `obsolete_retention_ms` defers deleting an object this instance superseded, so that a read-only
 * instance in another process holding an older version can still read it. Liveness is tracked per
 * process, so this delay is the only thing protecting that reader. Zero deletes immediately.
 *
 * `orphan_retention_ms` is how long an object must be *continuously observed* unreferenced before
 * the sweep deletes it, which is what protects a concurrently-writing process. Zero leaves the
 * default of 24 hours; switch the sweep off with orphan_sweep_interval_ms instead. Must be at
 * least obsolete_retention_ms, or elysiumkv_open reports ELYSIUMKV_CONFIG: a crash empties the
 * pending queue, and a superseded object then returns as an orphan protected by this window alone.
 *
 * `orphan_sweep_interval_ms` is how often to list the stores looking for orphans. Zero disables
 * the sweep, which costs storage only — correctness never depends on reclamation.
 *
 * `flush_interval_ms` flushes the memtable once it has been open this long, whatever its size, and
 * bounds how long a write sits in memory under a trickle of traffic. No tier age bound can do
 * that: those act on files, and an unflushed memtable is not one. Zero leaves it unset.
 *
 * `maintenance_interval_ms` is how often the coordinator evaluates every background policy against
 * current state and dispatches what is due. Zero leaves the default of one second. Not a latency
 * knob: it is the smallest term in the exposure window.
 *
 * `allow_reads_before_recovery` serves reads after a transient store is discarded, before the
 * embedder has replayed the gap. The default refuses them with ELYSIUMKV_RECOVERY_REQUIRED,
 * because a discarded store is wrong rather than incomplete. Writes are never refused either way.
 */
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
 * `tombstone_density_trigger` compacts a file once that fraction of its entries are tombstones.
 * Zero, the default, leaves it off. The trigger the size ratios cannot express: a delete-heavy
 * store staying inside its byte and file budgets never compacts, so tombstones accumulate and
 * every scan over the deleted region pays to skip them.
 *
 * `tombstone_density_min_entries` is the floor a file must reach before its density counts, so a
 * two-entry file holding one tombstone does not score 0.5 and compact itself repeatedly. Zero
 * leaves the default. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_configure_compaction(elysiumkv_options*,
                                                     double tombstone_density_trigger,
                                                     uint64_t tombstone_density_min_entries);

/* Spreads the two age triggers, so a store whose files all carry nearly the same write time — a
 * rebuild from a log — migrates as a trickle rather than one burst. Both are fractions in [0, 1];
 * zero, the default, keeps the trigger exact. Outside that range is ELYSIUMKV_CONFIG.
 *
 * `age_jitter` applies per file and only ever fires it *earlier*, because a transient tier's
 * `max_age` is an exposure bound the engine promises. The offset is derived from the file's number
 * and write time rather than rolled, so a reopen recomputes the same one. A tier's `stall_age` is
 * left exact: it is an alarm.
 *
 * `flush_interval_jitter` applies per memtable and fires either way — a late flush costs replay on
 * restart and breaks no promise. It smooths compaction queue depth. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_configure_jitter(elysiumkv_options*,
                                                     double age_jitter,
                                                     double flush_interval_jitter);

/* --- diagnostics -----------------------------------------------------------
 *
 * Nothing here is persisted; `write` is the only way the engine says anything. `event` is a stable
 * code (elysiumkv_log_event) so a binding can route and count without parsing `message`.
 *
 * Called on engine threads — flush, compaction, maintenance — synchronously and with no engine
 * lock held. A blocking sink applies backpressure to the operation that produced the line. The
 * sink must not call back into the store.
 *
 * `message` is not NUL-terminated and is valid only for the duration of the call.
 *
 * `min_level` follows elysiumkv_log_level: 0 debug, 1 info, 2 warn, 3 error, 4 off. A NULL vtable
 * disables logging, which is the default. */
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
 * `get` writes at most `len` bytes from `offset` into `out` and sets `*out_len` to what it wrote;
 * a read past the end is truncated rather than an error. `list` reports names through `emit`, once
 * per name.
 *
 * The implementation must honour ARCHITECTURE.md "Immutable named objects": ELYSIUMKV_NOT_FOUND
 * means the object is definitely absent and ELYSIUMKV_IO means the store could not determine
 * anything. Recovery draws conclusions from absence, so reporting NOT_FOUND for a failure to look
 * causes data loss. */
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

/* --- encryption at rest -----------------------------------------------------------------
 *
 * Two seams: the engine owns the cryptography, the embedder owns the key custody. A passthrough
 * provider is registered under the reserved empty id and is primary unless another is named, so an
 * unencrypted store is that provider being primary rather than a special case.
 *
 * Every callback returns ELYSIUMKV_OK or an error, and out-parameters are written only on OK. A
 * buffer too small is ELYSIUMKV_CONFIG with the required length in the *_len out-parameter.
 *
 * Key material handed to the engine is copied into engine-owned storage and the caller's buffer is
 * zeroed before the call returns. */

/* Wrapping and unwrapping. Keys are 32 bytes; anything else is a configuration error. */
typedef struct {
    void* context;
    /* A fresh data key: the plaintext in key_out, the form safe to persist in envelope_out. */
    elysiumkv_status (*new_data_key)(void* context, uint8_t* key_out, size_t key_cap,
                                     uint8_t* envelope_out, size_t envelope_cap,
                                     size_t* envelope_len);
    /* The plaintext key for an envelope this manager produced. */
    elysiumkv_status (*open_data_key)(void* context, const uint8_t* envelope, size_t envelope_len,
                                      uint8_t* key_out, size_t key_cap);
    /* Called once when the options are destroyed. May be NULL. */
    void (*destroy)(void* context);
} elysiumkv_encryption_key_manager;

/* A whole construction, for an embedder that must use a specific one.
 *
 * One vtable rather than two, so a single lifetime crosses the boundary: create and open return an
 * opaque cipher handle that the remaining calls operate on, and destroy_cipher ends it.
 *
 * chunk_bytes and overhead_bytes must be constant for a cipher's life; they are read once and
 * cached, and a varying value corrupts reads undetectably. object_id must return what create was
 * given: migration renumbers a byte-for-byte copy, so a chunk authenticated against the file's
 * current number would strand every migrated file.
 *
 * seal and open_chunk are called once per chunk on background threads. A binding that upcalls into
 * a managed runtime must attach them. */
typedef struct {
    void* context;
    elysiumkv_status (*create)(void* context, uint64_t object_id, void** cipher_out,
                               uint8_t* metadata_out, size_t metadata_cap, size_t* metadata_len);
    elysiumkv_status (*open)(void* context, uint64_t object_id, const uint8_t* metadata,
                             size_t metadata_len, void** cipher_out);
    void (*destroy_cipher)(void* context, void* cipher);

    size_t (*chunk_bytes)(void* context, void* cipher);
    size_t (*overhead_bytes)(void* context, void* cipher);
    uint64_t (*object_id)(void* context, void* cipher);

    elysiumkv_status (*seal)(void* context, void* cipher, uint64_t chunk,
                             const uint8_t* plaintext, size_t plaintext_len,
                             const uint8_t* aad, size_t aad_len,
                             uint8_t* out, size_t out_cap, size_t* out_len);
    elysiumkv_status (*open_chunk)(void* context, void* cipher, uint64_t chunk,
                                   const uint8_t* ciphertext, size_t ciphertext_len,
                                   const uint8_t* aad, size_t aad_len,
                                   uint8_t* out, size_t out_cap, size_t* out_len);
    /* Called once when the options are destroyed. May be NULL. */
    void (*destroy)(void* context);
} elysiumkv_encryption_provider;

/* Registers the built-in AES-256-GCM construction under `id`, keyed by the embedder's manager: a
 * fresh data key per object, wrapped by that manager. This is the call almost everyone wants.
 * `chunk_bytes` zero leaves the default. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_add_aes256_gcm_encryption(
    elysiumkv_options*, const char* id, const elysiumkv_encryption_key_manager*,
    size_t chunk_bytes);

/* The same construction, keyed by one master key held in this process rather than by a callback.
 *
 * A data key is still minted per object and wrapped under the master key: nonces are derived from
 * the chunk index, so one key across every object would repeat them. This is a key-custody choice,
 * not a weaker construction.
 *
 * `master_key` must be exactly 32 bytes. It is copied into storage the engine zeroes
 * deterministically; the caller's buffer is untouched and remains theirs to wipe. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_add_aes256_gcm_encryption_with_static_key(
    elysiumkv_options*, const char* id, const uint8_t* master_key, size_t master_key_len,
    size_t chunk_bytes);

/* The same construction over AWS KMS: `GenerateDataKey` per object, `Decrypt` to reopen one, so the
 * key that wraps them never enters this process.
 *
 * Absent unless built with the AWS SDK, reporting ELYSIUMKV_CONFIG and naming the build option.
 * NULL for `region`, `endpoint` or the credentials means the same as it does for the remote seams
 * below; zero for `timeout_ms` means the built-in default.
 *
 * `key_id` is what a rotation changes. A wrapped data key records which key produced it, so files
 * written under an earlier one keep opening without it being named here. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_add_aes256_gcm_encryption_with_kms(
    elysiumkv_options*, const char* id, const char* key_id, const char* region,
    const char* endpoint, const char* access_key, const char* secret_key, int64_t timeout_ms,
    size_t chunk_bytes);

/* Registers an embedder's own construction under `id`. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_add_encryption_provider(
    elysiumkv_options*, const char* id, const elysiumkv_encryption_provider*);

/* Which registered id writes new objects. Must name one that was added; an empty or NULL id means
 * the passthrough, which is the default. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_primary_encryption_provider(
    elysiumkv_options*, const char* id);

/* Rewrite files recorded under any other provider, in the background, until none are left. Off by
 * default.
 *
 * Changing the primary does not finish a rotation: existing files keep the provider they were
 * written under, and a cold one may never be compacted. This is what makes it converge.
 * `files_pending_reencryption` in the stats buffer reaches zero when it has, which is the moment
 * the retired provider may be unregistered — the manifest is re-sealed as part of it. */
ELYSIUMKV_API elysiumkv_status elysiumkv_options_set_encryption_rewrite_to_primary(
    elysiumkv_options*, int enabled);

/* --- the shared memory budget (ARCHITECTURE.md "A process-wide memory budget") ------------------
 *
 * Per process, not per instance, which is why it is a handle rather than a number on
 * elysiumkv_options: sizing multiplies by instance count when an embedder runs one per shard. One
 * budget is passed to every instance and every in-memory cache in the process.
 *
 * Exceeding it sheds in order: evict the block cache, flush memtables, stall writes. No write ever
 * fails because of it.
 *
 * Must outlive every options object, database and cache it was given to. */
ELYSIUMKV_API void* elysiumkv_memory_budget_create(size_t total_bytes);
ELYSIUMKV_API void elysiumkv_memory_budget_destroy(void*);
/* Bytes currently charged. May exceed the total — see elysiumkv_stats_snapshot. */
ELYSIUMKV_API size_t elysiumkv_memory_budget_used(const void*);

/* --- cache layers (ARCHITECTURE.md "Caches chain") ----------------------------------------------
 *
 * A cache is a store wrapping a store, so they chain: memory over disk over S3 is two calls. The
 * handle returned is a store handle like any other.
 *
 * `delegate` must outlive the cache built over it: the cache holds a reference to the store, not a
 * copy of the handle.
 *
 * A cache may never be a tier's innermost store — it holds only copies, so eviction would have
 * nothing to fall back on. elysiumkv_open reports ELYSIUMKV_CONFIG for that arrangement.
 *
 * These report a status because a bad argument is ELYSIUMKV_CONFIG while an uncreatable cache
 * directory is ELYSIUMKV_IO, and a NULL return could not distinguish them. */

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
 * sequential read costs one request per chunk rather than one per block. It needs no notion of a
 * scan: a point lookup whose neighbour is read later is served from what the first one pulled.
 *
 * Amplification is bounded by the chunk rather than the object — a small read against a large file
 * pulls one chunk. Zero fetches exactly what was asked. */
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

/* --- remote seams (ARCHITECTURE.md "The ABI boundary", ARCHITECTURE.md "Ownership is one
 * compare-and-set") ------------------------------------------------------------------------------
 *
 * S3 and DynamoDB. Absent unless built with them; ELYSIUMKV_FEATURE_AWS says which, and these
 * report ELYSIUMKV_CONFIG naming the missing build option otherwise.
 *
 * These report a status and hand back the handle, rather than returning a pointer like the local
 * constructors, because two failures must stay distinct: a bad bucket or table name is
 * ELYSIUMKV_CONFIG and retrying is pointless, while `create_table_if_missing` against an
 * unreachable DynamoDB is ELYSIUMKV_IO and retrying is correct.
 *
 * NULL for `endpoint` means the real service. NULL credentials mean the SDK's own chain —
 * environment, profile, instance role. Zero for any timeout means the built-in default.
 *
 * Handles are destroyed with elysiumkv_blob_store_destroy / elysiumkv_manifest_catalog_destroy. */

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

/* Returns the number of pins and iterators still outstanding, so a binding's tests can fail on a
 * leak. Zero means a clean close.
 *
 * Closing releases every pin and detaches every live iterator: a detached iterator yields nothing
 * further and is safe to destroy. A pin handle is only meaningful with its db, so elysiumkv_unpin
 * after close is a use-after-free.
 *
 * Closing also attempts a flush, since there is no write-ahead log. The attempt is best-effort and
 * its failure is not reported here; elysiumkv_flush is the only way to know. Use
 * elysiumkv_close_without_flush to skip it. */
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
/* Drops every key below `key` in one manifest edit rather than one tombstone per key.
 *
 * Monotone: a call at or below the current point is a no-op returning OK, so a caller need not
 * track what it already asked for. There is no un-truncate.
 *
 * Visibility changes at once; space returns over time. A file entirely below the point is unlinked
 * whole, one straddling it is narrowed by the next compaction. An open iterator holds the version
 * it started on and is unaffected. */
ELYSIUMKV_API elysiumkv_status elysiumkv_truncate_below(elysiumkv_db*, const uint8_t* key,
                                             size_t key_len);

/* Deletes every key in [lower, upper) — the counterpart to elysiumkv_truncate_below for a range
 * that is not a prefix of the keyspace.
 *
 * Lower is included and upper is not, the convention the iterators use. An empty or inverted range
 * deletes nothing and returns OK.
 *
 * Not permanent, unlike a truncation point: the range may be written again immediately and a later
 * write wins. Not as cheap either — this writes a tombstone that reads in the range consult until
 * compaction resolves it. Space returns as covered files are rewritten, or at once for a file the
 * range covers whole. */
ELYSIUMKV_API elysiumkv_status elysiumkv_delete_range(elysiumkv_db*, const uint8_t* lower,
                                           size_t lower_len, const uint8_t* upper,
                                           size_t upper_len);

/* Whether a `delete_range(lower, upper)` has finished travelling through the tree: no file at any
 * level still holds data in the band. Writes 1 or 0 to `erased`. Costs no reads — every file's key
 * range is already in the manifest.
 *
 * Conservative, because a recorded range is a hull and a file can overlap the band while holding no
 * key in it. 1 means every file that could have held one is gone; 0 carries no information. A band
 * hidden by a truncation point is not erased: the objects remain until the reclaim collects them.
 *
 * Answers about files, so a write into the band after the deletion is a new write and sits in the
 * memtable until flushed. Available on a read-only handle. */
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

/* The same two scans, descending: elysiumkv_iter_next still advances, towards smaller keys. The
 * first call yields the largest key in range.
 *
 * Bounds keep their forward meaning — `lo` inclusive, `hi` exclusive — so both directions describe
 * the same set and only the order differs. */
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
/* Advances up to `cap` bytes' worth of entries in one call, packing them into `buf` as
 * `u32 key_len | key | u32 value_len | value`, little-endian, repeated `*out_count` times over
 * `*out_bytes` bytes.
 *
 * Amortises the boundary crossings a scan otherwise pays three times per entry, which dominate its
 * cost from a managed runtime (ARCHITECTURE.md "The ABI boundary"). It trades zero-copy for that:
 * the bytes are copied into the caller's buffer rather than borrowed from the block, which is the
 * right trade for a scan and the wrong one for a point lookup — elysiumkv_get still pins.
 *
 * `*out_count == 0` with ELYSIUMKV_OK means the iteration is exhausted, unless `*out_bytes` is
 * non-zero, which means the next entry needs a buffer that large. Check elysiumkv_iter_status
 * afterwards, as with next(). */
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_next_batch(elysiumkv_iter*, uint8_t* buf, size_t cap,
                                                   size_t* out_count, size_t* out_bytes);

/* Check after next() returns false: exhaustion and failure look alike otherwise. */
ELYSIUMKV_API elysiumkv_status elysiumkv_iter_status(elysiumkv_iter*);
ELYSIUMKV_API void elysiumkv_iter_destroy(elysiumkv_iter*);

/* --- statistics -------------------------------------------------------------
 *
 * One call for the whole aggregate, serialized into a caller buffer
 * (ARCHITECTURE.md "Statistics are a buffer, not a struct"). A snapshot assembled from per-field
 * accessors would be torn, each call observing a different instant. Read in one call the
 * cross-field relationships hold: every file sits in exactly one level and exactly one tier, so
 * summing `bytes` over levels and over tiers gives the same total.
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
 *     u64 memtable_entries, memtable_tombstones,
 *         background_failures                          offset 216
 *     u64 compactions_trimmed                          offset 240
 *     u64 reencryptions, files_pending_reencryption    offset 248
 *                                                      header_bytes = 264
 *
 * `watermark_present` exists because zero is a valid watermark — a store at the start of its log —
 * so the value alone cannot express absence. An exporter omits the series when the flag is zero.
 *   level record, level_count of them
 *     i32 level, i32 file_count, u64 bytes, u64 oldest_file_age_ms,
 *     i32 files_stale_codec, u8 age_triggered, u8 stalling, u8 reserved[2]
 *   tier record, tier_count of them
 *     i32 tier, i32 file_count, u64 bytes, u64 oldest_file_age_ms,
 *     i32 files_pending_migration, u8 stalling, u8 reserved[3]
 *
 * Decode by the declared sizes, not by sizeof: a reader that starts records at `header_bytes`,
 * steps by `*_record_bytes` and ignores trailing bytes it does not recognise survives a field
 * being appended.
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
 * Records that every write completed so far is at or before `position` in whatever log the embedder
 * replays. The engine orders it, carries it with the data and hands it back at the next open; it
 * never interprets one.
 *
 * A position, not a time, and unrelated to a tier's max_age. Positions must be non-decreasing; a
 * decreasing one returns ELYSIUMKV_CONFIG rather than being clamped, since clamping would hide a
 * replay that went backwards.
 *
 * One store under the lock the write path already takes: it forces no flush and writes no manifest,
 * so it may be called as often as the embedder commits. The value becomes durable when the memtable
 * holding it is flushed, which elysiumkv_flush does immediately. */
/* --- read-only -------------------------------------------------------------
 *
 * Opens without taking ownership: no manifest write, no background threads, no reclamation, no
 * compare-and-set. Any number may be open at once alongside a writer, with no registration.
 *
 * Refuses a store with no manifest (ELYSIUMKV_NOT_FOUND) rather than creating one, and refuses one
 * whose Transient tier has lost files: repairing that is a manifest write, and serving a version
 * with holes would present stale values as current.
 *
 * Unlike the C++ API there is one handle type, so the write entry points — put, remove, write,
 * flush, compact_level, set_watermark — return ELYSIUMKV_CONFIG on a read-only handle.
 *
 * The writer must set obsolete_retention_ms for this to be safe: its collector cannot see a reader
 * in another process, so that delay is the only thing between a compaction there and a vanished
 * file here. A reader that falls behind the window is told ELYSIUMKV_STALE, never
 * ELYSIUMKV_CORRUPT. */
ELYSIUMKV_API elysiumkv_status elysiumkv_open_read_only(const elysiumkv_options*, elysiumkv_db** out);

/* Re-reads the manifest and installs the newest version. Explicit, never
 * automatic: two reads in one logical operation must be able to see one version.
 * Open iterators are unaffected — an iterator holds the version it started on.
 * A no-op returning ELYSIUMKV_OK on a writable handle. */
ELYSIUMKV_API elysiumkv_status elysiumkv_refresh(elysiumkv_db*);

ELYSIUMKV_API elysiumkv_status elysiumkv_set_watermark(elysiumkv_db*, uint64_t position);

/* The last position whose effect on the store is known to have survived, as established at open.
 * Replaying only the positions after it yields the same logical state as replaying the whole log —
 * exclusive, so `80` means resume at `81`.
 *
 * `*present` is 0 when nothing can be certified — no watermark was ever set, or a lost transient
 * store held data predating the first one — and the embedder should replay from the beginning.
 * Distinct from a watermark of zero. `*out` is untouched when `*present` is 0.
 *
 * A restore must use this value rather than the stats buffer's `durable_watermark`, which is the
 * live frontier for observation and may have been rounded through a double by a metrics pipeline.
 */
ELYSIUMKV_API elysiumkv_status elysiumkv_watermark(elysiumkv_db*, uint64_t* out, bool* present);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* ELYSIUMKV_H */
