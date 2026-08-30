package io.veridia.elysiumkv;

/**
 * Configuration, built fluently and applied at {@link ElysiumKV#open}.
 *
 * <p>Tiers and levels reach the engine as they are declared, each being a complete aggregate on
 * its own. Everything else is held here and sent in a single call, so a half-configured aggregate
 * is not representable.
 *
 * <p>The two axes are independent (ARCHITECTURE.md "A tier is not a level"). A level is LSM
 * structure: overlap, capacity, compression. A tier is storage: which store holds a file, chosen
 * per file by age and size. One level routinely spans several tiers.
 */
public final class ElysiumKVOptions implements AutoCloseable {
    private long handle;

    private long memtableBytes;
    private long flushIntervalMs;
    private long maintenanceIntervalMs;
    private long obsoleteRetentionMs;
    private long orphanRetentionMs;
    private long orphanSweepIntervalMs;
    private double ageJitter;
    private double flushIntervalJitter;
    private double tombstoneDensityTrigger;
    private LoggerBridge loggerBridge;
    private int minLogLevel = ElysiumKVLogger.Level.INFO.ordinal();
    private long tombstoneDensityMinEntries;
    private long blockBytes;
    private long blockCacheBytes;
    private long readerCacheBytes;
    private int bloomBitsPerKey;
    private long maxCompactionBytes;
    private long compactionWindowBytes;
    /// Tri-state like its neighbours: -1 leaves the engine's default, 0 refuses, 1 serves.
    private int allowReadsBeforeRecovery = -1;
    private int manifestEditsPerGeneration;
    private int paranoidChecks = -1;   // tri-state: negative keeps the engine default
    private int blockOnStall = -1;
    private long catalogHandle;
    private long budgetHandle;

    public ElysiumKVOptions() {
        Native.ensureLoaded();
        handle = Native.optionsCreate();
    }

    /**
     * The process-wide memory budget (ARCHITECTURE.md "A process-wide memory budget"), shared by every instance in the process.
     * Without one, each instance is bounded only by its own settings — which for an
     * embedder running one instance per shard means the real ceiling is the shard count
     * times those settings.
     */
    public ElysiumKVOptions memoryBudget(MemoryBudget budget) {
        budgetHandle = budget.handle();
        return this;
    }

    public ElysiumKVOptions manifestCatalog(ManifestCatalog catalog) {
        catalogHandle = catalog.handle();
        return this;
    }

    /**
     * Appends a tier, hot to cold. Zero means "no bound". ARCHITECTURE.md "A tier is not a level" — the last tier
     * must be durable and must not bound age — {@link ElysiumKV#open} rejects a
     * configuration that breaks either rule rather than documenting it as a
     * precondition.
     *
     * <p>{@code maxBytes} is the <em>tier's</em> capacity, evicted oldest-first. There is no
     * per-file size bound, because placement must be monotone in age alone: size would give an
     * independent route to a colder tier. To keep large files off a fast tier, lower that level's
     * {@code targetFileBytes}.
     */
    public ElysiumKVOptions addTier(BlobStore store, Durability durability, long maxAgeMs,
                                  long maxBytes, long stallAgeMs) {
        Native.optionsAddTier(handle(), store.handle(), durability.code(), maxAgeMs,
                              maxBytes, stallAgeMs);
        return this;
    }

    /** LSM structure only — no storage decisions. Levels may skip numbers. */
    public ElysiumKVOptions level(int level, Compression compression, long maxBytes, int maxFiles,
                               int slowdownAt, int stopAt, long targetFileBytes) {
        Native.optionsSetLevel(handle(), level, compression.code(), maxBytes, maxFiles, slowdownAt,
                               stopAt, targetFileBytes);
        return this;
    }

    /**
     * Flush the memtable once it has been open this long, even if it never reaches {@link
     * #memtableBytes}. Size and age are alternatives — whichever comes first flushes. Zero (the
     * default) leaves it unset, so size alone decides.
     *
     * <p>This is the only bound on how long a write stays in memory. A tier's age bound acts on
     * files, and an unflushed memtable is not one, so under a trickle of writes that never fills
     * a memtable the data is held indefinitely whatever the tiers say. Costs write amplification:
     * a short interval on a quiet store produces small L0 files that compaction must merge away.
     */
    public ElysiumKVOptions flushIntervalMs(long millis) {
        flushIntervalMs = millis;
        return this;
    }

    /**
     * How long data lives before the engine drops it, measured from when it was written. Zero — the
     * default — never expires anything.
     *
     * <p>Expiry by manifest edit: a file whose every write has outlived this is unlinked whole,
     * nothing read and nothing rewritten. That is cheap, and it is why the three limits below are
     * part of what the option means rather than caveats on it.
     *
     * <p><b>The granularity is the file, not the key.</b> The manifest names files, so reaching
     * inside one means rewriting it. This buys "data older than this disappears", not "this key
     * expires at this time" — a key written a second before its file's newest write outlives the
     * limit by as long as that file survives.
     *
     * <p>At or after, never before: a file is dropped when the sweep next finds it expired, so data
     * may outlive the limit by up to one {@link #orphanSweepIntervalMs(long)}. And a file expires only once
     * no older file overlaps its range, because dropping one that shadows an older version of the
     * same key would uncover that version rather than remove the key.
     */
    public ElysiumKVOptions ttlMs(long millis) {
        if (millis < 0) throw new IllegalArgumentException("ttl must not be negative: " + millis);
        Native.optionsSetTtl(handle(), millis);
        return this;
    }

    /**
     * How often the maintenance coordinator reconciles: it evaluates every background policy —
     * flush, compaction, migration off a transient tier, capacity eviction, obsolete-object
     * collection — against current state and the clock, and dispatches what is due. Zero (the
     * default) leaves the engine default of one second.
     *
     * <p>A policy driven by time needs a trigger that is not a write: without this, a store that
     * goes quiet with a file on a transient tier leaves it there indefinitely.
     *
     * <p>Not a latency knob — the interval is the smallest term in the exposure window
     * {@code maxAge + interval + queueing behind an in-flight compaction + the migration itself}.
     * An idle tick performs no version scan, which is what makes the default affordable across
     * many partition stores in one process.
     */
    public ElysiumKVOptions maintenanceIntervalMs(long millis) {
        maintenanceIntervalMs = millis;
        return this;
    }

    /**
     * How long an object this instance superseded is kept after nothing local references it.
     *
     * <p>Protects readers, and only readers. A {@link ReadOnlyStore} in another process holds
     * a version this one has already replaced, and the collector cannot see it — liveness is tracked
     * per process. This delay is the only thing between a compaction here and a vanished file there.
     * Set it comfortably above how often your readers call {@link ReadOnlyStore#refresh()}.
     *
     * <p>Zero — the default — deletes immediately, which is correct when nothing else has the store
     * open.
     */
    public ElysiumKVOptions obsoleteRetentionMs(long millis) {
        obsoleteRetentionMs = millis;
        return this;
    }

    /**
     * How long an object must be <em>continuously observed</em> unreferenced before the sweep
     * deletes it.
     *
     * <p>Protects a concurrently-writing process, and is needed whether or not readers exist:
     * an object unreferenced at the instant we happen to look is indistinguishable from another
     * writer's file whose edit committed a moment ago. Zero leaves the engine default of 24 hours.
     * Must be at least {@link #obsoleteRetentionMs}, because a crash empties the pending queue and a
     * superseded object comes back as an orphan protected by this window alone.
     */
    public ElysiumKVOptions orphanRetentionMs(long millis) {
        orphanRetentionMs = millis;
        return this;
    }

    /**
     * How often to list the stores looking for orphans. Zero disables the sweep, which costs storage
     * and nothing else — correctness never depends on reclamation happening.
     */
    public ElysiumKVOptions orphanSweepIntervalMs(long millis) {
        orphanSweepIntervalMs = millis;
        return this;
    }

    public ElysiumKVOptions memtableBytes(long bytes) {
        memtableBytes = bytes;
        return this;
    }

    public ElysiumKVOptions blockBytes(long bytes) {
        blockBytes = bytes;
        return this;
    }

    public ElysiumKVOptions blockCacheBytes(long bytes) {
        blockCacheBytes = bytes;
        return this;
    }

    /**
     * Bytes of open-SST-reader state — each file's index block and bloom filter —
     * kept resident, least-recently-used first. Zero keeps the engine default.
     *
     * <p>The filter is what makes this matter: at 10 bits per key it is ~1.25 MB for a
     * million-entry file. Size it generously — evicting a reader costs three reads to
     * reopen the file, which against a remote tier is three round trips, so a reader
     * cache too small for the working set is a worse deal than the memory it saves.
     */
    public ElysiumKVOptions readerCacheBytes(long bytes) {
        this.readerCacheBytes = bytes;
        return this;
    }

    public ElysiumKVOptions bloomBitsPerKey(int bits) {
        bloomBitsPerKey = bits;
        return this;
    }

    public ElysiumKVOptions maxCompactionBytes(long bytes) {
        maxCompactionBytes = bytes;
        return this;
    }

    /**
     * How much of a compaction input is read at a time; zero leaves the default of 2 MiB.
     *
     * <p>Total requests are {@code input bytes / this}, which against object storage is what a
     * compaction costs — measured at 20&nbsp;ms of injected latency, raising it from 2&nbsp;MiB to
     * 8&nbsp;MiB cut a compaction's requests by 63% and its duration by a third.
     *
     * <p>Traded directly against memory. A merge interleaves its inputs, so every input's
     * window is live at once, and each input holds two — the one being merged and the one being
     * fetched ahead of it. The footprint is {@code 2 x this x inputs x concurrent compactions}, and
     * it is charged to the memory budget when one is set.
     */
    public ElysiumKVOptions compactionWindowBytes(long bytes) {
        // Checked here, unlike its neighbours on this call, because a negative is not merely
        // ignored downstream. It crosses as an unsigned size and arrives as SIZE_MAX, which the C
        // ABI reads as a deliberate setting and which then overflows the budget charge — a store
        // that opened and quietly tried to buffer everything.
        if (bytes < 0) {
            throw new IllegalArgumentException("compactionWindowBytes must not be negative: " + bytes);
        }
        compactionWindowBytes = bytes;
        return this;
    }

    /**
     * Serve reads while a discarded transient store is still unreplayed. Off unless set.
     *
     * <p>What survives a discard is wrong rather than merely incomplete — a key whose newer
     * value lived on the lost store now reads as its older one — so reads fail with
     * {@link RecoveryRequiredException} until {@link ElysiumKV#markRecoveryComplete()}. Writes are
     * never refused either way, because the replay is made of them.
     *
     * <p>Turn it on for a replay that also reads, which is the shape a changelog consumer usually
     * has, and accept in doing so that those reads may be behind.
     */
    public ElysiumKVOptions allowReadsBeforeRecovery(boolean allow) {
        allowReadsBeforeRecovery = allow ? 1 : 0;
        return this;
    }

    /**
     * Registers the built-in AES-256-GCM envelope construction under {@code id}, keyed by your
     * manager, and makes it the one new objects are written with.
     *
     * <p>The id is persisted, written into every object this provider encrypts. Renaming it
     * orphans every file recorded against the old name. It may not be empty: that one is reserved
     * for the passthrough the engine always registers, which is why an unconfigured store is not a
     * special case but simply that provider being primary.
     *
     * <p>{@code chunkBytes} of zero leaves the default of 4096.
     */
    public ElysiumKVOptions encryptWith(String id, EncryptionKeyManager keys, long chunkBytes) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("the empty provider id is reserved for the passthrough");
        }
        if (keys == null) throw new IllegalArgumentException("a key manager is required");
        if (chunkBytes < 0) {
            throw new IllegalArgumentException("chunkBytes must not be negative: " + chunkBytes);
        }
        Native.optionsAddAes256GcmEncryption(handle(), id, keys, chunkBytes);
        Native.optionsSetPrimaryEncryptionProvider(handle(), id);
        return this;
    }

    /**
     * Registers a provider for reading without making it primary — the shape a migration to a new
     * construction takes: the new one is primary, the old one stays registered until no file
     * records it.
     */
    public ElysiumKVOptions alsoDecryptWith(String id, EncryptionKeyManager keys, long chunkBytes) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("the empty provider id is reserved for the passthrough");
        }
        if (keys == null) throw new IllegalArgumentException("a key manager is required");
        Native.optionsAddAes256GcmEncryption(handle(), id, keys, chunkBytes);
        return this;
    }

    /**
     * The same, keyed by a manager the engine implements rather than one of yours.
     *
     * <p>See {@link BuiltinEncryptionKeyManager} for why this is the shape to prefer when it fits:
     * nothing crosses back into the JVM per object, and the envelope layout comes from a single
     * implementation.
     */
    public ElysiumKVOptions encryptWith(String id, BuiltinEncryptionKeyManager keys,
                                        long chunkBytes) {
        register(id, keys, chunkBytes);
        Native.optionsSetPrimaryEncryptionProvider(handle(), id);
        return this;
    }

    /** Registers a built-in manager for reading without making it primary. */
    public ElysiumKVOptions alsoDecryptWith(String id, BuiltinEncryptionKeyManager keys,
                                            long chunkBytes) {
        register(id, keys, chunkBytes);
        return this;
    }

    private void register(String id, BuiltinEncryptionKeyManager keys, long chunkBytes) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("the empty provider id is reserved for the passthrough");
        }
        if (keys == null) throw new IllegalArgumentException("a key manager is required");
        if (chunkBytes < 0) {
            throw new IllegalArgumentException("chunkBytes must not be negative: " + chunkBytes);
        }
        keys.register(handle(), id, chunkBytes);
    }

    /**
     * Rewrite files recorded under any other provider, in the background, until none are left.
     * Off by default.
     *
     * <p>Changing the primary does not finish a rotation. Every file already written keeps
     * the provider it was written under, and reads keep routing to it; compaction rewrites such a
     * file only when it happens to compact it, which for a cold file may be never — and that is
     * exactly the file a key rotation was performed to stop depending on.
     *
     * <p>{@link ElysiumKVStats#filesPendingReencryption()} reaches zero when the rotation has
     * converged, which is the moment the retired provider may be unregistered. The manifest is
     * re-sealed as part of it, so the store then opens without that provider registered at all.
     */
    public ElysiumKVOptions rewriteToPrimaryEncryptionProvider(boolean enabled) {
        Native.optionsSetEncryptionRewriteToPrimary(handle(), enabled);
        return this;
    }

    /**
     * Replaces the level map with the classic geometric layout: L0 bounded by file count, each
     * deeper level {@code multiplier} times the capacity of the one above it, and the last
     * carrying none because it absorbs everything.
     *
     * <p>Choose {@code count} against expected total size: more levels means lower write
     * amplification, and configured levels sitting empty cost nothing.
     *
     * <p>Replaces rather than merges, so call it before any {@link #level} of your own.
     */
    public ElysiumKVOptions geometricLevels(long base, int multiplier, int count) {
        if (base <= 0) throw new IllegalArgumentException("base must be positive: " + base);
        if (multiplier < 2) {
            throw new IllegalArgumentException("multiplier must be at least 2: " + multiplier);
        }
        if (count < 2) throw new IllegalArgumentException("count must be at least 2: " + count);
        Native.optionsSetGeometricLevels(handle(), base, multiplier, count);
        return this;
    }

    public ElysiumKVOptions manifestEditsPerGeneration(int edits) {
        manifestEditsPerGeneration = edits;
        return this;
    }

    /**
     * Turns on the engine's invariant checks <em>and</em> this binding's: pins
     * and iterators then refuse use after close and from another thread. Both
     * are debugging aids, so they travel together.
     */
    public ElysiumKVOptions paranoidChecks(boolean enabled) {
        paranoidChecks = enabled ? 1 : 0;
        return this;
    }

    /**
     * When false, a write that would stall returns rather than blocking — the
     * caller gets a {@link RetryableException}. The stall valve itself cannot be
     * turned off (ARCHITECTURE.md "Migration between tiers").
     */
    public ElysiumKVOptions blockOnStall(boolean enabled) {
        blockOnStall = enabled ? 1 : 0;
        return this;
    }

    /**
     * Where the engine reports what it is doing. Null, the default, means no logging and no
     * message is formatted.
     *
     * <p>The sink runs on engine threads and the operation that produced the line waits for it, so
     * give it an async appender rather than doing work in it. See {@link ElysiumKVLogger}.
     */
    public ElysiumKVOptions logger(ElysiumKVLogger sink, ElysiumKVLogger.Level minLevel) {
        loggerBridge = sink == null ? null : new LoggerBridge(sink);
        minLogLevel = minLevel == null ? ElysiumKVLogger.Level.INFO.ordinal() : minLevel.ordinal();
        return this;
    }

    boolean checked() {
        return paranoidChecks > 0;
    }

    /**
     * Compacts a file once this fraction of its entries are tombstones. Zero, the default, is off.
     *
     * <p>The trigger the size ratios cannot express. A tombstone shadows older copies of its
     * key and can only be dropped once a compaction reaches the bottommost level for its range, so
     * a delete-heavy store whose levels stay inside their byte and file budgets never trips one —
     * and every scan over the deleted region goes on paying to skip them. That shows up as scans
     * getting slower, which is the hardest kind of regression to attribute.
     *
     * <p>A store that deletes in bulk usually wants {@link ElysiumKV#truncateBelow} instead, which
     * reclaims without rewriting anything.
     */
    public ElysiumKVOptions tombstoneDensityTrigger(double fraction) {
        tombstoneDensityTrigger = fraction;
        return this;
    }

    /**
     * Entries a file needs before its density counts. Zero leaves the engine default of 1024.
     *
     * <p>Without a floor, a file of two entries with one tombstone scores 0.5 and fires a compaction
     * that rewrites almost nothing — then fires again on its own output.
     */
    public ElysiumKVOptions tombstoneDensityMinEntries(long entries) {
        tombstoneDensityMinEntries = entries;
        return this;
    }

    /**
     * Spreads each file's tier {@code maxAgeMs} crossing across {@code [maxAge * (1 - j), maxAge]}.
     * Zero, the default, keeps it exact; outside {@code [0, 1]} is a config error at open.
     *
     * <p>Earlier only. A transient tier's age bound is an exposure window the engine
     * promises, so a file may cross early but never late.
     *
     * <p>For the case where stores do not drift apart on their own: a rebuild stamps everything it
     * replays within the same few minutes, so the whole store crosses together and migrates as one
     * burst — every rebalance, for a store rebuilt on partition assignment. The offset is derived
     * from the file rather than rolled, so a reopen recomputes it instead of re-clustering what it
     * just spread.
     */
    public ElysiumKVOptions ageJitter(double fraction) {
        ageJitter = fraction;
        return this;
    }

    /**
     * Spreads {@link #flushIntervalMs} across {@code [interval * (1 - j), interval * (1 + j)]}, per
     * memtable. Zero, the default, keeps it exact.
     *
     * <p>Both directions, unlike {@link #ageJitter}: a late flush costs replay on restart and
     * breaks no promise. What it smooths is compaction queue depth — instances opened together
     * flush together, and their L0 files reach the compactor as one wave.
     */
    public ElysiumKVOptions flushIntervalJitter(double fraction) {
        flushIntervalJitter = fraction;
        return this;
    }

    /** Flushes the scalars in one call and hands back the native handle. */
    long prepare() {
        Native.optionsConfigure(handle(), catalogHandle, budgetHandle, memtableBytes, blockBytes,
                                blockCacheBytes,
                                readerCacheBytes, bloomBitsPerKey, maxCompactionBytes,
                                compactionWindowBytes,
                                manifestEditsPerGeneration, paranoidChecks, blockOnStall,
                                allowReadsBeforeRecovery,
                                flushIntervalMs, maintenanceIntervalMs, obsoleteRetentionMs,
                                orphanRetentionMs, orphanSweepIntervalMs);
        // A second call rather than more positions on the first: the C ABI keeps these apart so
        // that adding a knob does not break every existing caller of the other.
        Native.optionsConfigureJitter(handle(), ageJitter, flushIntervalJitter);
        Native.optionsConfigureCompaction(handle(), tombstoneDensityTrigger,
                                          tombstoneDensityMinEntries);
        Native.optionsSetLogger(handle(), loggerBridge, minLogLevel);
        return handle();
    }

    @Override
    public void close() {
        if (handle == 0) return;
        long h = handle;
        handle = 0;
        Native.optionsDestroy(h);
    }

    private long handle() {
        if (handle == 0) throw new IllegalStateException("options are closed");
        return handle;
    }
}
