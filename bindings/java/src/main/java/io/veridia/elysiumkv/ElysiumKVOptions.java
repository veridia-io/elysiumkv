package io.veridia.elysiumkv;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration, built fluently and applied at {@link ElysiumKV#open}.
 *
 * <p>Tiers and levels reach the engine as they are declared, because each is a
 * complete aggregate on its own. Everything else is held here and sent in a
 * <em>single</em> call, mirroring the rule: a half-configured aggregate should
 * not be representable, and nine separate setters made it so.
 *
 * <p>Two axes, and they are independent (ARCHITECTURE.md "A tier is not a level"). A <b>level</b> is LSM structure:
 * overlap, capacity, compression. A <b>tier</b> is storage: which store holds a
 * file, chosen per file by age and size. One level routinely spans several
 * tiers.
 */
public final class ElysiumKVOptions implements AutoCloseable {
    private long handle;
    private final List<Runnable> pending = new ArrayList<>();

    private long memtableBytes;
    private long flushIntervalMs;
    private long maintenanceIntervalMs;
    private long blockBytes;
    private long blockCacheBytes;
    private long readerCacheBytes;
    private int bloomBitsPerKey;
    private long maxCompactionBytes;
    private int manifestEditsPerGeneration;
    private int paranoidChecks = -1;   // tri-state: negative keeps the engine default
    private int blockOnStall = -1;
    private int reclaimOrphansAtOpen = -1;
    private long catalogHandle;
    private long budgetHandle;

    public ElysiumKVOptions() {
        Native.ensureLoaded();
        handle = Native.optionsCreate();
    }

    /**
     * Delete unreferenced objects at open, reclaiming the residue of a flush or compaction that
     * died before its manifest edit was durable. <b>Off by default.</b>
     *
     * <p>Turning it on asserts something the engine cannot check: that no other process has
     * this store open. Open takes no lock and performs no compare-and-set, so an unreferenced
     * object is indistinguishable from a concurrent writer's in-flight file — or from one whose
     * edit became durable between the moment the manifest was read and the moment the store was
     * listed. Deleting it destroys committed data, silently.
     *
     * <p>The engine does not need it: a stale file number is stepped over at open rather than
     * reclaimed. Leaving it off costs storage and nothing else, and on S3 a lifecycle expiry
     * rule on the prefix is the other answer.
     */
    public ElysiumKVOptions reclaimOrphansAtOpen(boolean reclaim) {
        this.reclaimOrphansAtOpen = reclaim ? 1 : 0;
        return this;
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
     * must be durable and must bound neither age nor file size — {@link
     * ElysiumKV#open} rejects a configuration that breaks either rule rather than
     * documenting it as a precondition.
     */
    public ElysiumKVOptions addTier(BlobStore store, Durability durability, long maxAgeMs,
                                  long maxFileBytes, long maxBytes, long stallAgeMs) {
        Native.optionsAddTier(handle(), store.handle(), durability.code(), maxAgeMs, maxFileBytes,
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
     * How often the maintenance coordinator reconciles: it evaluates every background policy —
     * flush, compaction, migration off a transient tier, capacity eviction, obsolete-object
     * collection — against current state and the clock, and dispatches what is due. Zero (the
     * default) leaves the engine default of one second.
     *
     * <p>It exists because <strong>a policy driven by time needs a trigger that is not a
     * write</strong>. Without it a store that goes quiet with a file on a transient tier leaves it
     * there indefinitely, however the tiers are configured.
     *
     * <p>Not a latency knob. The interval is the smallest term in the exposure window
     * {@code maxAge + interval + queueing behind an in-flight compaction + the migration itself},
     * so shortening it buys very little; an idle tick performs no version scan, which is what
     * makes the default affordable across many partition stores in one process.
     */
    public ElysiumKVOptions maintenanceIntervalMs(long millis) {
        maintenanceIntervalMs = millis;
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

    boolean checked() {
        return paranoidChecks > 0;
    }

    /** Flushes the scalars in one call and hands back the native handle. */
    long prepare() {
        Native.optionsConfigure(handle(), catalogHandle, budgetHandle, memtableBytes, blockBytes,
                                blockCacheBytes,
                                readerCacheBytes, bloomBitsPerKey, maxCompactionBytes,
                                manifestEditsPerGeneration, paranoidChecks, blockOnStall,
                                reclaimOrphansAtOpen, flushIntervalMs, maintenanceIntervalMs);
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
