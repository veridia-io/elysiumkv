package io.veridia.elysiumkv;

/**
 * The shared memory budget (ARCHITECTURE.md "A process-wide memory budget") — <b>per process, not per instance</b>.
 *
 * <p>That distinction is the whole reason this is an object you create and pass around
 * rather than a number on {@link ElysiumKVOptions}. Many embedders run one instance per
 * shard, partition or tenant, so memtable and cache sizing multiplies by the instance
 * count; a per-instance constant is the wrong unit. Create one and give it to every
 * database and every in-memory cache in the process.
 *
 * <pre>{@code
 * MemoryBudget budget = new MemoryBudget(4L << 30);        // 4 GiB for this process
 * for (Shard shard : shards) {
 *     options.memoryBudget(budget).memtableBytes(64 << 20);
 * }
 * }</pre>
 *
 * <p>When the budget is exceeded the engine sheds, in this order: evict the block cache,
 * flush memtables, then stall writes. <b>No write ever fails because of it</b> —
 * refusing a put because a different instance is using memory would be a surprising
 * failure mode, so the budget shapes behaviour rather than rejecting work. {@link
 * ElysiumKVStats#budgetSheds()} counts how often shedding has happened, which is the
 * number that tells you the budget is set too low for the instances sharing it.
 *
 * <p><b>Must outlive every options object, database and cache it was given to.</b>
 */
public final class MemoryBudget implements AutoCloseable {
    private long handle;

    public MemoryBudget(long totalBytes) {
        Native.ensureLoaded();
        handle = Native.memoryBudgetCreate(totalBytes);
    }

    /**
     * Bytes currently charged: memtable arenas, the block cache, open SST readers and
     * any in-memory blob cache. May exceed the total — a memtable arena charges
     * unconditionally for a write already accepted, and that overage is exactly what the
     * write path sheds on.
     */
    public long used() {
        return Native.memoryBudgetUsed(handle());
    }

    @Override
    public void close() {
        if (handle == 0) return;
        long h = handle;
        handle = 0;
        Native.memoryBudgetDestroy(h);
    }

    long handle() {
        if (handle == 0) throw new IllegalStateException("memory budget is closed");
        return handle;
    }
}
