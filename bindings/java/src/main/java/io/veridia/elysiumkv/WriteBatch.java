package io.veridia.elysiumkv;

/**
 * Puts and deletes applied as a unit by {@link ElysiumKV#write(WriteBatch)}. The
 * batch is ordered: a delete after a put of the same key wins, as it would if
 * they were separate calls.
 */
public final class WriteBatch implements AutoCloseable {
    private long handle;

    public WriteBatch() {
        Native.ensureLoaded();
        handle = Native.batchCreate();
    }

    /** Same size limits as {@link ElysiumKV#put}, checked when the batch is written. */
    public WriteBatch put(byte[] key, byte[] value) {
        Native.batchPut(handle(), key, key.length, value, value.length);
        return this;
    }

    public WriteBatch delete(byte[] key) {
        Native.batchDelete(handle(), key, key.length);
        return this;
    }

    /**
     * Deletes {@code [lower, upper)} as part of this batch.
     *
     * <p>Order within the batch decides what survives. A put after this one lands on top of
     * the range and lives; a put before it is covered. That is what makes "evict a tenant and
     * re-seed the space" a single atomic step rather than two calls with the range visibly empty
     * in between.
     */
    public WriteBatch deleteRange(byte[] lower, byte[] upper) {
        Native.batchDeleteRange(handle(), lower, upper);
        return this;
    }

    /** Number of operations, not bytes. */
    public long size() {
        return Native.batchSize(handle());
    }

    @Override
    public void close() {
        if (handle == 0) return;
        long h = handle;
        handle = 0;
        Native.batchDestroy(h);
    }

    long handle() {
        if (handle == 0) throw new IllegalStateException("batch is closed");
        return handle;
    }
}
