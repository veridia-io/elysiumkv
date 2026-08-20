package io.veridia.elysiumkv;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;

/**
 * A scan that crosses the JNI boundary once per batch rather than three times
 * per entry. Use this for long scans; use {@link ElysiumKVIterator} when you
 * want the value without copying it.
 *
 * <pre>{@code
 * try (BatchedIterator scan = db.batchedPrefixIterator(prefix)) {
 *     byte[] key = new byte[64], value = new byte[256];
 *     while (scan.next()) {
 *         int keyLength = scan.keyInto(key);
 *         int valueLength = scan.valueInto(value);
 *         ...
 *     }
 * }
 * }</pre>
 *
 * <p>The trade is copies for crossings: measured over a 1000-key prefix scan, ~58ns per entry
 * here against ~419ns through {@link ElysiumKVIterator#key()}/{@code value()}, where most of the
 * cost is the two accessor crossings rather than the advance.
 *
 * <p>The buffer is direct and the native side fills it in place, so nothing is staged between the
 * engine and the decode below. Entries are copied rather than borrowed from the block cache, which
 * is right for a scan and wrong for a point lookup; {@link ElysiumKV#get} still pins.
 */
public final class BatchedIterator implements AutoCloseable {
    /**
     * Big enough that an ordinary entry batches heavily and a maximal one still
     * fits: ARCHITECTURE.md "Inside an SST" caps a value at 1 MiB and a key at 64 KiB. A buffer that could
     * not hold one entry would degrade to a crossing per entry plus a resize.
     */
    private static final int MAX_CAPACITY = (1 << 20) + (64 << 10) + 1024;

    private final ElysiumKV owner;
    private final Thread creator;   // null unless checked
    private ByteBuffer buffer = direct(64 * 1024);
    private long handle;

    private int remaining;      // entries left in the current batch
    private int cursor;         // read position within it
    private boolean positioned;
    private int keyOffset;
    private int keyLength;
    private int valueOffset;
    private int valueLength;

    BatchedIterator(ElysiumKV owner, long handle, boolean checked) {
        this.owner = owner;
        this.handle = handle;
        this.creator = checked ? Thread.currentThread() : null;
    }

    private static ByteBuffer direct(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    /** Advances, refilling from the engine only when the batch drains. */
    public boolean next() {
        check();
        if (remaining == 0 && !refill()) {
            positioned = false;
            return false;
        }
        keyLength = buffer.getInt(cursor);
        keyOffset = cursor + 4;
        valueLength = buffer.getInt(keyOffset + keyLength);
        valueOffset = keyOffset + keyLength + 4;
        cursor = valueOffset + valueLength;
        --remaining;
        positioned = true;
        return true;
    }

    private boolean refill() {
        while (true) {
            int result = Native.iterNextBatch(handle, buffer, buffer.capacity());
            if (result > 0) {
                remaining = result;
                cursor = 0;
                return true;
            }
            if (result == 0) return false;
            // Negative: one entry needs this much room and none was made.
            buffer = direct(Math.min(-result, MAX_CAPACITY));
        }
    }

    /** Copies the current key into {@code dst}; returns its full length. */
    public int keyInto(byte[] dst) {
        return copy(keyOffset, keyLength, dst);
    }

    /** Copies the current value into {@code dst}; returns its full length. */
    public int valueInto(byte[] dst) {
        return copy(valueOffset, valueLength, dst);
    }

    public byte[] keyBytes() {
        check();
        requirePositioned();
        byte[] out = new byte[keyLength];
        copy(keyOffset, keyLength, out);
        return out;
    }

    public byte[] valueBytes() {
        check();
        requirePositioned();
        byte[] out = new byte[valueLength];
        copy(valueOffset, valueLength, out);
        return out;
    }

    /** Throws if the scan stopped because of a failure rather than exhaustion. */
    public void status() {
        check();
        Native.iterStatus(handle);
    }

    @Override
    public void close() {
        if (handle == 0) return;
        long h = handle;
        handle = 0;
        owner.forgetBatched(this);
        Native.iterDestroy(h);
    }

    void detach() {
        handle = 0;
        remaining = 0;
        positioned = false;
    }

    private int copy(int offset, int length, byte[] dst) {
        check();
        requirePositioned();
        int copied = Math.min(length, dst.length);
        for (int i = 0; i < copied; ++i) dst[i] = buffer.get(offset + i);
        return length;
    }

    private void requirePositioned() {
        if (!positioned) throw new NoSuchElementException("call next() first");
    }

    private void check() {
        if (handle == 0) throw new IllegalStateException("iterator is closed");
        if (creator != null && creator != Thread.currentThread()) {
            throw new IllegalStateException("iterators are single-threaded");
        }
    }
}
