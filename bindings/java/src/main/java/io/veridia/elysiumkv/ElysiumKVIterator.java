package io.veridia.elysiumkv;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * A forward scan over a key range. There is no seek and no {@code valid()} —
 * the first {@link #next()} positions it, so the shape is a while-loop:
 *
 * <pre>{@code
 * try (ElysiumKVIterator it = db.prefixIterator(prefix)) {
 *     while (it.next()) {
 *         process(it.key(), it.value());
 *     }
 * }
 * }</pre>
 *
 * <p>Check {@link #next()} returning false with {@link #status()} when it
 * matters: exhaustion and failure look identical otherwise, and a scan cut short
 * by an unreachable store would otherwise read as a short result.
 *
 * <p>Buffers returned by {@link #key()} and {@link #value()} are valid only
 * until the next {@link #next()}. Single-threaded, like the C ABI's iterators.
 */
public final class ElysiumKVIterator implements AutoCloseable {
    private final ElysiumKV db;
    private final Thread owner;   // null unless checked
    private long handle;
    private boolean positioned;

    ElysiumKVIterator(ElysiumKV db, long handle, boolean checked) {
        this.db = db;
        this.handle = handle;
        this.owner = checked ? Thread.currentThread() : null;
    }

    public boolean next() {
        check();
        positioned = Native.iterNext(handle);
        return positioned;
    }

    /** The current key, native-backed. Valid until the next {@link #next()}. */
    public ByteBuffer key() {
        requirePositioned();
        return Native.iterKey(handle).asReadOnlyBuffer();
    }

    public ByteBuffer value() {
        requirePositioned();
        return Native.iterValue(handle).asReadOnlyBuffer();
    }

    public byte[] keyBytes() {
        return copy(key());
    }

    public byte[] valueBytes() {
        return copy(value());
    }

    /**
     * Copies the current key into {@code dst} and returns its full length, which
     * may exceed {@code dst.length} — check it.
     *
     * <p>For a long scan this is the accessor to use. {@link #key()} allocates a
     * direct buffer per call, and ARCHITECTURE.md "Absence is an answer, not an error" makes prefix scanning the dominant read
     * pattern, so that allocation lands on the hottest path there is. Reusing one
     * array removes it without copying any more bytes than the caller was going
     * to read anyway.
     */
    public int keyInto(byte[] dst) {
        requirePositioned();
        return Native.iterKeyInto(handle, dst);
    }

    /** As {@link #keyInto}, for the value. */
    public int valueInto(byte[] dst) {
        requirePositioned();
        return Native.iterValueInto(handle, dst);
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
        db.forget(this);
        Native.iterDestroy(h);
    }

    private static byte[] copy(ByteBuffer buffer) {
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }

    private void requirePositioned() {
        check();
        if (!positioned) throw new NoSuchElementException("call next() first");
    }

    private void check() {
        if (handle == 0) throw new IllegalStateException("iterator is closed");
        if (owner != null && owner != Thread.currentThread()) {
            throw new IllegalStateException(
                    "iterator created on " + owner.getName() + ", used on "
                            + Thread.currentThread().getName() + "; iterators are single-threaded");
        }
    }
}
