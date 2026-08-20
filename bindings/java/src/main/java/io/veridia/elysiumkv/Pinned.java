package io.veridia.elysiumkv;

import java.nio.ByteBuffer;

/**
 * A borrowed value and the pin that keeps it alive. This is the whole point of a
 * native core: {@link #value()} is a direct buffer over the block in the cache,
 * so a lookup copies nothing (ARCHITECTURE.md "The ABI boundary").
 *
 * <p>Close it. A leaked pin holds a block-cache entry indefinitely — not
 * a leak of a few bytes, but a cache slot that can never be evicted. Use it in
 * try-with-resources and let {@link ElysiumKV#pinsOutstanding()} confirm.
 *
 * <p>The buffer dies with the pin, and Java cannot enforce that. A direct
 * ByteBuffer's address cannot be revoked, so a reference kept past {@link
 * #close()} reads freed memory rather than throwing. {@link #value()} refuses
 * after close, and with {@link ElysiumKVOptions#paranoidChecks(boolean)} so does
 * every access from a thread other than the one that took the pin — but neither
 * can reach a reference already handed out. Do not store the buffer; store the
 * {@code Pinned}, or copy with {@link #toByteArray()}.
 */
public final class Pinned implements AutoCloseable {
    private final ElysiumKV db;
    private final long pin;
    private final ByteBuffer value;
    private final Thread owner;   // null unless checked
    private boolean closed;

    Pinned(ElysiumKV db, long pin, ByteBuffer value, boolean checked) {
        this.db = db;
        this.pin = pin;
        this.value = value.asReadOnlyBuffer();
        this.owner = checked ? Thread.currentThread() : null;
    }

    /** The value, read-only and native-backed. Valid until {@link #close()}. */
    public ByteBuffer value() {
        check();
        return value;
    }

    /** Copies out, for callers that want to outlive the pin. */
    public byte[] toByteArray() {
        check();
        ByteBuffer copy = value.duplicate();
        byte[] out = new byte[copy.remaining()];
        copy.get(out);
        return out;
    }

    public int size() {
        check();
        return value.remaining();
    }

    @Override
    public void close() {
        if (closed) return;   // idempotent: try-with-resources plus an explicit close is normal
        closed = true;
        Native.unpin(db.handle(), pin);
    }

    private void check() {
        if (closed) throw new IllegalStateException("pin already released");
        if (owner != null && owner != Thread.currentThread()) {
            throw new IllegalStateException(
                    "pin taken on " + owner.getName() + ", used on "
                            + Thread.currentThread().getName()
                            + "; iterators and pins are single-threaded (ARCHITECTURE.md - The ABI boundary)");
        }
    }
}
