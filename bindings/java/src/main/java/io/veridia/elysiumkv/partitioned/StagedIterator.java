package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.ElysiumKVIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A scan of committed state with this transaction's staged mutations folded in, in key order.
 *
 * <pre>{@code
 * try (StagedIterator it = store.prefixIterator(partition, prefix)) {
 *     while (it.next()) {
 *         process(it.key(), it.value());
 *     }
 *     it.status();
 * }
 * }</pre>
 *
 * <p>Check {@link #status()} when {@link #next()} returns false: exhaustion and a scan cut short by
 * an unreachable store look identical otherwise, and the staged side keeps delivering after the
 * committed side has stopped.
 *
 * <p>Unlike an engine iterator this copies rather than borrows. A merge draws from a native buffer
 * and a Java array through one accessor, and only one of those can be borrowed — so the batched
 * committed-only scans remain the path for bulk reads.
 *
 * <p>Fixed at creation: neither staging more nor mutating an array already staged changes what it
 * delivers, exactly as an engine iterator holds the version it started on. Single-threaded, and closing it is
 * not optional — an outstanding iterator fails the partition's close under
 * {@code paranoidChecks}.
 */
public final class StagedIterator implements AutoCloseable {

    private final ElysiumKVIterator committed;
    private final StagedSnapshot staged;
    private final Iterator<Map.Entry<byte[], byte[]>> injections;
    private final boolean reverse;

    private byte[] heldKey;
    private byte[] heldValue;
    private Map.Entry<byte[], byte[]> injection;
    private byte[] key;
    private byte[] value;
    private boolean committedDone;
    private boolean closed;
    private RuntimeException committedFailure;
    private boolean exhausted;

    StagedIterator(ElysiumKVIterator committed, StagedSnapshot staged, boolean reverse) {
        this.committed = committed;
        this.staged = staged;
        this.injections = staged.injections();
        this.reverse = reverse;
    }

    /** Positions on the next entry, in ascending key order or descending for a reverse scan. */
    public boolean next() {
        if (exhausted) {
            return false;
        }
        if (heldKey == null && !committedDone) {
            advanceCommitted();
        }
        if (injection == null && injections.hasNext()) {
            injection = injections.next();
        }
        if (heldKey == null && injection == null) {
            exhausted = true;
            key = null;
            value = null;
            return false;
        }
        if (injection == null || (heldKey != null && precedes(heldKey, injection.getKey()))) {
            key = heldKey;
            value = heldValue;
            heldKey = null;
            heldValue = null;
        } else {
            key = injection.getKey();
            value = injection.getValue();
            injection = null;
        }
        return true;
    }

    /** The current key, a copy. */
    public byte[] key() {
        return positioned(key);
    }

    /** The current value, a copy. */
    public byte[] value() {
        return positioned(value);
    }

    /**
     * Throws if the committed side stopped because of a failure rather than exhaustion. Answers after
     * {@link #close} as well as before: the reason is captured where the scan stops, so a caller can
     * drain inside a try-with-resources and ask afterwards.
     *
     * <p>Silent for a scan abandoned before the committed side ran out, which has no exhaustion to
     * explain.
     */
    public void status() {
        if (committedFailure != null) {
            throw committedFailure;
        }
        if (!committedDone && !closed) {
            committed.status();
        }
    }

    @Override
    public void close() {
        closed = true;
        committed.close();
    }

    /** Skips forward over committed keys the staged set answers for; they arrive from that side or not at all. */
    private void advanceCommitted() {
        while (committed.next()) {
            byte[] candidate = committed.keyBytes();
            if (staged.decides(candidate)) {
                continue;
            }
            heldKey = candidate;
            heldValue = committed.valueBytes();
            return;
        }
        committedDone = true;
        // Asked here, while the iterator is certainly open: exhaustion and failure look identical from
        // next() alone, and this is the moment the difference is still available.
        try {
            committed.status();
        } catch (RuntimeException failure) {
            committedFailure = failure;
        }
    }

    private boolean precedes(byte[] a, byte[] b) {
        int order = StagedOverlay.BY_KEY.compare(a, b);
        return reverse ? order > 0 : order < 0;
    }

    private byte[] positioned(byte[] current) {
        if (current == null) {
            throw new NoSuchElementException("call next() first");
        }
        return current;
    }
}
