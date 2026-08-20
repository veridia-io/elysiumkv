package io.veridia.elysiumkv;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

/**
 * The read surface, and nothing else.
 *
 * <p>A type rather than a runtime refusal, mirroring the C++ split where {@code ReadOnlyDB} is a
 * base of {@code DB}. The C ABI cannot express it and reports {@code ELYSIUMKV_CONFIG} instead.
 *
 * <p>Several of these may be open at once against a store another process is writing: objects are
 * immutable and write-once, so a cached block can never become wrong, and a reader performs no
 * compare-and-set, so it can neither fence a writer nor be fenced by one.
 *
 * <p>The writer must set {@code obsoleteRetentionMs} for that to be safe. Its collector decides an
 * object is collectible when nothing in <em>its own process</em> references it, and a reader
 * elsewhere is invisible to that. A reader that falls behind the window gets
 * {@link StaleException}, never a corruption error — the data is not damaged.
 */
public interface ReadOnlyStore extends AutoCloseable {
    /**
     * Zero-copy lookup. Returns null if the key is absent — and only then.
     * Close the result; a leaked pin holds a block-cache entry forever.
     */
    Pinned get(byte[] key);

    /** As {@link #get(byte[])} for a key already off-heap. */
    Pinned get(ByteBuffer key);

    /** Copying lookup. Returns null if the key is absent — and only then. */
    byte[] getCopy(byte[] key);

    ElysiumKVIterator iterator(byte[] lo, byte[] hi);

    ElysiumKVIterator prefixIterator(byte[] prefix);

    /**
     * The same two scans, descending: {@code next()} advances towards smaller keys, so the first
     * entry is the largest in range. Bounds keep their forward meaning — {@code lo} inclusive,
     * {@code hi} exclusive — so a direction change reorders the answer without changing it.
     */
    ElysiumKVIterator reverseIterator(byte[] lo, byte[] hi);

    ElysiumKVIterator reversePrefixIterator(byte[] prefix);

    BatchedIterator batchedPrefixIterator(byte[] prefix);

    /**
     * Half-open range scan, batched; null bounds are unbounded. The prefix scans have had a
     * batched form since the beginning and this did not, for no reason anyone recorded — so a
     * range scan through the binding was stuck on the slow path while the README measured the
     * batched one at 4–7x faster per entry.
     */
    BatchedIterator batchedIterator(byte[] lo, byte[] hi);

    /** The same batched range scan, descending. */
    BatchedIterator batchedReverseIterator(byte[] lo, byte[] hi);

    /**
     * The batched scan, descending. Present so that choosing a direction never costs the batching:
     * a long descending scan is exactly the case the batched path exists for, and without this one
     * the only reverse option is the per-entry iterator at roughly 7x the cost per entry.
     */
    BatchedIterator batchedReversePrefixIterator(byte[] prefix);

    /** One instant of the engine, from a single native call. */
    ElysiumKVStats stats();

    /**
     * Re-reads the manifest and installs the newest version.
     *
     * <p>Explicit, never automatic. A background refresh would let two reads in one logical
     * operation observe different versions with nothing marking where that can happen, and a caller
     * that wants a stable view for the length of a query must be able to have one.
     *
     * <p>Open iterators are unaffected: an iterator holds the version it started on. A no-op on a
     * writable handle, whose version is the newest by construction.
     */
    void refresh();

    /** See {@link ElysiumKV#recoveredWatermark()}. Fixed at open, including across {@link #refresh()}. */
    OptionalLong recoveredWatermark();

    /** See {@link ElysiumKV#rangeIsErased(byte[], byte[])}. Reads only, so a reader may ask it. */
    boolean rangeIsErased(byte[] lower, byte[] upper);

    long pinsOutstanding();

    boolean isOpen();

    @Override
    void close();
}
