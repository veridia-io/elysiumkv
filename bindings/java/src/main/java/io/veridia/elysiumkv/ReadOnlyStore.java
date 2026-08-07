package io.veridia.elysiumkv;

import java.nio.ByteBuffer;
import java.util.OptionalLong;

/**
 * The read surface, and nothing else.
 *
 * <p><b>A type rather than a runtime refusal.</b> A handle typed as {@code ReadOnlyStore} cannot be
 * passed somewhere that writes — the compiler says so. That mirrors the C++ split, where
 * {@code ReadOnlyDB} is a base of {@code DB}; the C ABI cannot express it and reports
 * {@code ELYSIUMKV_CONFIG} instead, which is why this exists on the side that can.
 *
 * <p>Several of these may be open at once against a store another process is writing. That works
 * because objects are immutable and write-once, so a cached block can never become wrong, and
 * because a reader performs no compare-and-set and is therefore outside the ownership protocol
 * entirely — it can neither fence a writer nor be fenced by one.
 *
 * <p><b>The writer must set {@code obsoleteRetentionMs} for any of this to be safe.</b> Its
 * collector decides an object is collectible when nothing in <em>its own process</em> references it,
 * and a reader elsewhere is invisible to that. The retention window is the only thing standing
 * between a compaction there and a vanished file here. A reader that falls behind the window is told
 * so — {@link StaleException}, never a corruption error, because the data is not damaged and there is
 * nothing to restore.
 */
public interface ReadOnlyStore extends AutoCloseable {
    /**
     * Zero-copy lookup. Returns null if the key is absent — and only then.
     * <b>Close the result</b>; a leaked pin holds a block-cache entry forever.
     */
    Pinned get(byte[] key);

    /** As {@link #get(byte[])} for a key already off-heap. */
    Pinned get(ByteBuffer key);

    /** Copying lookup. Returns null if the key is absent — and only then. */
    byte[] getCopy(byte[] key);

    ElysiumKVIterator iterator(byte[] lo, byte[] hi);

    ElysiumKVIterator prefixIterator(byte[] prefix);

    BatchedIterator batchedPrefixIterator(byte[] prefix);

    /** One instant of the engine, from a single native call. */
    ElysiumKVStats stats();

    /**
     * Re-reads the manifest and installs the newest version.
     *
     * <p><b>Explicit, never automatic.</b> A background refresh would let two reads in one logical
     * operation observe different versions with nothing marking where that can happen, and a caller
     * that wants a stable view for the length of a query must be able to have one.
     *
     * <p>Open iterators are unaffected: an iterator holds the version it started on. A no-op on a
     * writable handle, whose version is the newest by construction.
     */
    void refresh();

    /** See {@link ElysiumKV#recoveredWatermark()}. Fixed at open, including across {@link #refresh()}. */
    OptionalLong recoveredWatermark();

    long pinsOutstanding();

    boolean isOpen();

    @Override
    void close();
}
