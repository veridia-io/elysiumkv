package io.veridia.elysiumkv;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * An embedded LSM key-value store, over a native core.
 *
 * <pre>{@code
 * try (LocalFileBlobStore store = new LocalFileBlobStore("/data/store", "hot");
 *      FileManifestCatalog catalog = new FileManifestCatalog("/data");
 *      ElysiumKVOptions options = new ElysiumKVOptions()
 *          .manifestCatalog(catalog)
 *          .addTier(store, Durability.DURABLE, 0, 0, 0, 0)
 *          .level(0, Compression.NONE, 0, 4, 8, 12, 0)
 *          .level(1, Compression.ZSTD, 0, 0, 0, 0, 0);
 *      ElysiumKV db = ElysiumKV.open(options)) {
 *
 *     db.put(key, value);
 *     try (Pinned pinned = db.get(key)) {
 *         if (pinned != null) consume(pinned.value());   // zero copy
 *     }
 * }
 * }</pre>
 *
 * <p><b>Absence is null, never an exception.</b> Every other failure throws, and
 * the type says whether retrying makes sense — see {@link ElysiumKVException}. The
 * distinction is the ABI's first rule (ARCHITECTURE.md "The ABI boundary"): folding an unreachable store into
 * "no such key" turns an outage into apparent data loss.
 *
 * <p><b>Single-writer.</b> The engine serialises writes internally, but handles
 * — iterators and pins especially — belong to one thread. {@link
 * ElysiumKVOptions#paranoidChecks(boolean)} turns that from documentation into an
 * exception.
 */
public final class ElysiumKV implements AutoCloseable {
    private final boolean checked;
    private final Set<ElysiumKVIterator> iterators =
            Collections.synchronizedSet(Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
    private final Set<BatchedIterator> batchedIterators =
            Collections.synchronizedSet(Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
    private long handle;
    private byte[] statsBuffer = new byte[256];

    private ElysiumKV(long handle, boolean checked) {
        this.handle = handle;
        this.checked = checked;
    }

    /** The native library's compiled-in version. */
    public static String nativeVersion() {
        Native.ensureLoaded();
        return Native.version();
    }

    /**
     * Whether this native library contains the S3 and DynamoDB implementations —
     * {@link S3BlobStore}, {@link S3ManifestCatalog}, {@link
     * DynamoManifestCatalog}. They are an optional component, because the AWS SDK
     * is by far the heaviest dependency in the native build and an embedder with
     * no remote tier should not pay for it.
     *
     * <p><b>They are always bound, present or not</b>, and fail with a {@link
     * ConfigException} naming the missing build option when absent. Making them
     * vanish instead would mean this class failed to <em>load</em> rather than
     * failing to find a feature — the ABI's shape must not depend on how it was
     * compiled.
     */
    public static boolean hasAwsSupport() {
        Native.ensureLoaded();
        return (Native.features() & FEATURE_AWS) != 0;
    }

    private static final int FEATURE_AWS = 1;

    /**
     * Opens, refusing any configuration that contains a transient tier. That is
     * a check rather than a documented precondition (ARCHITECTURE.md "A tier is not a level"): adding a transient
     * tier later must not leave existing call sites silently serving stale
     * values after a discard. Use {@link #openWithResult} when the exposure is
     * intended and understood.
     */
    public static ElysiumKV open(ElysiumKVOptions options) {
        Native.ensureLoaded();
        return new ElysiumKV(Native.open(options.prepare()), options.checked());
    }

    /** Opens any configuration and reports what was discarded (ARCHITECTURE.md "A tier is not a level"). */
    public static OpenResult openWithResult(ElysiumKVOptions options) {
        Native.ensureLoaded();
        long[] dbOut = new long[1];
        long[] discardedFiles = new long[1];
        boolean[] requiresRecovery = new boolean[1];
        String[] stores = Native.openWithResult(options.prepare(), dbOut, discardedFiles,
                                                requiresRecovery);
        ElysiumKV db = new ElysiumKV(dbOut[0], options.checked());
        List<String> discarded =
                stores == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(stores));
        return new OpenResult(db, discarded, discardedFiles[0], requiresRecovery[0]);
    }

    // --- reads ---------------------------------------------------------------

    /**
     * Zero-copy lookup. Returns null if the key is absent — and only then.
     * <b>Close the result</b>; a leaked pin holds a block-cache entry forever.
     */
    public Pinned get(byte[] key) {
        long[] pin = new long[1];
        ByteBuffer value = Native.get(handle(), key, key.length, pin);
        return value == null ? null : new Pinned(this, pin[0], value, checked);
    }

    /**
     * As {@link #get(byte[])} for a key already off-heap — nothing is copied in
     * either direction. ARCHITECTURE.md "The ABI boundary" — hot-path callers should allocate keys natively.
     */
    public Pinned get(ByteBuffer key) {
        if (!key.isDirect()) {
            throw new IllegalArgumentException("key buffer must be direct; use get(byte[])");
        }
        long[] pin = new long[1];
        ByteBuffer value = Native.getDirect(handle(), key, key.remaining(), pin);
        return value == null ? null : new Pinned(this, pin[0], value, checked);
    }

    /** Copies rather than pinning. Returns null if the key is absent. */
    public byte[] getCopy(byte[] key) {
        byte[] out = new byte[512];
        int length = Native.getCopy(handle(), key, key.length, out);
        if (length < 0) return null;
        if (length <= out.length) return Arrays.copyOf(out, length);

        // The value was larger than the guess; ask again with room for it.
        out = new byte[length];
        length = Native.getCopy(handle(), key, key.length, out);
        if (length < 0) return null;
        return length == out.length ? out : Arrays.copyOf(out, length);
    }

    /** Pins currently held. Nonzero at close is a leak (ARCHITECTURE.md "The ABI boundary"). */
    public long pinsOutstanding() {
        return Native.pinsOutstanding(handle());
    }

    // --- writes --------------------------------------------------------------

    /**
     * Writes a value of at most 1 MiB under a key of at most 64 KiB. Anything
     * larger throws {@link ConfigException} rather than being accepted and lost:
     * the limit exists because a block the reader will refuse must never be
     * written in the first place.
     */
    public void put(byte[] key, byte[] value) {
        Native.put(handle(), key, key.length, value, value.length);
    }

    public void delete(byte[] key) {
        Native.delete(handle(), key, key.length);
    }

    /** Applies the batch as a unit. */
    public void write(WriteBatch batch) {
        Native.write(handle(), batch.handle());
    }

    /** Flushes the memtable to L0. */
    public void flush() {
        Native.flush(handle());
    }

    /**
     * Rewrites every file at a level under current settings, in one pass. This
     * is how a codec change finishes: compaction reaches a key range only when
     * that range receives writes, so a dormant range keeps its old codec
     * indefinitely (ARCHITECTURE.md "Inside an SST"). Terminates by construction; a second call does
     * nothing. Watch {@link ElysiumKVStats.Level#filesStaleCodec()}.
     */
    public void compactLevel(int level) {
        Native.compactLevel(handle(), level);
    }

    // --- iteration -----------------------------------------------------------

    /** Half-open range scan; null bounds are unbounded. */
    public ElysiumKVIterator iterator(byte[] lo, byte[] hi) {
        long iter = Native.iterCreate(handle(), lo, lo == null ? 0 : lo.length, hi,
                                      hi == null ? 0 : hi.length);
        return track(new ElysiumKVIterator(this, iter, checked));
    }

    /** Prefix scan — ARCHITECTURE.md "Absence is an answer, not an error" makes this a first-class path, not sugar over a range. */
    public ElysiumKVIterator prefixIterator(byte[] prefix) {
        long iter = Native.iterPrefix(handle(), prefix, prefix.length);
        return track(new ElysiumKVIterator(this, iter, checked));
    }

    /**
     * The same scan, batched: about 7x faster per entry, at the cost of copying
     * each entry instead of borrowing it. See {@link BatchedIterator} for the
     * numbers. Prefer this for a long scan.
     */
    public BatchedIterator batchedPrefixIterator(byte[] prefix) {
        long iter = Native.iterPrefix(handle(), prefix, prefix.length);
        BatchedIterator batched = new BatchedIterator(this, iter, checked);
        batchedIterators.add(batched);
        return batched;
    }

    // --- statistics ----------------------------------------------------------

    /** One instant of the engine (ARCHITECTURE.md "Statistics are a buffer, not a struct"), from a single native call. */
    public ElysiumKVStats stats() {
        int needed = Native.statsSnapshot(handle(), statsBuffer);
        if (needed > statsBuffer.length) {
            statsBuffer = new byte[needed];
            needed = Native.statsSnapshot(handle(), statsBuffer);
        }
        return ElysiumKVStats.decode(statsBuffer, needed);
    }

    /** Clears {@code requiresRecovery} after a discard. The only way to (ARCHITECTURE.md "A tier is not a level"). */
    public void markRecoveryComplete() {
        Native.markRecoveryComplete(handle());
    }

    // --- lifecycle -----------------------------------------------------------

    /**
     * Closes, releasing any pin still held and detaching any live iterator.
     *
     * <p>With {@link ElysiumKVOptions#paranoidChecks(boolean)} on, leaving either
     * outstanding throws: a leaked pin is a block-cache entry that can never be
     * evicted, which is a bug worth failing a test over rather than a tidiness
     * complaint. Without it, closing simply cleans up.
     */
    @Override
    public void close() {
        long outstanding = closeReportingOutstanding();
        if (checked && outstanding != 0) {
            throw new IllegalStateException(
                    outstanding + " pins or iterators were outstanding at close; a leaked pin "
                            + "holds a block-cache entry indefinitely (ARCHITECTURE.md - The ABI boundary)");
        }
    }

    /** Closes and returns what was left outstanding. Zero is clean. */
    public long closeReportingOutstanding() {
        if (handle == 0) return 0;
        long h = handle;
        handle = 0;

        // Close natively *first*: it is what counts the outstanding pins and
        // iterators, and tidying the Java wrappers beforehand would zero the
        // number this method exists to report. The C ABI detaches live iterators
        // rather than freeing them, so destroying the wrappers afterwards is
        // safe by contract.
        long outstanding = Native.close(h);
        synchronized (iterators) {
            for (ElysiumKVIterator iterator : new ArrayList<>(iterators)) iterator.close();
            iterators.clear();
        }
        synchronized (batchedIterators) {
            for (BatchedIterator batched : new ArrayList<>(batchedIterators)) batched.detach();
            batchedIterators.clear();
        }
        return outstanding;
    }

    public boolean isOpen() {
        return handle != 0;
    }

    long handle() {
        if (handle == 0) throw new IllegalStateException("database is closed");
        return handle;
    }

    private ElysiumKVIterator track(ElysiumKVIterator iterator) {
        iterators.add(iterator);
        return iterator;
    }

    void forget(ElysiumKVIterator iterator) {
        iterators.remove(iterator);
    }

    void forgetBatched(BatchedIterator iterator) {
        batchedIterators.remove(iterator);
    }
}
