package io.veridia.elysiumkv;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * An embedded LSM key-value store, over a native core.
 *
 * <pre>{@code
 * try (DiskBlobStore store = new DiskBlobStore("/data/store", "hot");
 *      DiskManifestCatalog catalog = new DiskManifestCatalog("/data");
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
 * <p>Absence is null, never an exception. Every other failure throws, and the exception type says
 * whether retrying makes sense — see {@link ElysiumKVException}. Folding an unreachable store into
 * "no such key" would turn an outage into apparent data loss
 * (ARCHITECTURE.md "The ABI boundary").
 *
 * <p>Single-writer. The engine serialises writes internally, but handles
 * — iterators and pins especially — belong to one thread. {@link
 * ElysiumKVOptions#paranoidChecks(boolean)} turns that from documentation into an
 * exception.
 */
public final class ElysiumKV implements ReadOnlyStore {
    private final boolean checked;
    private final Set<ElysiumKVIterator> iterators =
            Collections.synchronizedSet(Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
    private final Set<BatchedIterator> batchedIterators =
            Collections.synchronizedSet(Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
    private volatile long handle;
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();

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
     * {@link S3BlobStore}, {@link S3ManifestCatalog}, {@link DynamoManifestCatalog}. They are an
     * optional component of the native build, since the AWS SDK is its heaviest dependency.
     *
     * <p>They are always bound, present or not, and throw {@link ConfigException} naming the
     * missing build option when absent: the ABI's shape must not depend on how it was compiled, or
     * a missing feature would surface as this class failing to <em>load</em>.
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

    /**
     * Opens without taking ownership: no manifest write of any kind, no background threads, no
     * reclamation, no compare-and-set. Several may be open at once, alongside a writer, and there is
     * no registration and so no limit on how many.
     *
     * <p>Returns the read-only <em>type</em>, so a caller cannot reach a write by accident. Refuses
     * a store with no manifest rather than creating one, and refuses a store whose transient tier
     * has lost files — repairing that is a manifest write, and serving a version with holes would
     * present stale values as current.
     *
     * <p>The writer must set {@code obsoleteRetentionMs} for this to be safe; see
     * {@link ReadOnlyStore}.
     */
    public static ReadOnlyStore openReadOnly(ElysiumKVOptions options) {
        Native.ensureLoaded();
        return new ElysiumKV(Native.openReadOnly(options.prepare()), options.checked());
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
     * Close the result; a leaked pin holds a block-cache entry forever.
     */
    @Override
    public Pinned get(byte[] key) {
        return callHandle(h -> {
            long[] pin = new long[1];
            ByteBuffer value = Native.get(h, key, key.length, pin);
            return value == null ? null : new Pinned(this, pin[0], value, checked);
        });
    }

    /**
     * As {@link #get(byte[])} for a key already off-heap — nothing is copied in
     * either direction. ARCHITECTURE.md "The ABI boundary" — hot-path callers should allocate keys natively.
     */
    @Override
    public Pinned get(ByteBuffer key) {
        if (!key.isDirect()) {
            throw new IllegalArgumentException("key buffer must be direct; use get(byte[])");
        }
        return callHandle(h -> {
            long[] pin = new long[1];
            ByteBuffer value = Native.getDirect(h, key, key.position(), key.remaining(), pin);
            return value == null ? null : new Pinned(this, pin[0], value, checked);
        });
    }

    /** Copies rather than pinning. Returns null if the key is absent. */
    @Override
    public byte[] getCopy(byte[] key) {
        return callHandle(h -> {
            byte[] out = new byte[512];
            int length = Native.getCopy(h, key, key.length, out);
            if (length < 0) return null;
            while (length > out.length) {
                out = new byte[length];
                length = Native.getCopy(h, key, key.length, out);
                if (length < 0) return null;
            }
            return length == out.length ? out : Arrays.copyOf(out, length);
        });
    }

    /** Pins currently held. Nonzero at close is a leak (ARCHITECTURE.md "The ABI boundary"). */
    @Override
    public long pinsOutstanding() {
        return callHandle(Native::pinsOutstanding);
    }

    // --- writes --------------------------------------------------------------

    /**
     * Writes a value of at most 1 MiB under a key of at most 64 KiB. Anything
     * larger throws {@link ConfigException} rather than being accepted and lost:
     * the limit exists because a block the reader will refuse must never be
     * written in the first place.
     */
    public void put(byte[] key, byte[] value) {
        runHandle(h -> Native.put(h, key, key.length, value, value.length));
    }

    public void delete(byte[] key) {
        runHandle(h -> Native.delete(h, key, key.length));
    }

    /** Applies the batch as a unit. */
    public void write(WriteBatch batch) {
        runHandle(h -> Native.write(h, batch.handle()));
    }

    /** Flushes the memtable to L0. */
    public void flush() {
        runHandle(Native::flush);
    }

    /**
     * Rewrites every file at a level under current settings, in one pass. This
     * is how a codec change finishes: compaction reaches a key range only when
     * that range receives writes, so a dormant range keeps its old codec
     * indefinitely (ARCHITECTURE.md "Inside an SST"). Terminates by construction; a second call does
     * nothing. Watch {@link ElysiumKVStats.Level#filesStaleCodec()}.
     */
    public void compactLevel(int level) {
        runHandle(h -> Native.compactLevel(h, level));
    }

    /**
     * Drops every key below {@code key} in one manifest edit, rather than one tombstone per key.
     *
     * <p>Monotone. A call at or below the current point is a no-op, so this is safe to drive
     * from a loop that does not track what it already asked for. There is no un-truncate.
     *
     * <p>Visibility changes at once; space returns over time. An open iterator is unaffected — it
     * holds the version it started on, as it already does across a compaction.
     */
    public void truncateBelow(byte[] key) {
        runHandle(h -> Native.truncateBelow(h, key, key.length));
    }

    /**
     * Deletes every key in {@code [lower, upper)} — the counterpart to {@link #truncateBelow} for a
     * range that is not a prefix of the keyspace. A tenant sitting in the middle of a keyspace is
     * the case that needs it.
     *
     * <p>Bounds keep their meaning rather than their role: {@code lower} is included and
     * {@code upper} is not, the same convention the iterators use. An empty or inverted range
     * deletes nothing and is not an error, exactly as an iterator over those bounds yields nothing.
     *
     * <p>Not permanent, and not as cheap as a truncation: the range may be written to again
     * immediately, and where a truncation moves one value in the manifest this writes a tombstone
     * that reads in the range consult until compaction resolves it. Space returns as the covered
     * files are rewritten, or at once for a file the range covers whole.
     */
    public void deleteRange(byte[] lower, byte[] upper) {
        Objects.requireNonNull(lower, "lower");
        Objects.requireNonNull(upper, "upper");
        runHandle(h -> Native.deleteRange(h, lower, upper));
    }

    /**
     * Whether a {@link #deleteRange} has finished travelling through the tree: no file at any
     * level still holds data in the band.
     *
     * <p>Answered from the manifest alone, with no reads.
     *
     * <p>Conservative: a recorded key range is a hull, so a file can overlap the band while holding
     * no key in it. {@code true} means every file that could have held one is gone; {@code false}
     * carries no information.
     *
     * <p>A band hidden by {@link #truncateBelow} is not erased — the objects remain until the
     * reclaim collects them. Because it answers about files, a write into the band after the
     * deletion is a new write and sits in the memtable until flushed.
     */
    @Override
    public boolean rangeIsErased(byte[] lower, byte[] upper) {
        Objects.requireNonNull(lower, "lower");
        Objects.requireNonNull(upper, "upper");
        return callHandle(h -> Native.rangeIsErased(h, lower, upper));
    }

    // --- iteration -----------------------------------------------------------

    /** Half-open range scan; null bounds are unbounded. */
    @Override
    public ElysiumKVIterator iterator(byte[] lo, byte[] hi) {
        return callHandle(h -> track(new ElysiumKVIterator(
                this, Native.iterCreate(h, lo, lo == null ? 0 : lo.length, hi,
                                        hi == null ? 0 : hi.length), checked)));
    }

    /** Prefix scan — ARCHITECTURE.md "Absence is an answer, not an error" makes this a first-class path, not sugar over a range. */
    @Override
    public ElysiumKVIterator prefixIterator(byte[] prefix) {
        return callHandle(h -> track(new ElysiumKVIterator(
                this, Native.iterPrefix(h, prefix, prefix.length), checked)));
    }

    /**
     * The same range scan, descending. {@code next()} still advances; it advances towards smaller
     * keys, so the first entry is the largest in range.
     *
     * <p>Bounds keep the meaning they have forward — {@code lo} inclusive, {@code hi} exclusive —
     * so both directions describe the same set of keys and differ only in delivery order.
     */
    @Override
    public ElysiumKVIterator reverseIterator(byte[] lo, byte[] hi) {
        return callHandle(h -> track(new ElysiumKVIterator(
                this, Native.iterCreateReverse(h, lo, lo == null ? 0 : lo.length, hi,
                                               hi == null ? 0 : hi.length), checked)));
    }

    /** Prefix scan, descending. */
    @Override
    public ElysiumKVIterator reversePrefixIterator(byte[] prefix) {
        return callHandle(h -> track(new ElysiumKVIterator(
                this, Native.iterPrefixReverse(h, prefix, prefix.length), checked)));
    }

    /**
     * The same scan, batched: about 7x faster per entry, at the cost of copying
     * each entry instead of borrowing it. See {@link BatchedIterator} for the
     * numbers. Prefer this for a long scan.
     */
    @Override
    public BatchedIterator batchedPrefixIterator(byte[] prefix) {
        return callHandle(h -> track(new BatchedIterator(
                this, Native.iterPrefix(h, prefix, prefix.length), checked)));
    }

    /**
     * Half-open range scan, batched; null bounds are unbounded. Same native iterator as {@link
     * #iterator(byte[], byte[])} — what differs is that entries are copied across in blocks rather
     * than borrowed one at a time, which is where the 4–7x comes from.
     */
    @Override
    public BatchedIterator batchedIterator(byte[] lo, byte[] hi) {
        return callHandle(h -> track(new BatchedIterator(
                this, Native.iterCreate(h, lo, lo == null ? 0 : lo.length, hi,
                                        hi == null ? 0 : hi.length), checked)));
    }

    /** The same batched range scan, descending. */
    @Override
    public BatchedIterator batchedReverseIterator(byte[] lo, byte[] hi) {
        return callHandle(h -> track(new BatchedIterator(
                this, Native.iterCreateReverse(h, lo, lo == null ? 0 : lo.length, hi,
                                               hi == null ? 0 : hi.length), checked)));
    }

    /** The same batched scan, descending. */
    @Override
    public BatchedIterator batchedReversePrefixIterator(byte[] prefix) {
        return callHandle(h -> track(new BatchedIterator(
                this, Native.iterPrefixReverse(h, prefix, prefix.length), checked)));
    }

    // --- statistics ----------------------------------------------------------

    /** One instant of the engine (ARCHITECTURE.md "Statistics are a buffer, not a struct"), from a single native call. */
    @Override
    public ElysiumKVStats stats() {
        return callHandle(h -> {
            byte[] buffer = new byte[256];
            int needed = Native.statsSnapshot(h, buffer);
            if (needed > buffer.length) {
                buffer = new byte[needed];
                needed = Native.statsSnapshot(h, buffer);
            }
            return ElysiumKVStats.decode(buffer, needed);
        });
    }

    @Override
    public void refresh() {
        runHandle(Native::refresh);
    }

    /** Clears {@code requiresRecovery} after a discard. The only way to (ARCHITECTURE.md "A tier is not a level"). */
    public void markRecoveryComplete() {
        runHandle(Native::markRecoveryComplete);
    }

    // --- watermark -----------------------------------------------------------

    /**
     * Records that every write completed so far is at a position at or before {@code position} in
     * whatever log this store is replaying — a changelog offset, typically. The engine orders it,
     * carries it with the data and hands it back at the next open; it never invents, interpolates
     * or interprets one.
     *
     * <p>It is a <em>position</em>, not a time, and unrelated to a tier's {@code maxAge}.
     * Positions must be non-decreasing; a decreasing one raises {@link ConfigException} rather
     * than being clamped, because clamping would hide a replay that went backwards.
     *
     * <p>Cheap and non-blocking: it forces no flush and writes no manifest, so it can be called
     * as often as the caller commits. The value becomes durable when the memtable holding it is
     * flushed, which is why {@link #flush()} promotes it immediately and why
     * {@code flushIntervalMs} is what bounds the lag on a quiet partition.
     *
     * <p>Together with {@link #flush()} this is the whole of a KIP-1035 store-managed offset:
     * {@code commit(offsets)} is {@code setWatermark(offset)} then {@code flush()}, and
     * {@code committedOffset()} is {@link #recoveredWatermark()}.
     */
    public void setWatermark(long position) {
        runHandle(h -> Native.setWatermark(h, position));
    }

    /**
     * The last position whose effect on this store is known to have survived, as established at
     * <em>open</em>. Replaying only the positions <strong>after</strong> it yields the same
     * logical key-value state as replaying the entire log — exclusive, so {@code 80} means resume
     * at {@code 81}.
     *
     * <p>Fixed at open and never changes. The <em>live</em> frontier is
     * {@link ElysiumKVStats#durableWatermark()}, under a different name so that this one's meaning
     * cannot change after the first write.
     *
     * <p>Empty when nothing can be certified — no watermark was ever set, or a lost transient
     * store held data predating the first one — and the caller should replay from the beginning.
     * Distinct from a watermark of zero, which is a valid position.
     *
     * <p>A restore must use this value and not one that has been through a metrics pipeline.
     */
    @Override
    public OptionalLong recoveredWatermark() {
        long value = callHandle(Native::watermark);
        return value < 0 ? OptionalLong.empty() : OptionalLong.of(value);
    }

    // --- lifecycle -----------------------------------------------------------

    /**
     * Closes, releasing any pin still held and detaching any live iterator.
     *
     * <p>With {@link ElysiumKVOptions#paranoidChecks(boolean)} on, leaving either outstanding
     * throws — a leaked pin is a block-cache entry that can never be evicted. Without it, closing
     * simply cleans up.
     *
     * <p>Closing also attempts a flush, since there is no write-ahead log. The attempt is
     * best-effort and a failure is not reported here — {@link #flush()} is the only way to know —
     * and {@link #closeWithoutFlush()} skips it.
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

    /**
     * Closes without the flush {@link #close} attempts, discarding whatever the memtable still
     * holds.
     *
     * <p>That is what a crash leaves behind. Two callers want it: a test that means to lose the
     * writes, and an embedder that has decided they are not worth the shutdown latency.
     */
    public void closeWithoutFlush() {
        long outstanding = closeReportingOutstanding(false);
        if (checked && outstanding != 0) {
            throw new IllegalStateException(
                    outstanding + " pins or iterators were outstanding at close; a leaked pin "
                            + "holds a block-cache entry indefinitely (ARCHITECTURE.md - The ABI boundary)");
        }
    }

    /** Closes and returns what was left outstanding. Zero is clean. */
    public long closeReportingOutstanding() {
        return closeReportingOutstanding(true);
    }

    private long closeReportingOutstanding(boolean flushFirst) {
        lifecycle.writeLock().lock();
        try {
            long h = handle;
            if (h == 0) return 0;
            handle = 0;

            // Close natively *first*: it is what counts the outstanding pins and
            // iterators, and tidying the Java wrappers beforehand would zero the
            // number this method exists to report. The C ABI detaches live iterators
            // rather than freeing them, so destroying the wrappers afterwards is
            // safe by contract.
            long outstanding = flushFirst ? Native.close(h) : Native.closeWithoutFlush(h);
            synchronized (iterators) {
                for (ElysiumKVIterator iterator : new ArrayList<>(iterators)) iterator.close();
                iterators.clear();
            }
            synchronized (batchedIterators) {
                for (BatchedIterator batched : new ArrayList<>(batchedIterators)) batched.detach();
                batchedIterators.clear();
            }
            return outstanding;
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    public boolean isOpen() {
        return handle != 0;
    }

    long handle() {
        long h = handle;
        if (h == 0) throw new IllegalStateException("database is closed");
        return h;
    }

    void release(long pin) {
        lifecycle.readLock().lock();
        try {
            long h = handle;
            if (h != 0) Native.unpin(h, pin);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private <T> T callHandle(LongFunction<T> call) {
        lifecycle.readLock().lock();
        try {
            return call.apply(handle());
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private void runHandle(LongConsumer call) {
        lifecycle.readLock().lock();
        try {
            call.accept(handle());
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private ElysiumKVIterator track(ElysiumKVIterator iterator) {
        iterators.add(iterator);
        return iterator;
    }

    private BatchedIterator track(BatchedIterator iterator) {
        batchedIterators.add(iterator);
        return iterator;
    }

    void forget(ElysiumKVIterator iterator) {
        iterators.remove(iterator);
    }

    void forgetBatched(BatchedIterator iterator) {
        batchedIterators.remove(iterator);
    }
}
