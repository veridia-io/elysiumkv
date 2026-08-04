package io.veridia.elysiumkv;

import java.nio.ByteBuffer;

/**
 * Every native entry point, and the only class in the binding that has any. The
 * ergonomic layer is built on top of this in plain Java.
 *
 * <p>These are registered explicitly by {@code JNI_OnLoad} rather than resolved
 * by name mangling (ARCHITECTURE.md "The ABI boundary"), so a rename or a signature change fails at library
 * load with a message naming the method, instead of as an {@code
 * UnsatisfiedLinkError} the first time some rarely-used call is made. That
 * matters here: the two rarest methods in this class, {@link
 * #markRecoveryComplete} and {@link #compactLevel}, are the ones an operator
 * reaches for after a discard or a codec change — exactly when a latent link
 * error is least welcome.
 *
 * <p>Handles cross as {@code long}. Errors arrive as exceptions thrown from the
 * glue, never as status codes returned here — except absence, which is not an
 * error and is signalled by {@code null} or {@code -1}.
 */
final class Native {
    private Native() {}

    static {
        NativeLibrary.load();
    }

    /** Forces the static initialiser, and therefore the library load. */
    static void ensureLoaded() {}

    // --- diagnostics ---------------------------------------------------------

    /** Thread-local detail for the last failing call on this thread. */
    static native String lastError();

    static native String version();

    /**
     * What this build can do, as a bitmask. The remote implementations are an
     * optional native component, but the ABI shape is not — the constructors
     * below are always bound, and this says whether they will work.
     */
    static native int features();

    // --- configuration -------------------------------------------------------

    static native long optionsCreate();

    static native void optionsDestroy(long options);

    static native void optionsAddTier(long options, long store, int durability, long maxAgeMs,
                                      long maxFileBytes, long maxBytes, long stallAgeMs);

    static native void optionsSetLevel(long options, int level, int compression, long maxBytes,
                                       int maxFiles, int slowdownAt, int stopAt,
                                       long targetFileBytes);

    /**
     * The whole of Options that is neither a tier nor a level, in one call.
     * Zero means "engine default"; the two flags are tri-state, with a negative
     * value keeping the default.
     */
    static native void optionsConfigure(long options, long manifestCatalog, long memoryBudget,
                                        long memtableBytes,
                                        long blockBytes, long blockCacheBytes,
                                        long readerCacheBytes, int bloomBitsPerKey,
                                        long maxCompactionBytes, int manifestEditsPerGeneration,
                                        int paranoidChecks, int blockOnStall,
                                        int reclaimOrphansAtOpen, long flushIntervalMs);

    // --- seams ---------------------------------------------------------------

    static native long localBlobStoreCreate(String rootDirectory, String storeId);

    static native void blobStoreDestroy(long store);

    static native long fileManifestCatalogCreate(String directory);

    static native void manifestCatalogDestroy(long catalog);

    // --- the shared memory budget (ARCHITECTURE.md "A process-wide memory budget") -------------------------------------

    static native long memoryBudgetCreate(long totalBytes);

    static native void memoryBudgetDestroy(long budget);

    static native long memoryBudgetUsed(long budget);

    // --- cache layers --------------------------------------------------------
    //
    // The delegate handle must outlive the cache built over it: the cache holds a
    // reference to the store, not a copy of the handle.

    static native long diskCacheBlobStoreCreate(String directory, long delegate,
                                                long maxCacheBytes, boolean cacheOnWrite);

    static native long memoryCacheBlobStoreCreate(long delegate, long budget, long maxCacheBytes,
                                                  boolean cacheOnWrite);

    // --- remote seams --------------------------------------------------------
    //
    // These report a status through an exception and return the handle, rather
    // than returning 0 for failure like the local constructors: a bad bucket name
    // is a configuration error and retrying is pointless, while reaching an
    // unreachable DynamoDB is I/O and retrying is right. A single "returned 0"
    // cannot tell those apart, and ElysiumKV's whole error contract rests on the
    // distinction.
    //
    // A null String means "unset": the real service for an endpoint, the SDK's own
    // credential chain for a key. Zero for a timeout means the native default.

    static native long s3BlobStoreCreate(String bucket, String prefix, String region,
                                         String endpoint, String accessKey, String secretKey,
                                         long pointTimeoutMs, long bulkTimeoutMs, String storeId);

    static native long s3ManifestCatalogCreate(String bucket, String prefix, String region,
                                               String endpoint, String accessKey, String secretKey,
                                               long pointTimeoutMs, long bulkTimeoutMs);

    static native long dynamoManifestCatalogCreate(String table, String storeId, String region,
                                                   String endpoint, String accessKey,
                                                   String secretKey, long timeoutMs,
                                                   boolean createTableIfMissing);

    // --- open and close ------------------------------------------------------

    static native long open(long options);

    /**
     * Opens any configuration — including one with a transient tier, which
     * {@link #open} refuses — and reports what was discarded. Returns the ids of
     * the discarded stores; the db handle lands in {@code dbOut[0]}.
     */
    static native String[] openWithResult(long options, long[] dbOut, long[] discardedFiles,
                                          boolean[] requiresRecovery);

    /** Returns the number of pins and iterators left outstanding. Zero is clean. */
    static native long close(long db);

    // --- reads ---------------------------------------------------------------

    /**
     * Zero-copy. Returns a direct buffer over the pinned block, with the pin
     * handle in {@code pinOut[0]}, or {@code null} if the key is absent — and
     * <em>only</em> if it is absent. Every other failure throws (ARCHITECTURE.md "The ABI boundary").
     */
    static native ByteBuffer get(long db, byte[] key, int keyLength, long[] pinOut);

    /** As {@link #get}, for a caller whose key is already off-heap. */
    static native ByteBuffer getDirect(long db, ByteBuffer key, int keyLength, long[] pinOut);

    static native void unpin(long db, long pin);

    /**
     * Copies instead of pinning. Returns the full value length, which may exceed
     * {@code out.length} — check it — or {@code -1} when the key is absent.
     */
    static native int getCopy(long db, byte[] key, int keyLength, byte[] out);

    static native long pinsOutstanding(long db);

    // --- writes --------------------------------------------------------------

    static native void put(long db, byte[] key, int keyLength, byte[] value, int valueLength);

    static native void delete(long db, byte[] key, int keyLength);

    static native long batchCreate();

    static native void batchDestroy(long batch);

    static native void batchPut(long batch, byte[] key, int keyLength, byte[] value,
                                int valueLength);

    static native void batchDelete(long batch, byte[] key, int keyLength);

    static native long batchSize(long batch);

    static native void write(long db, long batch);

    static native void flush(long db);

    static native void compactLevel(long db, int level);

    // --- iteration -----------------------------------------------------------

    static native long iterCreate(long db, byte[] lo, int loLength, byte[] hi, int hiLength);

    static native long iterPrefix(long db, byte[] prefix, int prefixLength);

    static native boolean iterNext(long iter);

    /** A direct buffer over the current key. Valid until the next {@link #iterNext}. */
    static native ByteBuffer iterKey(long iter);

    static native ByteBuffer iterValue(long iter);

    /**
     * Advances a batch of entries directly into {@code dst}, which must be a
     * direct buffer, packed as {@code u32 keyLen | key | u32 valueLen | value}.
     * Returns the entry count, {@code 0} at exhaustion, or a <em>negative</em>
     * number whose magnitude is the size the next single entry needs.
     */
    static native int iterNextBatch(long iter, ByteBuffer dst, int capacity);

    /**
     * Copies the current key into a buffer the caller reuses and returns its full
     * length, which may exceed {@code dst}. One crossing, no allocation — unlike
     * {@link #iterKey}, which mints a direct buffer per call.
     */
    static native int iterKeyInto(long iter, byte[] dst);

    static native int iterValueInto(long iter, byte[] dst);

    /** Throws if the iteration failed; exhaustion and failure look alike otherwise. */
    static native void iterStatus(long iter);

    static native void iterDestroy(long iter);

    // --- statistics ----------------------------------------------------------

    /**
     * Returns the number of bytes the snapshot needs. Pass {@code null} to ask
     * without writing; a buffer shorter than the answer is left untouched.
     */
    static native int statsSnapshot(long db, byte[] out);

    static native void markRecoveryComplete(long db);
}
