package io.veridia.elysiumkv;

/**
 * A local-disk cache in front of a slower {@link BlobStore} (ARCHITECTURE.md "Caches chain") — which in
 * practice means a remote one. This is the layer that turns a cold {@link
 * S3BlobStore} tier from "every read is a network round trip" into "every read after
 * the first is local".
 *
 * <p>A cache is itself a store, so they compose: memory over disk over S3 is two
 * wrappers, and the engine never learns any of it exists.
 *
 * <pre>{@code
 * try (S3BlobStore remote = S3BlobStore.builder("my-bucket").prefix("cold").open();
 *      DiskCacheBlobStore cold = new DiskCacheBlobStore("/var/cache/elysiumkv", remote,
 *                                                       20L << 30, true)) {
 *     options.addTier(cold, Durability.DURABLE, 0, 0, 0, 0);
 * }
 * }</pre>
 *
 * <p><b>The delegate must outlive this cache</b>, and both must outlive the database.
 * Closing this does not close the store underneath — a cache does not own the thing it
 * caches.
 *
 * <p>No fsync, no crash-consistency protocol, and the directory is wiped at startup.
 * That falls out of the design rather than being a shortcut: the authoritative store
 * is written and acknowledged before anything is cached, so a cache entry is never the
 * only copy and a lost one costs a single refetch.
 *
 * <p>A cache may never be the innermost store of a tier — it holds only copies, so
 * making one the only home for a file is the one arrangement a discard has nothing to
 * fall back on. {@link ElysiumKV#open} rejects it.
 */
public final class DiskCacheBlobStore extends BlobStore {
    /**
     * @param directory     this cache's exclusive property; anything already in it is
     *                      treated as cache content
     * @param delegate      the store below, which must outlive this cache
     * @param cacheOnWrite  populate on write, write-through and never write-back. It
     *                      pays mostly for L0, whose files are read almost
     *                      immediately by the next L0→L1 compaction.
     */
    public DiskCacheBlobStore(String directory, BlobStore delegate, long maxCacheBytes,
                              boolean cacheOnWrite) {
        super(create(directory, delegate, maxCacheBytes, cacheOnWrite));
    }


    /**
     * The same cache, rounding a miss out to a chunk of {@code fetchGranularity} bytes.
     *
     * <p>A sequential read then costs one request per chunk rather than one per block, which
     * against a remote store is the difference between a round trip per block and one per chunk.
     * Unlike a readahead inside an iterator it needs no notion of a scan, so a point lookup whose
     * neighbour is read later is served from what the first one pulled.
     *
     * <p>Amplification is bounded by the chunk rather than by the object: a small read against a
     * large file pulls one chunk, never the file. Zero fetches exactly what was asked, which is
     * what the other constructor does.
     */
    public DiskCacheBlobStore(String directory, BlobStore delegate, long maxCacheBytes,
                              boolean cacheOnWrite, long fetchGranularity) {
        super(create(directory, delegate, maxCacheBytes, cacheOnWrite, fetchGranularity));
    }

    private static long create(String directory, BlobStore delegate, long maxCacheBytes,
                               boolean cacheOnWrite) {
        Native.ensureLoaded();
        return Native.diskCacheBlobStoreCreate(directory, delegate.handle(), maxCacheBytes,
                                               cacheOnWrite);
    }

    private static long create(String directory, BlobStore delegate, long maxCacheBytes,
                               boolean cacheOnWrite, long fetchGranularity) {
        Native.ensureLoaded();
        return Native.diskCacheBlobStoreCreateChunked(directory, delegate.handle(), maxCacheBytes,
                                                      cacheOnWrite, fetchGranularity);
    }
}
