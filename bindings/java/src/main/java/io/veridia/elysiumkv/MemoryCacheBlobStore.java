package io.veridia.elysiumkv;

/**
 * An in-memory cache in front of a slower {@link BlobStore} (ARCHITECTURE.md "Caches chain").
 *
 * <p>Worth having over a remote delegate and mostly not otherwise: over local files it duplicates
 * the OS page cache, and over hot data the block cache intercepts first, so a range held in both is
 * stored twice and read once. Its non-overlapping role is buffering the large sequential reads of
 * compaction and long scans, which bypass the block cache by design.
 *
 * <p>The delegate must outlive this cache, and both must outlive the database.
 *
 * <p>Bounded by {@code maxCacheBytes} and, when one is supplied, by the process-wide
 * {@link MemoryBudget} it shares with the memtables, the block cache and the open SST
 * readers. A full budget means this cache stops populating — never that a read fails.
 */
public final class MemoryCacheBlobStore extends BlobStore {
    /** @param budget the process-wide budget (ARCHITECTURE.md "A process-wide memory budget"), or null to bound by size alone. */
    public MemoryCacheBlobStore(BlobStore delegate, MemoryBudget budget, long maxCacheBytes,
                                boolean cacheOnWrite) {
        super(create(delegate, budget, maxCacheBytes, cacheOnWrite));
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
    public MemoryCacheBlobStore(BlobStore delegate, MemoryBudget budget, long maxCacheBytes,
                                boolean cacheOnWrite, long fetchGranularity) {
        super(create(delegate, budget, maxCacheBytes, cacheOnWrite, fetchGranularity));
    }

    private static long create(BlobStore delegate, MemoryBudget budget, long maxCacheBytes,
                               boolean cacheOnWrite) {
        Native.ensureLoaded();
        return Native.memoryCacheBlobStoreCreate(delegate.handle(),
                                                 budget == null ? 0 : budget.handle(),
                                                 maxCacheBytes, cacheOnWrite);
    }

    private static long create(BlobStore delegate, MemoryBudget budget, long maxCacheBytes,
                               boolean cacheOnWrite, long fetchGranularity) {
        Native.ensureLoaded();
        return Native.memoryCacheBlobStoreCreateChunked(delegate.handle(),
                                                        budget == null ? 0 : budget.handle(),
                                                        maxCacheBytes, cacheOnWrite,
                                                        fetchGranularity);
    }

    /** Reads that the cache served without touching the delegate. */
    public long hits() {
        return Native.blobCacheStats(handle())[0];
    }

    /**
     * Reads that fell through to the delegate. Against a remote store each one is a round trip, so
     * the ratio against {@link #hits()} is read latency rather than a curiosity.
     */
    public long misses() {
        return Native.blobCacheStats(handle())[1];
    }
}
