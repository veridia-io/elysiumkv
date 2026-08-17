package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The hit rate of a cache in front of a remote store is the number that says whether it earns its
 * space — and it was reachable from C++ and from nowhere else.
 */
class CacheStatsTest {

    @Test
    void aDiskCacheReportsHitsAndMisses(@TempDir Path dir) throws IOException {
        Path backing = dir.resolve("backing");
        Path cacheDir = dir.resolve("cache");
        Files.createDirectories(backing);
        Files.createDirectories(cacheDir);

        try (DiskBlobStore below = new DiskBlobStore(backing.toString(), "below");
             DiskCacheBlobStore cache = new DiskCacheBlobStore(
                     cacheDir.toString(), below, 1L << 20, true)) {
            assertEquals(0, cache.hits());
            assertEquals(0, cache.misses());
        }
    }

    /** A plain store is not a cache, and saying so beats reporting zeroes. */
    @Test
    void aPlainStoreIsNotACache(@TempDir Path dir) throws IOException {
        Path backing = dir.resolve("plain");
        Files.createDirectories(backing);
        try (DiskBlobStore store = new DiskBlobStore(backing.toString(), "plain")) {
            assertThrows(RuntimeException.class, () -> Native.blobCacheStats(store.handle()));
        }
    }
}
