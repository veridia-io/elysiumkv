package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SmokeTest {
    /**
     * Only one caller may destroy the database. The second sees the handle already taken and does
     * nothing, which is what keeps a repeated or racing close from freeing it twice.
     */
    @Test
    void closingTwiceDestroysOnce(@TempDir Path dir) throws Exception {
        Path storeDir = dir.resolve("store");
        java.nio.file.Files.createDirectories(storeDir);

        try (DiskBlobStore store = new DiskBlobStore(storeDir.toString(), "store-0");
             DiskManifestCatalog catalog = new DiskManifestCatalog(dir.toString());
             ElysiumKVOptions options = new ElysiumKVOptions()
                     .manifestCatalog(catalog)
                     .paranoidChecks(true)
                     .addTier(store, Durability.DURABLE, 0, 0, 0)
                     .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                     .level(1, Compression.NONE, 0, 0, 0, 0, 0)) {

            ElysiumKV db = ElysiumKV.open(options);
            db.put(key(0), "v".getBytes(StandardCharsets.UTF_8));

            assertEquals(0, db.closeReportingOutstanding(), "a clean close reports nothing held");
            assertTrue(!db.isOpen());
            assertEquals(0, db.closeReportingOutstanding(),
                    "the handle is already taken, so the second close has nothing to destroy");
            db.close();   // and the ordinary spelling is a no-op too
        }
    }

    @Test
    void openWriteReadClose(@TempDir Path dir) throws Exception {
        Path storeDir = dir.resolve("store");
        java.nio.file.Files.createDirectories(storeDir);

        try (DiskBlobStore store = new DiskBlobStore(storeDir.toString(), "store-0");
             DiskManifestCatalog catalog = new DiskManifestCatalog(dir.toString());
             ElysiumKVOptions options = new ElysiumKVOptions()
                     .manifestCatalog(catalog)
                     .memtableBytes(64 * 1024)
                     .blockBytes(1024)
                     .paranoidChecks(true)
                     .addTier(store, Durability.DURABLE, 0, 0, 0)
                     .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                     .level(1, Compression.ZSTD, 0, 0, 0, 0, 0)) {

            ElysiumKV db = ElysiumKV.open(options);
            for (int i = 0; i < 500; ++i) {
                db.put(key(i), ("value:" + i).getBytes(StandardCharsets.UTF_8));
            }
            db.flush();

            try (Pinned pinned = db.get(key(42))) {
                assertNotNull(pinned);
                assertEquals("value:42",
                             StandardCharsets.UTF_8.decode(pinned.value()).toString());
            }
            assertEquals(0, db.pinsOutstanding());
            assertNull(db.get("absent".getBytes(StandardCharsets.UTF_8)), "absence is null");
            assertEquals("value:7",
                         new String(db.getCopy(key(7)), StandardCharsets.UTF_8));

            int seen = 0;
            try (ElysiumKVIterator it = db.prefixIterator("key:".getBytes(StandardCharsets.UTF_8))) {
                while (it.next()) ++seen;
                it.status();
            }
            assertEquals(500, seen);

            ElysiumKVStats stats = db.stats();
            assertEquals(1, stats.formatVersion());
            assertEquals(2, stats.levels().size());
            assertEquals(1, stats.tiers().size());
            assertEquals(stats.levelBytesTotal(), stats.tierBytesTotal(),
                         "one instant, two axes over the same files");
            assertTrue(stats.levelBytesTotal() > 0);

            assertEquals(0, db.closeReportingOutstanding());
        }
    }

    private static byte[] key(int i) {
        return String.format("key:%06d", i).getBytes(StandardCharsets.UTF_8);
    }
}
