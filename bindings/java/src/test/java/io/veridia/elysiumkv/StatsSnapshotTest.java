package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * ARCHITECTURE.md "The ABI boundary" — the snapshot is one call because a snapshot assembled from per-field
 * accessors is torn. This is the Java end of that: the decoder must read records
 * by the sizes the buffer declares, not by the field widths it happens to know,
 * or a library newer than this binding breaks it.
 */
@ExtendWith(PinLeakExtension.class)
class StatsSnapshotTest {
    @Test
    void theTwoAxesDescribeTheSameFiles(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 3000; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
            }
            db.flush();

            ElysiumKVStats stats = db.stats();
            assertEquals(1, stats.formatVersion());
            assertEquals(2, stats.levels().size());
            assertEquals(1, stats.tiers().size());
            assertTrue(stats.levelBytesTotal() > 0);
            assertEquals(stats.levelBytesTotal(), stats.tierBytesTotal(),
                         "every file is in exactly one level and exactly one tier");
            assertEquals(0, stats.pinsOutstanding());
            db.close();
        }
    }

    @Test
    void countersAndCacheStatsAreReachable(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 4000; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
            }
            db.flush();
            for (int i = 0; i < 4000; ++i) db.getCopy(TestSupport.key(i));

            ElysiumKVStats stats = db.stats();
            // These were unreachable before the snapshot replaced the accessors —
            // there was no per-field call for any of them.
            assertTrue(stats.blockCacheHits() + stats.blockCacheMisses() > 0,
                       "the cache was exercised");
            assertTrue(stats.blockCacheBytes() > 0);

            // The reader cache reaches the binding too. It was the one cache with no
            // number at all, and a cache whose size cannot be observed cannot be sized.
            assertTrue(stats.openReaders() > 0, "reads went through no readers at all");
            assertTrue(stats.readerCacheBytes() > 0,
                       "resident readers hold an index block and a bloom filter");
            assertTrue(stats.readerCacheHits() + stats.readerCacheMisses() > 0);
            // The valve legitimately engages under this workload — a 64 KiB
            // memtable and 4000 writes — so the assertion is on the accounting
            // being self-consistent, not on it never firing.
            if (stats.stallCount() == 0) {
                assertEquals(0, stats.stalledTotalMs(), "no stalls means no stalled time");
            }
            // Output can be far smaller than input — dropping overwrites is the
            // point — so the only safe claim is that both counters move together.
            // Bytes are counted only for a real rewrite: a trivial move reads
            // and writes nothing, so the two counters move together or not at all.
            assertEquals(stats.compactionBytesRead() > 0, stats.compactionBytesWritten() > 0,
                         "compaction byte accounting is one-sided");
            // No maxCompactionBytes is configured here, so nothing can have been trimmed — and a
            // trimmed compaction is one of the compactions rather than an extra one.
            assertEquals(0, stats.compactionsTrimmed(),
                         "no compaction budget was set, so nothing can have been trimmed");
            assertTrue(stats.compactionsTrimmed() <= stats.compactions());
            // No rotation was asked for, so nothing can be pending and nothing can have been
            // rewritten — and a gauge that read non-zero here would make the "rotation is done"
            // signal useless.
            assertEquals(0, stats.reencryptions());
            assertEquals(0, stats.filesPendingReencryption());
            assertEquals(0, stats.manifestPayloadsPendingReencryption());
            db.close();
        }
    }

    /**
     * A library newer than this binding writes wider records. Decoding by the
     * declared sizes must yield the same values; decoding by {@code sizeof} would
     * walk off into the next record and return nonsense.
     */
    @Test
    void theEntryCountIsAnUpperBoundOnLiveKeys(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            assertEquals(0L, db.stats().entryCount(), "an empty store counts nothing");

            for (int i = 0; i < 200; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v"));
            assertEquals(200L, db.stats().entryCount());
            assertEquals(200L, db.stats().memtableEntries(), "nothing flushed yet");

            db.flush();
            assertEquals(200L, db.stats().entryCount(), "a flush moves records, it creates none");
            assertEquals(0L, db.stats().memtableEntries());
            assertTrue(db.stats().levels().get(0).entries() > 0, "the records are at a level now");

            // Deleting every key: the tombstones are records, and the subtraction removes their own
            // contribution — leaving the shadowed puts, which is why this is a bound and not a count.
            for (int i = 0; i < 200; ++i) db.delete(TestSupport.key(i));
            db.flush();
            assertTrue(db.stats().entryCount() >= 0, "never negative");
            long tombstones = 0;
            for (ElysiumKVStats.Level level : db.stats().levels()) tombstones += level.tombstones();
            assertTrue(tombstones > 0, "the deletes are recorded as tombstones");

            // Compacted to the bottommost level, tombstones are dropped and nothing is left.
            for (int level = 0; level < db.stats().levels().size(); ++level) db.compactLevel(level);
            assertEquals(0L, db.stats().entryCount(), "nothing live, and nothing left to say so");
            db.close();
        }
    }

    @Test
    void aNewerFormatWithWiderRecordsStillDecodes(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 500; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v"));
            db.flush();
            ElysiumKVStats actual = db.stats();

            byte[] original = rawSnapshot(db);
            byte[] widened = widen(original, actual.levels().size(), actual.tiers().size());
            ElysiumKVStats decoded = ElysiumKVStats.decode(widened, widened.length);

            assertEquals(actual.levels().size(), decoded.levels().size());
            assertEquals(actual.tiers().size(), decoded.tiers().size());
            assertEquals(actual.levelBytesTotal(), decoded.levelBytesTotal());
            assertEquals(actual.tierBytesTotal(), decoded.tierBytesTotal());
            for (int i = 0; i < actual.levels().size(); ++i) {
                assertEquals(actual.levels().get(i).fileCount(), decoded.levels().get(i).fileCount());
                assertEquals(actual.levels().get(i).bytes(), decoded.levels().get(i).bytes());
            }
            db.close();
        }
    }

    @Test
    void aTierReportsTheIoItsStoreHasServed(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 2000; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v" + i));
            db.flush();
            for (int i = 0; i < 2000; ++i) db.getCopy(TestSupport.key(i));

            ElysiumKVStats.Tier tier = db.stats().tiers().get(0);
            assertTrue(tier.puts() > 0, "a flush writes objects");
            assertTrue(tier.bytesWritten() > 0);
            assertEquals(0L, tier.errors(), "nothing has gone wrong with this store");
            db.close();
        }
    }

    /** The other direction from {@link #aNewerFormatWithWiderRecordsStillDecodes}: a native library
     *  predating a field reports a shorter record, and the decoder must read the prefix rather than
     *  run into the next record. */
    @Test
    void anOlderFormatWithNarrowerTierRecordsStillDecodes(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 500; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v"));
            db.flush();
            ElysiumKVStats actual = db.stats();

            byte[] narrowed = narrowTierRecords(rawSnapshot(db), actual.levels().size(),
                                                actual.tiers().size(), 32);
            ElysiumKVStats decoded = ElysiumKVStats.decode(narrowed, narrowed.length);

            assertEquals(actual.tiers().size(), decoded.tiers().size());
            assertEquals(actual.tierBytesTotal(), decoded.tierBytesTotal());
            for (int i = 0; i < actual.tiers().size(); ++i) {
                assertEquals(actual.tiers().get(i).fileCount(), decoded.tiers().get(i).fileCount());
                assertEquals(0L, decoded.tiers().get(i).puts(),
                             "a field this record is too short to hold reads as zero");
            }
            db.close();
        }
    }

    private static byte[] rawSnapshot(ElysiumKV db) {
        byte[] buffer = new byte[Native.statsSnapshot(db.handle(), null)];
        int written = Native.statsSnapshot(db.handle(), buffer);
        assertEquals(buffer.length, written);
        return buffer;
    }

    /** Re-encodes with a longer header and wider records, as a later version would. */
    private static byte[] widen(byte[] original, int levelCount, int tierCount) {
        final int header = readInt(original, 4);
        final int levelBytes = readInt(original, 8);
        final int tierBytes = readInt(original, 12);
        final int grownHeader = header + 16;
        final int grownLevel = levelBytes + 8;
        final int grownTier = tierBytes + 8;

        byte[] out = new byte[grownHeader + levelCount * grownLevel + tierCount * grownTier];
        System.arraycopy(original, 0, out, 0, header);
        writeInt(out, 4, grownHeader);
        writeInt(out, 8, grownLevel);
        writeInt(out, 12, grownTier);
        for (int i = 0; i < levelCount; ++i) {
            System.arraycopy(original, header + i * levelBytes, out, grownHeader + i * grownLevel,
                             levelBytes);
        }
        for (int i = 0; i < tierCount; ++i) {
            System.arraycopy(original, header + levelCount * levelBytes + i * tierBytes, out,
                             grownHeader + levelCount * grownLevel + i * grownTier, tierBytes);
        }
        return out;
    }

    /** Re-encodes with a shorter tier record, as an earlier version would. */
    private static byte[] narrowTierRecords(byte[] original, int levelCount, int tierCount,
                                            int shrunkTier) {
        final int header = readInt(original, 4);
        final int levelBytes = readInt(original, 8);
        final int tierBytes = readInt(original, 12);

        byte[] out = new byte[header + levelCount * levelBytes + tierCount * shrunkTier];
        System.arraycopy(original, 0, out, 0, header + levelCount * levelBytes);
        writeInt(out, 12, shrunkTier);
        for (int i = 0; i < tierCount; ++i) {
            System.arraycopy(original, header + levelCount * levelBytes + i * tierBytes, out,
                             header + levelCount * levelBytes + i * shrunkTier, shrunkTier);
        }
        return out;
    }

    private static int readInt(byte[] b, int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8 | (b[at + 2] & 0xFF) << 16
                | (b[at + 3] & 0xFF) << 24;
    }

    private static void writeInt(byte[] b, int at, int value) {
        for (int i = 0; i < 4; ++i) b[at + i] = (byte) (value >>> (8 * i));
    }
}
