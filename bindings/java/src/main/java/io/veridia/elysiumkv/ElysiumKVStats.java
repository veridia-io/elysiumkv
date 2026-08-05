package io.veridia.elysiumkv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * One instant of the engine, decoded from a single native call.
 *
 * <p>That "single" is the point (ARCHITECTURE.md "The ABI boundary"). Assembled from per-field accessors, a
 * snapshot samples a different instant per field, so the compaction counters
 * would not describe the same engine state as the level counts beside them. In
 * one call the cross-field relationships hold — and there is a real one to lean
 * on: every file sits in exactly one level and exactly one tier, so {@link
 * #levelBytesTotal()} and {@link #tierBytesTotal()} are the same number seen
 * along the two axes.
 *
 * <p>The decoder follows the record sizes declared in the buffer rather than the
 * field widths it happens to know, so a library newer than this binding decodes
 * fine — the extra bytes per record are skipped.
 */
public final class ElysiumKVStats {
    /** Per-level facts. A level is LSM structure, never storage (ARCHITECTURE.md "A tier is not a level"). */
    public static final class Level {
        private final int level;
        private final int fileCount;
        private final long bytes;
        private final long oldestFileAgeMs;
        private final int filesStaleCodec;
        private final boolean ageTriggered;
        private final boolean stalling;

        Level(int level, int fileCount, long bytes, long oldestFileAgeMs, int filesStaleCodec,
              boolean ageTriggered, boolean stalling) {
            this.level = level;
            this.fileCount = fileCount;
            this.bytes = bytes;
            this.oldestFileAgeMs = oldestFileAgeMs;
            this.filesStaleCodec = filesStaleCodec;
            this.ageTriggered = ageTriggered;
            this.stalling = stalling;
        }

        public int level() { return level; }
        public int fileCount() { return fileCount; }
        public long bytes() { return bytes; }
        public long oldestFileAgeMs() { return oldestFileAgeMs; }

        /**
         * Files not yet rewritten under this level's current compression. Falls
         * to zero on its own as compaction sweeps — <em>except</em> in key
         * ranges receiving no writes, which are never swept (ARCHITECTURE.md "Inside an SST"). {@link
         * ElysiumKV#compactLevel(int)} forces completion; this is how you know it
         * finished.
         */
        public int filesStaleCodec() { return filesStaleCodec; }
        public boolean ageTriggered() { return ageTriggered; }
        public boolean stalling() { return stalling; }
    }

    /** Per-tier facts. A tier is storage, never structure. */
    public static final class Tier {
        private final int tier;
        private final int fileCount;
        private final long bytes;
        private final long oldestFileAgeMs;
        private final int filesPendingMigration;
        private final boolean stalling;

        Tier(int tier, int fileCount, long bytes, long oldestFileAgeMs, int filesPendingMigration,
             boolean stalling) {
            this.tier = tier;
            this.fileCount = fileCount;
            this.bytes = bytes;
            this.oldestFileAgeMs = oldestFileAgeMs;
            this.filesPendingMigration = filesPendingMigration;
            this.stalling = stalling;
        }

        public int tier() { return tier; }
        public int fileCount() { return fileCount; }
        public long bytes() { return bytes; }

        /**
         * How far back losing this tier's store would reach. On a transient tier
         * this is the number to alarm on — it is the exposure window.
         */
        public long oldestFileAgeMs() { return oldestFileAgeMs; }
        public int filesPendingMigration() { return filesPendingMigration; }
        public boolean stalling() { return stalling; }
    }

    private final int formatVersion;
    private final boolean requiresRecovery;
    private final long memtableBytes;
    private final long memtableAgeMs;
    private final long compactions;
    private final long compactionBytesRead;
    private final long compactionBytesWritten;
    private final long migrations;
    private final long migrationBytes;
    private final long stalledTotalMs;
    private final long stallCount;
    private final long blockCacheHits;
    private final long blockCacheMisses;
    private final long blockCacheBytes;
    private final long pinsOutstanding;
    private final long readerCacheHits;
    private final long readerCacheMisses;
    private final long readerCacheBytes;
    private final long openReaders;
    private final long memoryBudgetUsed;
    private final long memoryBudgetTotal;
    private final long budgetSheds;
    private final long flushes;
    private final long durableWatermark;
    private final boolean watermarkPresent;
    private final List<Level> levels;
    private final List<Tier> tiers;

    private ElysiumKVStats(byte[] buffer, int size) {
        formatVersion = readInt(buffer, 0);
        int headerBytes = readInt(buffer, 4);
        int levelRecordBytes = readInt(buffer, 8);
        int tierRecordBytes = readInt(buffer, 12);
        int levelCount = readInt(buffer, 16);
        int tierCount = readInt(buffer, 20);

        requiresRecovery = buffer[24] != 0;
        memtableBytes = readLong(buffer, 32);
        memtableAgeMs = readLong(buffer, 40);
        compactions = readLong(buffer, 48);
        compactionBytesRead = readLong(buffer, 56);
        compactionBytesWritten = readLong(buffer, 64);
        migrations = readLong(buffer, 72);
        migrationBytes = readLong(buffer, 80);
        stalledTotalMs = readLong(buffer, 88);
        stallCount = readLong(buffer, 96);
        blockCacheHits = readLong(buffer, 104);
        blockCacheMisses = readLong(buffer, 112);
        blockCacheBytes = readLong(buffer, 120);
        pinsOutstanding = readLong(buffer, 128);
        readerCacheHits = readLong(buffer, 136);
        readerCacheMisses = readLong(buffer, 144);
        readerCacheBytes = readLong(buffer, 152);
        openReaders = readLong(buffer, 160);
        memoryBudgetUsed = readLong(buffer, 168);
        memoryBudgetTotal = readLong(buffer, 176);
        budgetSheds = readLong(buffer, 184);
        // Appended after the twenty original scalars. Located by the offsets the format fixes,
        // and read only when the header says they are there: a store built against an older
        // native library reports a shorter header, and reading past it would be reading padding.
        flushes = headerBytes > 192 ? readLong(buffer, 192) : 0L;
        durableWatermark = headerBytes > 200 ? readLong(buffer, 200) : 0L;
        watermarkPresent = headerBytes > 208 && buffer[208] != 0;

        List<Level> levelList = new ArrayList<>(levelCount);
        int offset = headerBytes;
        for (int i = 0; i < levelCount && offset + levelRecordBytes <= size; ++i) {
            levelList.add(new Level(readInt(buffer, offset), readInt(buffer, offset + 4),
                                    readLong(buffer, offset + 8), readLong(buffer, offset + 16),
                                    readInt(buffer, offset + 24), buffer[offset + 28] != 0,
                                    buffer[offset + 29] != 0));
            offset += levelRecordBytes;
        }
        List<Tier> tierList = new ArrayList<>(tierCount);
        for (int i = 0; i < tierCount && offset + tierRecordBytes <= size; ++i) {
            tierList.add(new Tier(readInt(buffer, offset), readInt(buffer, offset + 4),
                                  readLong(buffer, offset + 8), readLong(buffer, offset + 16),
                                  readInt(buffer, offset + 24), buffer[offset + 28] != 0));
            offset += tierRecordBytes;
        }
        levels = Collections.unmodifiableList(levelList);
        tiers = Collections.unmodifiableList(tierList);
    }

    static ElysiumKVStats decode(byte[] buffer, int size) {
        return new ElysiumKVStats(buffer, size);
    }

    private static int readInt(byte[] b, int at) {
        return (b[at] & 0xFF) | (b[at + 1] & 0xFF) << 8 | (b[at + 2] & 0xFF) << 16
                | (b[at + 3] & 0xFF) << 24;
    }

    private static long readLong(byte[] b, int at) {
        return (readInt(b, at) & 0xFFFFFFFFL) | ((long) readInt(b, at + 4)) << 32;
    }

    public int formatVersion() { return formatVersion; }
    public List<Level> levels() { return levels; }
    public List<Tier> tiers() { return tiers; }

    /** True after a discard until {@link ElysiumKV#markRecoveryComplete()} (ARCHITECTURE.md "A tier is not a level"). */
    public boolean requiresRecovery() { return requiresRecovery; }

    public long memtableBytes() { return memtableBytes; }
    public long memtableAgeMs() { return memtableAgeMs; }
    public long compactions() { return compactions; }
    public long compactionBytesRead() { return compactionBytesRead; }
    public long compactionBytesWritten() { return compactionBytesWritten; }
    public long migrations() { return migrations; }

    /** Migration moves bytes without interpreting them, so cost is exactly this. */
    public long migrationBytes() { return migrationBytes; }

    public long stalledTotalMs() { return stalledTotalMs; }
    public long stallCount() { return stallCount; }
    public long blockCacheHits() { return blockCacheHits; }
    public long blockCacheMisses() { return blockCacheMisses; }
    public long blockCacheBytes() { return blockCacheBytes; }

    /** Nonzero at close is a leak, not a diagnostic (ARCHITECTURE.md "The ABI boundary"). */
    public long pinsOutstanding() { return pinsOutstanding; }

    /**
     * The open-SST-reader cache: index blocks and bloom filters. A rising miss count
     * against a steady byte count means {@code readerCacheBytes} is too small for the
     * working set — and each miss costs three reads to reopen the file, which against
     * a remote tier is three round trips.
     */
    public long readerCacheHits() { return readerCacheHits; }

    public long readerCacheMisses() { return readerCacheMisses; }

    public long readerCacheBytes() { return readerCacheBytes; }

    public long openReaders() { return openReaders; }

    /**
     * The process-wide memory budget (ARCHITECTURE.md "A process-wide memory budget"), zero when none was configured. {@code
     * used} may exceed {@code total}: a memtable arena charges unconditionally for a
     * write already accepted, and that overage is the signal the write path sheds on —
     * evict the block cache, flush memtables, then stall. {@code budgetSheds} counts how
     * often that has happened, which is the number that says whether the budget is set
     * too low for the instances sharing it.
     */
    public long memoryBudgetUsed() { return memoryBudgetUsed; }

    public long memoryBudgetTotal() { return memoryBudgetTotal; }

    public long budgetSheds() { return budgetSheds; }

    /**
     * Memtable rotations that became an L0 file. The first place a {@code flushIntervalMs} set
     * too short shows up — small L0 files mean more compaction — and the only way to confirm the
     * interval fires at all on a quiet partition, since {@link #memtableAgeMs()} is a gauge read
     * at scrape time and a flush between two scrapes leaves no trace in it.
     */
    public long flushes() { return flushes; }

    /**
     * The <em>live</em> watermark frontier: the position up to which this store's state would
     * survive losing every transient tier. Deliberately not the newest watermark over current
     * files, which would advance on a flush to transient storage and so report progress an
     * operator cannot rely on.
     *
     * <p><strong>This is the numerator of the only margin an operator can act on.</strong> When
     * migration is failing it stops advancing while the changelog keeps expiring, and the distance
     * between the log's earliest retained offset and this value is how much recovery capability is
     * left.
     *
     * <p>Empty when no watermark has been set. Zero is a valid position, so an exporter must omit
     * the series rather than publish zero. Observational: export it for the retention margin and
     * for alerting, but a restore must use {@link ElysiumKV#recoveredWatermark()}, whose value has
     * not been through a metrics pipeline that may carry it as a double.
     */
    public OptionalLong durableWatermark() {
        return watermarkPresent ? OptionalLong.of(durableWatermark) : OptionalLong.empty();
    }

    public long levelBytesTotal() {
        long total = 0;
        for (Level level : levels) total += level.bytes();
        return total;
    }

    public long tierBytesTotal() {
        long total = 0;
        for (Tier tier : tiers) total += tier.bytes();
        return total;
    }
}
