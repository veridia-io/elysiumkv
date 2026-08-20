package io.veridia.elysiumkv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * One instant of the engine, decoded from a single native call.
 *
 * <p>A single call, so that cross-field relationships hold (ARCHITECTURE.md "The ABI boundary"):
 * per-field accessors would sample a different instant each, leaving the compaction counters
 * describing a different engine state from the level counts beside them. Every file sits in exactly
 * one level and exactly one tier, so {@link #levelBytesTotal()} and {@link #tierBytesTotal()} are
 * the same number along the two axes.
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
        private final long entries;
        private final long tombstones;

        Level(int level, int fileCount, long bytes, long oldestFileAgeMs, int filesStaleCodec,
              boolean ageTriggered, boolean stalling, long entries, long tombstones) {
            this.level = level;
            this.fileCount = fileCount;
            this.bytes = bytes;
            this.oldestFileAgeMs = oldestFileAgeMs;
            this.filesStaleCodec = filesStaleCodec;
            this.ageTriggered = ageTriggered;
            this.stalling = stalling;
            this.entries = entries;
            this.tombstones = tombstones;
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

        /** Records at this level: superseded versions and tombstones included. See {@link ElysiumKVStats#entryCount()}. */
        public long entries() { return entries; }

        /** How many of {@link #entries()} are deletes. */
        public long tombstones() { return tombstones; }
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
        private final long gets;
        private final long puts;
        private final long removes;
        private final long lists;
        private final long bytesRead;
        private final long bytesWritten;
        private final long errors;

        Tier(int tier, int fileCount, long bytes, long oldestFileAgeMs, int filesPendingMigration,
             boolean stalling, long gets, long puts, long removes, long lists, long bytesRead,
             long bytesWritten, long errors) {
            this.tier = tier;
            this.fileCount = fileCount;
            this.bytes = bytes;
            this.oldestFileAgeMs = oldestFileAgeMs;
            this.filesPendingMigration = filesPendingMigration;
            this.stalling = stalling;
            this.gets = gets;
            this.puts = puts;
            this.removes = removes;
            this.lists = lists;
            this.bytesRead = bytesRead;
            this.bytesWritten = bytesWritten;
            this.errors = errors;
        }

        /**
         * Requests this tier's authoritative store has served since it was opened. Against
         * object storage this is the bill, which is per request as much as per byte — and a
         * cache in front of the tier is not counted here, because its effect is its hit rate.
         *
         * <p>Zero from a native library older than these fields: the record width is in the
         * header, so an older layout is read as a shorter record rather than misparsed.
         *
         * <p>Two tiers naming one store report the same numbers. They belong to the store,
         * not the tier, so summing them across tiers double-counts.
         */
        public long gets() {
            return gets;
        }

        public long puts() {
            return puts;
        }

        public long removes() {
            return removes;
        }

        public long lists() {
            return lists;
        }

        public long bytesRead() {
            return bytesRead;
        }

        public long bytesWritten() {
            return bytesWritten;
        }

        /** Failures that were not {@code NotFound}, counted whether or not a retry succeeded. */
        public long errors() {
            return errors;
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
    private final long memtableEntries;
    private final long memtableTombstones;
    private final long backgroundFailures;
    private final long compactionsTrimmed;
    private final long reencryptions;
    private final long filesPendingReencryption;
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
        memtableEntries = headerBytes > 216 ? readLong(buffer, 216) : 0L;
        memtableTombstones = headerBytes > 224 ? readLong(buffer, 224) : 0L;
        backgroundFailures = headerBytes > 232 ? readLong(buffer, 232) : 0L;
        compactionsTrimmed = headerBytes > 240 ? readLong(buffer, 240) : 0L;
        reencryptions = headerBytes > 248 ? readLong(buffer, 248) : 0L;
        filesPendingReencryption = headerBytes > 256 ? readLong(buffer, 256) : 0L;
        durableWatermark = headerBytes > 200 ? readLong(buffer, 200) : 0L;
        watermarkPresent = headerBytes > 208 && buffer[208] != 0;

        List<Level> levelList = new ArrayList<>(levelCount);
        int offset = headerBytes;
        for (int i = 0; i < levelCount && offset + levelRecordBytes <= size; ++i) {
            levelList.add(new Level(readInt(buffer, offset), readInt(buffer, offset + 4),
                                    readLong(buffer, offset + 8), readLong(buffer, offset + 16),
                                    readInt(buffer, offset + 24), buffer[offset + 28] != 0,
                                    buffer[offset + 29] != 0,
                                    levelRecordBytes >= 48 ? readLong(buffer, offset + 32) : 0L,
                                    levelRecordBytes >= 48 ? readLong(buffer, offset + 40) : 0L));
            offset += levelRecordBytes;
        }
        List<Tier> tierList = new ArrayList<>(tierCount);
        for (int i = 0; i < tierCount && offset + tierRecordBytes <= size; ++i) {
            // Read only what this record is wide enough to hold: a native library predating the
            // I/O counters reports a 32-byte record, and the header says so.
            final boolean hasIo = tierRecordBytes >= 88;
            tierList.add(new Tier(readInt(buffer, offset), readInt(buffer, offset + 4),
                                  readLong(buffer, offset + 8), readLong(buffer, offset + 16),
                                  readInt(buffer, offset + 24), buffer[offset + 28] != 0,
                                  hasIo ? readLong(buffer, offset + 32) : 0L,
                                  hasIo ? readLong(buffer, offset + 40) : 0L,
                                  hasIo ? readLong(buffer, offset + 48) : 0L,
                                  hasIo ? readLong(buffer, offset + 56) : 0L,
                                  hasIo ? readLong(buffer, offset + 64) : 0L,
                                  hasIo ? readLong(buffer, offset + 72) : 0L,
                                  hasIo ? readLong(buffer, offset + 80) : 0L));
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

    /** Records in the live and frozen memtables; the memtable deduplicates, so an overwrite adds none. */
    public long memtableEntries() { return memtableEntries; }

    /** How many of {@link #memtableEntries()} are deletes. */
    public long memtableTombstones() { return memtableTombstones; }

    /**
     * Background operations that failed — a flush or a compaction — counted even where the engine
     * retried and succeeded. A store working through a degraded object store otherwise looks
     * exactly like a healthy one.
     */
    public long backgroundFailures() { return backgroundFailures; }

    /**
     * Compactions whose input set {@code maxCompactionBytes} cut down, of {@link #compactions()}.
     *
     * <p>The budget biting is a trade, and this is the only way to see it. A trimmed
     * compaction leaves files behind at an overlapping level, so that level compacts again sooner:
     * the budget buys a bounded exposure window with write amplification. Near zero means the
     * budget is never reached; near {@code compactions()} means nearly every one is being cut, and
     * raising it would do less total work.
     */
    public long compactionsTrimmed() { return compactionsTrimmed; }

    /** Files re-sealed under the primary encryption provider by the background pass. */
    public long reencryptions() { return reencryptions; }

    /**
     * Files whose recorded encryption provider is not the primary.
     *
     * <p>Zero is the signal that a key rotation is complete — and therefore the moment the
     * previous provider may be unregistered. Non-zero while
     * {@code rewriteToPrimary} is off means a rotation was started and never finished, which is a
     * store still depending on a key someone believes they retired.
     */
    public long filesPendingReencryption() { return filesPendingReencryption; }

    /**
     * An <strong>upper bound</strong> on the number of distinct live keys — {@code records -
     * tombstones}, across the levels and the memtable.
     *
     * <p>Provable rather than typical: a live key's newest record is always a put, never a tombstone,
     * so {@code records >= live + tombstones}. The slack is superseded versions that compaction has
     * not merged yet, so on an update-heavy workload over a small key space this can exceed the true
     * count substantially and then fall sharply when compaction catches up. It is exact once
     * everything has merged into the bottommost level.
     *
     * <p>Not named {@code approximateNumEntries}: that name belongs to the Kafka Streams interface,
     * whose accuracy contract the engine does not adopt.
     *
     * <p>Costs a {@link ElysiumKV#stats()} call, which is O(files). Fine on a reporting interval,
     * wrong in a per-record path.
     */
    public long entryCount() {
        long total = memtableEntries - memtableTombstones;
        for (Level level : levels) total += level.entries() - level.tombstones();
        return total;
    }

    /**
     * The <em>live</em> watermark frontier: the position up to which this store's state would
     * survive losing every transient tier. Deliberately not the newest watermark over current
     * files, which would advance on a flush to transient storage and so report progress an
     * operator cannot rely on.
     *
     * <p>The distance between the log's earliest retained offset and this value is how much
     * recovery capability is left: when migration is failing this stops advancing while the
     * changelog keeps expiring.
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
