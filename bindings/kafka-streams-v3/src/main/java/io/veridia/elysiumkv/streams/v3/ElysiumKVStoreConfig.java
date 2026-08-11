package io.veridia.elysiumkv.streams.v3;

import io.veridia.elysiumkv.BlobStore;
import io.veridia.elysiumkv.Compression;
import io.veridia.elysiumkv.Durability;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.DiskManifestCatalog;
import io.veridia.elysiumkv.DiskBlobStore;
import io.veridia.elysiumkv.ManifestCatalog;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

/**
 * How one partition's store is configured, and the presets for the three modes.
 *
 * <p>The engine's tier and level configuration is deliberately reachable — a Streams user who wants
 * to tune it should not have to fork the adapter — but the presets exist because the three modes
 * have a small number of settings that must agree, and getting them to disagree silently is worse
 * than not offering the knob.
 */
public final class ElysiumKVStoreConfig {
    private final StorageMode mode;
    private Function<Path, BlobStore> coldStoreFactory;
    private long hotTierMaxAgeMs = 300_000;
    private long memtableBytes = 32L << 20;
    private long flushIntervalMs = 30_000;
    private long obsoleteRetentionMs;
    private Compression compression = Compression.LZ4;

    private ElysiumKVStoreConfig(StorageMode mode) {
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    /** One durable local tier: the RocksDB-shaped configuration. */
    public static ElysiumKVStoreConfig local() {
        return new ElysiumKVStoreConfig(StorageMode.LOCAL);
    }

    /**
     * One durable object-store tier. The factory receives the partition's directory and returns the
     * store to use — normally a disk cache wrapping S3, because every cold read is otherwise a GET
     * on the processing path.
     *
     * <p>A cache chain as the single tier is legal: the engine rejects only a configuration whose
     * <em>innermost</em> authoritative store is a cache.
     */
    public static ElysiumKVStoreConfig remote(Function<Path, BlobStore> store) {
        ElysiumKVStoreConfig config = new ElysiumKVStoreConfig(StorageMode.REMOTE);
        config.coldStoreFactory = Objects.requireNonNull(store, "store");
        return config;
    }

    /**
     * Not available in Phase 1. Throws {@link UnsupportedOperationException} with the reason, rather
     * than building something that would lose data after a pod loss.
     *
     * @see StorageMode#HYBRID
     */
    public static ElysiumKVStoreConfig hybrid(Function<Path, BlobStore> coldStore) {
        throw new UnsupportedOperationException(
                "hybrid mode needs store-managed changelog offsets (KIP-1035, Kafka 4.x). Without "
                + "them Streams' checkpoint file assumes local state is durable, so after losing "
                + "the transient tier it would resume from an offset whose state lived only there. "
                + "Use StorageMode.REMOTE until that lands.");
    }

    /** How long a file stays on the hot tier before migrating. Ignored in {@link StorageMode#LOCAL}. */
    public ElysiumKVStoreConfig hotTierMaxAgeMs(long millis) {
        this.hotTierMaxAgeMs = millis;
        return this;
    }

    public ElysiumKVStoreConfig memtableBytes(long bytes) {
        this.memtableBytes = bytes;
        return this;
    }

    /**
     * Bounds how long a write stays only in memory. Matters more here than in a plain embedding: a
     * partition that goes quiet still has unflushed state, and no tier age bound can reach it
     * because those act on files.
     */
    public ElysiumKVStoreConfig flushIntervalMs(long millis) {
        this.flushIntervalMs = millis;
        return this;
    }

    /**
     * Set this if anything opens the store read-only while Streams is writing it — an inspection
     * tool, or interactive queries from another process. It is the only thing keeping the writer's
     * collector from deleting files that reader is still reading.
     */
    public ElysiumKVStoreConfig obsoleteRetentionMs(long millis) {
        this.obsoleteRetentionMs = millis;
        return this;
    }

    public ElysiumKVStoreConfig compression(Compression compression) {
        this.compression = Objects.requireNonNull(compression, "compression");
        return this;
    }

    StorageMode mode() {
        return mode;
    }

    /**
     * Builds the engine options for one partition's directory.
     *
     * <p>Every store created here is closed by the caller through the returned options' lifecycle;
     * the blob stores and catalog are owned by the options object.
     */
    ElysiumKVOptions toOptions(Path directory) {
        ManifestCatalog catalog = new DiskManifestCatalog(directory.toString());

        ElysiumKVOptions options = new ElysiumKVOptions()
                .manifestCatalog(catalog)
                .memtableBytes(memtableBytes)
                .flushIntervalMs(flushIntervalMs)
                .obsoleteRetentionMs(obsoleteRetentionMs);

        if (mode == StorageMode.LOCAL) {
            // The engine writes objects into this directory but does not create it: a store whose
            // root is missing is an unreachable store, and inventing one would hide a mistyped path.
            Path sst = directory.resolve("sst");
            sst.toFile().mkdirs();
            BlobStore hot = new DiskBlobStore(sst.toString(), "store-0");
            options.addTier(hot, Durability.DURABLE, 0, 0, 0);
        } else {
            // One durable tier, which is normally a cache chain over object storage. There is no
            // hot tier to age off, so hotTierMaxAgeMs does not apply.
            BlobStore cold = coldStoreFactory.apply(directory);
            options.addTier(cold, Durability.DURABLE, 0, 0, 0);
        }

        // L0 with generous file limits, then two compacted levels. Streams workloads are
        // update-heavy on a bounded key space, so most of the win is in merging duplicates.
        options.level(0, Compression.NONE, 0, 4, 8, 12, 0)
               .level(1, compression, 64L << 20, 0, 0, 0, 0)
               .level(2, compression, 0, 0, 0, 0, 0);
        return options;
    }

    long hotTierMaxAgeMs() {
        return hotTierMaxAgeMs;
    }
}
