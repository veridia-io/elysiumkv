package io.veridia.elysiumkv.streams.v3;

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;

/**
 * Hands Streams an ElysiumKV-backed versioned store — what a versioned KTable asks for.
 *
 * <pre>{@code
 * builder.table(topic, Materialized.as(ElysiumKVVersionedBytesStoreSupplier.of(
 *         "prices", config, Duration.ofDays(7))));
 * }</pre>
 *
 * <p><b>{@code historyRetention} bounds the past, not the present.</b> A key's current value is
 * readable however long ago it was written; the retention says how far back {@code get(key, asOf)}
 * can reach, and a write older than it is refused rather than stored where nothing could read it.
 */
public final class ElysiumKVVersionedBytesStoreSupplier implements VersionedBytesStoreSupplier {
    private final String name;
    private final ElysiumKVStoreConfig config;
    private final long historyRetentionMs;
    private final long segmentIntervalMs;

    private ElysiumKVVersionedBytesStoreSupplier(String name, ElysiumKVStoreConfig config,
                                                 long historyRetentionMs, long segmentIntervalMs) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        if (config.mode() == StorageMode.HYBRID) {
            throw new UnsupportedOperationException(
                    "hybrid mode needs store-managed changelog offsets (KIP-1035, Kafka 4.x)");
        }
        if (historyRetentionMs < 1) {
            throw new IllegalArgumentException("historyRetention must be positive");
        }
        if (segmentIntervalMs < 1) {
            throw new IllegalArgumentException("segmentInterval must be positive");
        }
        this.historyRetentionMs = historyRetentionMs;
        this.segmentIntervalMs = segmentIntervalMs;
    }

    public static ElysiumKVVersionedBytesStoreSupplier of(String name, ElysiumKVStoreConfig config,
                                                          Duration historyRetention) {
        final long retentionMs = historyRetention.toMillis();
        return new ElysiumKVVersionedBytesStoreSupplier(
                name, config, retentionMs,
                ElysiumKVWindowBytesStoreSupplier.defaultSegmentInterval(retentionMs));
    }

    /** The same, with the history segment interval chosen explicitly. */
    public static ElysiumKVVersionedBytesStoreSupplier of(String name, ElysiumKVStoreConfig config,
                                                          Duration historyRetention,
                                                          Duration segmentInterval) {
        return new ElysiumKVVersionedBytesStoreSupplier(name, config, historyRetention.toMillis(),
                                                        segmentInterval.toMillis());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return new ElysiumKVVersionedStore(name, config, historyRetentionMs, segmentIntervalMs);
    }

    @Override
    public long historyRetentionMs() {
        return historyRetentionMs;
    }

    @Override
    public String metricsScope() {
        return "elysiumkv-versioned";
    }
}
