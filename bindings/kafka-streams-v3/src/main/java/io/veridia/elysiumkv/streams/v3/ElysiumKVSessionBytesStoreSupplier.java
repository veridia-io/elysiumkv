package io.veridia.elysiumkv.streams.v3;

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;

/**
 * Hands Streams an ElysiumKV-backed session store — what {@code windowedBy(SessionWindows)} asks
 * for.
 *
 * <pre>{@code
 * stream.groupByKey()
 *       .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
 *       .count(Materialized.as(ElysiumKVSessionBytesStoreSupplier.of(
 *               "sessions", config, Duration.ofDays(1))));
 * }</pre>
 *
 * <p>There is no timestamped variant, because Kafka has no {@code TimestampedSessionStore} — the
 * split that the key-value and window stores need does not arise here.
 */
public final class ElysiumKVSessionBytesStoreSupplier implements SessionBytesStoreSupplier {
    private final String name;
    private final ElysiumKVStoreConfig config;
    private final long retentionPeriodMs;
    private final long segmentIntervalMs;

    private ElysiumKVSessionBytesStoreSupplier(String name, ElysiumKVStoreConfig config,
                                               long retentionPeriodMs, long segmentIntervalMs) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        if (config.mode() == StorageMode.HYBRID) {
            throw new UnsupportedOperationException(
                    "hybrid mode needs store-managed changelog offsets (KIP-1035, Kafka 4.x)");
        }
        if (retentionPeriodMs < 1) {
            throw new IllegalArgumentException("retentionPeriod must be positive");
        }
        if (segmentIntervalMs < 1) {
            throw new IllegalArgumentException("segmentInterval must be positive");
        }
        this.retentionPeriodMs = retentionPeriodMs;
        this.segmentIntervalMs = segmentIntervalMs;
    }

    public static ElysiumKVSessionBytesStoreSupplier of(String name, ElysiumKVStoreConfig config,
                                                        Duration retentionPeriod) {
        final long retentionMs = retentionPeriod.toMillis();
        return new ElysiumKVSessionBytesStoreSupplier(
                name, config, retentionMs,
                ElysiumKVWindowBytesStoreSupplier.defaultSegmentInterval(retentionMs));
    }

    /** The same, with the segment interval chosen explicitly. */
    public static ElysiumKVSessionBytesStoreSupplier of(String name, ElysiumKVStoreConfig config,
                                                        Duration retentionPeriod,
                                                        Duration segmentInterval) {
        return new ElysiumKVSessionBytesStoreSupplier(name, config, retentionPeriod.toMillis(),
                                                      segmentInterval.toMillis());
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SessionStore<Bytes, byte[]> get() {
        return new ElysiumKVSessionStore(name, config, retentionPeriodMs, segmentIntervalMs);
    }

    @Override
    public long segmentIntervalMs() {
        return segmentIntervalMs;
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriodMs;
    }

    @Override
    public String metricsScope() {
        return "elysiumkv-session";
    }
}
