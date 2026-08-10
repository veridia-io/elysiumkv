package io.veridia.elysiumkv.streams.v3;

import java.time.Duration;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Hands Streams an ElysiumKV-backed window store — the shape a windowed aggregation or a
 * stream-stream join asks for.
 *
 * <pre>{@code
 * stream.groupByKey()
 *       .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
 *       .count(Materialized.as(ElysiumKVWindowBytesStoreSupplier.of(
 *               "counts", config, Duration.ofDays(1), Duration.ofMinutes(5), false)));
 * }</pre>
 *
 * <p>The <b>segment interval</b> is the granularity retention works at: entries are grouped by
 * {@code timestamp / segmentInterval} and expiry drops whole groups, so a smaller interval reclaims
 * sooner and makes a fetch span more groups. It defaults to Streams' own rule — half the retention
 * period, floored at a minute — because that is what its stores use and there is no reason to
 * surprise anyone with a different one.
 *
 * <p><b>{@code retainDuplicates} is not a tuning knob.</b> A stream-stream join stores several
 * records under one key and timestamp and needs every one of them; an aggregation stores one value
 * per window and would see duplicates as corruption. Streams sets it for you when you use the DSL —
 * pass what the operator needs, not what looks harmless.
 */
public final class ElysiumKVWindowBytesStoreSupplier implements WindowBytesStoreSupplier {
    private final String name;
    private final ElysiumKVStoreConfig config;
    private final long retentionPeriodMs;
    private final long segmentIntervalMs;
    private final long windowSizeMs;
    private final boolean retainDuplicates;
    private final boolean timestamped;

    private ElysiumKVWindowBytesStoreSupplier(String name, ElysiumKVStoreConfig config,
                                              long retentionPeriodMs, long segmentIntervalMs,
                                              long windowSizeMs, boolean retainDuplicates,
                                              boolean timestamped) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        if (config.mode() == StorageMode.HYBRID) {
            throw new UnsupportedOperationException(
                    "hybrid mode needs store-managed changelog offsets (KIP-1035, Kafka 4.x)");
        }
        if (retentionPeriodMs < 1) {
            throw new IllegalArgumentException("retentionPeriod must be positive");
        }
        if (windowSizeMs < 1) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
        if (segmentIntervalMs < 1) {
            throw new IllegalArgumentException("segmentInterval must be positive");
        }
        if (windowSizeMs > retentionPeriodMs) {
            // Streams checks the same thing: a window wider than the retention period can never be
            // read whole, so it is a configuration mistake rather than an unusual choice.
            throw new IllegalArgumentException(
                    "windowSize (" + windowSizeMs + "ms) cannot exceed retentionPeriod ("
                            + retentionPeriodMs + "ms)");
        }
        this.retentionPeriodMs = retentionPeriodMs;
        this.segmentIntervalMs = segmentIntervalMs;
        this.windowSizeMs = windowSizeMs;
        this.retainDuplicates = retainDuplicates;
        this.timestamped = timestamped;
    }

    /** A supplier for a KTable-backing window store, with Streams' own segment-interval rule. */
    public static ElysiumKVWindowBytesStoreSupplier of(String name, ElysiumKVStoreConfig config,
                                                       Duration retentionPeriod, Duration windowSize,
                                                       boolean retainDuplicates) {
        final long retentionMs = retentionPeriod.toMillis();
        return new ElysiumKVWindowBytesStoreSupplier(name, config, retentionMs,
                                                     defaultSegmentInterval(retentionMs),
                                                     windowSize.toMillis(), retainDuplicates, true);
    }

    /** The same, with the segment interval chosen explicitly. */
    public static ElysiumKVWindowBytesStoreSupplier of(String name, ElysiumKVStoreConfig config,
                                                       Duration retentionPeriod,
                                                       Duration segmentInterval, Duration windowSize,
                                                       boolean retainDuplicates) {
        return new ElysiumKVWindowBytesStoreSupplier(name, config, retentionPeriod.toMillis(),
                                                     segmentInterval.toMillis(),
                                                     windowSize.toMillis(), retainDuplicates, true);
    }

    /** A plain window store, for {@code Stores.windowStoreBuilder} rather than a KTable. */
    public static ElysiumKVWindowBytesStoreSupplier plain(String name, ElysiumKVStoreConfig config,
                                                          Duration retentionPeriod,
                                                          Duration windowSize,
                                                          boolean retainDuplicates) {
        final long retentionMs = retentionPeriod.toMillis();
        return new ElysiumKVWindowBytesStoreSupplier(name, config, retentionMs,
                                                     defaultSegmentInterval(retentionMs),
                                                     windowSize.toMillis(), retainDuplicates, false);
    }

    /** A plain store with the segment interval chosen explicitly. */
    public static ElysiumKVWindowBytesStoreSupplier plain(String name, ElysiumKVStoreConfig config,
                                                          Duration retentionPeriod,
                                                          Duration segmentInterval,
                                                          Duration windowSize,
                                                          boolean retainDuplicates) {
        return new ElysiumKVWindowBytesStoreSupplier(name, config, retentionPeriod.toMillis(),
                                                     segmentInterval.toMillis(),
                                                     windowSize.toMillis(), retainDuplicates,
                                                     false);
    }

    /** Streams' rule: half the retention period, never below a minute. */
    static long defaultSegmentInterval(long retentionPeriodMs) {
        return Math.max(retentionPeriodMs / 2, 60_000L);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        return timestamped
                ? new ElysiumKVTimestampedWindowStore(name, config, retentionPeriodMs,
                                                      segmentIntervalMs, windowSizeMs,
                                                      retainDuplicates)
                : new ElysiumKVWindowStore(name, config, retentionPeriodMs, segmentIntervalMs,
                                           windowSizeMs, retainDuplicates);
    }

    @Override
    public long segmentIntervalMs() {
        return segmentIntervalMs;
    }

    @Override
    public long windowSize() {
        return windowSizeMs;
    }

    @Override
    public boolean retainDuplicates() {
        return retainDuplicates;
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriodMs;
    }

    @Override
    public String metricsScope() {
        return "elysiumkv-window";
    }
}
