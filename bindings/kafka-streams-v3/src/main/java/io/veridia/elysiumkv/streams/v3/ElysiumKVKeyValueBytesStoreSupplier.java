package io.veridia.elysiumkv.streams.v3;

import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Hands Streams an ElysiumKV-backed bytes store.
 *
 * <p>Use it where you would use {@code Stores.persistentKeyValueStore(name)}. For a KTable — the
 * common case — the plain constructor is what you want:
 *
 * <pre>{@code
 * Materialized.<String, Long>as(new ElysiumKVKeyValueBytesStoreSupplier(name, config))
 *         .withKeySerde(Serdes.String())
 *         .withValueSerde(Serdes.Long());
 * }</pre>
 *
 * <p>For a <b>plain</b> Processor-API store, ask for one explicitly:
 *
 * <pre>{@code
 * StoreBuilder<KeyValueStore<String, Long>> builder = Stores.keyValueStoreBuilder(
 *         ElysiumKVKeyValueBytesStoreSupplier.plain(name, config),
 *         Serdes.String(), Serdes.Long());
 * }</pre>
 *
 * <p>The two are not interchangeable — see {@link ElysiumKVTimestampedKeyValueStore} for what the
 * difference buys and why picking the wrong one fails loudly at startup rather than quietly later.
 *
 * <p>{@link #metricsScope()} reports {@code elysiumkv} so a dashboard can tell these stores from
 * RocksDB ones — which matters during a migration, when both are running and their cost profiles
 * are not comparable.
 */
public final class ElysiumKVKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {
    private final String name;
    private final ElysiumKVStoreConfig config;
    private final boolean timestamped;

    /**
     * A supplier for a KTable-backing store. Defaults to the timestamped variant because that is
     * what {@code Materialized} builds, and because the alternative fails quietly: a plain store in
     * that position loses every record timestamp and cannot answer a range query.
     */
    public ElysiumKVKeyValueBytesStoreSupplier(String name, ElysiumKVStoreConfig config) {
        this(name, config, true);
    }

    /** A supplier for a plain Processor-API store, as built by {@code Stores.keyValueStoreBuilder}. */
    public static ElysiumKVKeyValueBytesStoreSupplier plain(String name,
                                                            ElysiumKVStoreConfig config) {
        return new ElysiumKVKeyValueBytesStoreSupplier(name, config, false);
    }

    /** Explicit form of the default, for call sites that would rather say which they mean. */
    public static ElysiumKVKeyValueBytesStoreSupplier timestamped(String name,
                                                                  ElysiumKVStoreConfig config) {
        return new ElysiumKVKeyValueBytesStoreSupplier(name, config, true);
    }

    private ElysiumKVKeyValueBytesStoreSupplier(String name, ElysiumKVStoreConfig config,
                                                boolean timestamped) {
        this.timestamped = timestamped;
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        if (config.mode() == StorageMode.HYBRID) {
            throw new UnsupportedOperationException(
                    "hybrid mode needs store-managed changelog offsets (KIP-1035, Kafka 4.x)");
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
        return timestamped
                ? new ElysiumKVTimestampedKeyValueStore(name, config)
                : new ElysiumKVKeyValueStore(name, config);
    }

    @Override
    public String metricsScope() {
        return "elysiumkv";
    }
}
