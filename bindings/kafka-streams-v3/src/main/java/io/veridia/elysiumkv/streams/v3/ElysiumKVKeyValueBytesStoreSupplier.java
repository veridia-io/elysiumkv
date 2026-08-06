package io.veridia.elysiumkv.streams.v3;

import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Hands Streams an ElysiumKV-backed bytes store.
 *
 * <p>Use it where you would use {@code Stores.persistentKeyValueStore(name)}:
 *
 * <pre>{@code
 * StoreBuilder<KeyValueStore<String, Long>> builder = Stores.keyValueStoreBuilder(
 *         new ElysiumKVKeyValueBytesStoreSupplier(name, ElysiumKVStoreConfig.local()),
 *         Serdes.String(), Serdes.Long());
 * }</pre>
 *
 * <p>{@link #metricsScope()} reports {@code elysiumkv} so a dashboard can tell these stores from
 * RocksDB ones — which matters during a migration, when both are running and their cost profiles
 * are not comparable.
 */
public final class ElysiumKVKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {
    private final String name;
    private final ElysiumKVStoreConfig config;

    public ElysiumKVKeyValueBytesStoreSupplier(String name, ElysiumKVStoreConfig config) {
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
        return new ElysiumKVKeyValueStore(name, config);
    }

    @Override
    public String metricsScope() {
        return "elysiumkv";
    }
}
