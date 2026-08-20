package io.veridia.elysiumkv.streams.v3;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

/**
 * The variant a KTable needs: an {@link ElysiumKVKeyValueStore} that declares it stores the {@code
 * timestamp + value} bytes Streams hands it, verbatim.
 *
 * <p>The marker is the whole class: {@link TimestampedBytesStore} has no methods, and storing bytes
 * unaltered is what makes the claim true. Without it, a store materialized into a KTable gets a
 * {@code KeyValueToTimestampedKeyValueByteStoreAdapter} spliced in front, and that adapter:
 *
 * <ul>
 *   <li>it <em>strips the record timestamp before writing</em> and fabricates {@code -1} on read, so
 *       every {@code ValueAndTimestamp} carries a meaningless timestamp while the value looks
 *       correct — a silent loss;
 *   <li>it hard-casts the inner store's iterators to RocksDB's own iterator type, so an IQv2 range
 *       query dies with a {@code ClassCastException} thrown from inside Kafka.
 * </ul>
 *
 * <p>The marker must stay on a separate class rather than move onto the base. Streams reads it in
 * two places — the KTable store builder, to decide whether to splice the adapter, and the state
 * manager, to decide whether restored changelog values need a timestamp prepended. A single marked
 * class used as a <em>plain</em> Processor-API store would take the second path without the first,
 * writing bare values live and timestamp-prefixed values on restore, which corrupts the store only
 * after a rebalance.
 */
public class ElysiumKVTimestampedKeyValueStore extends ElysiumKVKeyValueStore
        implements TimestampedBytesStore {

    ElysiumKVTimestampedKeyValueStore(String name, ElysiumKVStoreConfig config) {
        super(name, config);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        requireTimestampedChain(root);
        super.init(context, root);
    }

    @Override
    @Deprecated
    public void init(ProcessorContext context, StateStore root) {
        requireTimestampedChain(root);
        super.init(context, root);
    }

    /**
     * Fails a store that was built as a plain key-value store, rather than letting it run.
     *
     * <p>The mismatch is otherwise undetectable until a restore rewrites live data in a format the
     * live path does not write — so this trades a startup error, which names its own fix, for
     * corruption discovered later and elsewhere.
     */
    private void requireTimestampedChain(StateStore root) {
        if (!(root instanceof TimestampedKeyValueStore)) {
            throw new IllegalStateException(
                    "Store '" + name + "' stores values in timestamped format but was built as a "
                            + "plain key-value store (" + root.getClass().getSimpleName() + "). Use "
                            + "ElysiumKVKeyValueBytesStoreSupplier.plain(...) for a plain store, or "
                            + "build this one with Stores.timestampedKeyValueStoreBuilder / "
                            + "Materialized.");
        }
    }
}
