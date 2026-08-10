package io.veridia.elysiumkv.streams.v3;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;

/**
 * The variant a KTable needs: an {@link ElysiumKVWindowStore} that declares it stores the {@code
 * timestamp + value} bytes Streams hands it, verbatim.
 *
 * <p>Exactly the split the key-value side has, for exactly the same reason. Without the marker,
 * {@code TimestampedWindowStoreBuilder} splices a {@code WindowToTimestampedWindowByteStoreAdapter}
 * in front of the store, which calls {@code rawValue} on the way in — measured, not assumed: a
 * counted window arrives here as sixteen bytes with the marker and eight without it, the record
 * timestamp being what the adapter removed.
 *
 * <p>And the marker cannot simply go on the base class: Streams also reads it to decide whether
 * restored changelog values need a timestamp prepended, so a marked store used as a <em>plain</em>
 * one would write bare values live and timestamped values on restore, diverging only after a
 * rebalance. That mismatch is what {@code requireTimestampedChain} refuses.
 */
public class ElysiumKVTimestampedWindowStore extends ElysiumKVWindowStore
        implements TimestampedBytesStore {

    ElysiumKVTimestampedWindowStore(String name, ElysiumKVStoreConfig config, long retentionPeriodMs,
                                    long segmentIntervalMs, long windowSizeMs,
                                    boolean retainDuplicates) {
        super(name, config, retentionPeriodMs, segmentIntervalMs, windowSizeMs, retainDuplicates);
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
     * Refuses a store built as a plain window store rather than letting it run, since the mismatch
     * is otherwise invisible until a restore writes a format the live path does not.
     */
    private void requireTimestampedChain(StateStore root) {
        if (!(root instanceof TimestampedWindowStore)) {
            throw new IllegalStateException(
                    "Store '" + name + "' stores values in timestamped format but was built as a "
                            + "plain window store (" + root.getClass().getSimpleName() + "). Use "
                            + "ElysiumKVWindowBytesStoreSupplier.plain(...) for a plain store, or "
                            + "build this one with Stores.timestampedWindowStoreBuilder / "
                            + "Materialized.");
        }
    }
}
