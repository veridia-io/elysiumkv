package io.veridia.elysiumkv.partitioned;

import java.util.OptionalLong;

/**
 * Replays a partition's changelog into the store. Called on assignment, and again by
 * {@link PartitionedStore#repair} for a partition that fell behind.
 *
 * @param <K> the key type, whose equality the caller supplies
 */
@FunctionalInterface
public interface Restore<K> {
    /**
     * @param materializedThrough the last position this store already holds, <b>inclusive</b> — the
     *                            replay resumes at {@code materializedThrough + 1}. Empty means the
     *                            store has never committed and everything must be replayed; that is
     *                            not the same as zero, which is a real position.
     */
    void restore(int partition, OptionalLong materializedThrough, WriteSink<K> sink);
}
