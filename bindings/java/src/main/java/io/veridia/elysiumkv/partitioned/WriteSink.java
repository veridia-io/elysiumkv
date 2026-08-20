package io.veridia.elysiumkv.partitioned;

import java.util.Map;

/**
 * Where a restore writes. Bypasses staging entirely: there is no transaction to belong to, the log is
 * the authority being copied from, and routing a replay through a staged set would build all of it in
 * memory before writing any of it.
 *
 * <p>The position travels <em>with</em> the batch rather than being reported at the end, which is
 * what lets a restore advance its own watermark: without it, a replay of 101..500 that closes before
 * any live transaction still reports 100 at the next open, and a cold restore reports nothing until
 * the first subsequent commit. An interrupted restore also resumes from its last flushed batch
 * rather than from where it started.
 *
 * @param <K> the key type, whose equality the caller supplies
 */
@FunctionalInterface
public interface WriteSink<K> {
    /**
     * Applies a batch and records that the store now holds everything through {@code throughOffset}.
     *
     * @param throughOffset the last log position in this batch, inclusive
     * @param mutations     applied as one batch, so the position and the writes it covers become
     *                      durable together
     */
    void putBatch(long throughOffset, Map<K, Mutation> mutations);
}
