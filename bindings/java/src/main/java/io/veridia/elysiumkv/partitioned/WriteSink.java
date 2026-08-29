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
public interface WriteSink<K> {
    /**
     * Applies a batch and records that the store now holds everything through {@code throughOffset}.
     *
     * @param throughOffset the last log position in this batch, inclusive
     * @param mutations     applied as one batch, so the position and the writes it covers become
     *                      durable together
     */
    void putBatch(long throughOffset, Map<K, Mutation> mutations);

    /**
     * Applies a range delete and records that the store now holds everything through
     * {@code throughOffset}.
     *
     * <p>Precondition: called in log order, so a replay that batches must flush what it holds first.
     * A range delete covers what exists at its own position, and a mutation from an earlier position
     * applied after it resurrects a key the range removed.
     *
     * <p>Abstract rather than defaulted, so that a sink decorating another has to forward this as
     * well as {@link #putBatch}. Inheriting a throw here meant a decorator dropped range replay and
     * only found out when a log first carried one.
     *
     * @param lower included, {@code upper} excluded — the convention the iterators' bounds use
     */
    void deleteRange(long throughOffset, byte[] lower, byte[] upper);
}
