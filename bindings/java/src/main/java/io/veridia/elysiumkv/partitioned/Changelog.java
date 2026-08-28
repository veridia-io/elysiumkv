package io.veridia.elysiumkv.partitioned;

/**
 * Produces one state record into whatever transaction the caller has open, and hands back where it
 * landed. Called by {@link PartitionedStore#put}, never by anything else.
 *
 * <p>The returned position is the state-topic offset that becomes the partition's watermark after
 * a successful commit, and the only thing that makes an incremental restore possible.
 *
 * <p>Implementations should classify their own failures. Anything thrown here happens strictly
 * before the commit and so is abortable, but this component cannot say that in a type the caller's
 * transaction understands, and an unclassified throw escaping the caller's catch blocks leaves a
 * transaction open with batches staged.
 *
 * @param <K> the key type, whose equality the caller supplies
 */
public interface Changelog<K> {
    /** @param mutation never {@code null}, and a delete is a {@link Mutation}, never a tombstone */
    PendingPosition send(int partition, K key, Mutation mutation);

    /**
     * Produces one record deleting every key in {@code [lower, upper)}, for
     * {@link PartitionedStore#deleteRange}. Bounds are store bytes, not {@code K}: a bound need
     * not be a whole key.
     *
     * <p>Precondition: the record must reach a restore under a log key that nothing later
     * supersedes. Compaction retains the newest record per key and a range delete carries no per-key
     * tombstone, so a rebuild that dropped it would resurrect every key the range covered while the
     * puts beneath it are still retained.
     *
     * <p>Abstract rather than defaulted, so that a codec with no range record is a compile error
     * here instead of an {@code UnsupportedOperationException} the first time one is staged. Throwing
     * from an implementation is a fine answer; inheriting the throw silently was not.
     */
    PendingPosition sendDeleteRange(int partition, byte[] lower, byte[] upper);
}
