package io.veridia.elysiumkv.partitioned;

/**
 * Produces one state record into whatever transaction the caller has open, and hands back where it
 * landed. Called by {@link PartitionedStore#stage}, never by anything else.
 *
 * <p>The returned position is what the whole design turns on: it is the state-topic offset that
 * becomes the partition's watermark after a successful commit. A callback typed to return nothing
 * throws away the only thing that makes an incremental restore possible.
 *
 * <p>Implementations should classify their own failures. Anything thrown here happens strictly before
 * the commit, so it is by construction abortable — but this component cannot say that in a type the
 * caller's transaction understands, and an unclassified throw escaping the caller's catch blocks is
 * how a transaction gets left open with batches staged.
 *
 * @param <K> the key type, whose equality the caller supplies
 */
@FunctionalInterface
public interface Changelog<K> {
    /** @param mutation never {@code null}, and a delete is a {@link Mutation}, never a tombstone */
    PendingPosition send(int partition, K key, Mutation mutation);
}
