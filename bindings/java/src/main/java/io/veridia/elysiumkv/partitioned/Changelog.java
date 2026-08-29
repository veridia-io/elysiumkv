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
 * <p>Reading the partition from inside a send works today, and is deliberately not promised. A send
 * runs while {@link PartitionedStore} holds that partition's write lock, and the store's read paths
 * take its read lock, which the same thread may acquire while holding the write one — so
 * {@link PartitionedStore#get} and the scans do return here, for a hook that wants to know what it is
 * about to change.
 *
 * <p>Not a compatibility guarantee, because promising it would pin the read path to a lock re-entrant
 * with the stage lock, and the read lock is a contended write to one cacheline on every read — which
 * a store folding many keys of one partition at once may well want to replace with a snapshot nothing
 * locks to read. Ask if you need it guaranteed; it is one edit and no consumer currently relies on
 * it.
 *
 * <p>Three conditions on that. It must be <em>this</em> thread: handing the read to another one
 * deadlocks, because that thread's read lock waits on the write lock this one holds. What it sees is
 * committed state plus everything staged earlier in this transaction, but not the mutation now being
 * sent — that is recorded after the send returns. And the read runs inside the critical section, so
 * it extends the time every other stage and read on that partition is blocked; a band scan over a
 * tiered store can reach object storage, which is a long time to hold a partition still.
 *
 * <p>A transaction boundary from inside a send is refused rather than permitted: it would find the
 * lock held and throw.
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
