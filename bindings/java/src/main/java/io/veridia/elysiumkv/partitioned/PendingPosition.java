package io.veridia.elysiumkv.partitioned;

/**
 * Where a staged changelog record landed, readable once the commit has succeeded.
 *
 * <p><b>It does not block.</b> Kafka guarantees that every record callback in a transaction has
 * already run by the time {@code commitTransaction()} returns, so an adapter captures the offset in
 * its send callback and this is a field read. The contract that buys is worth having: a successful
 * commit implies every pending position is synchronously available, which removes an outcome rather
 * than handling one — there is no "committed but the offset could not be resolved" case, and no
 * interruption to propagate.
 *
 * <p>Reading one before a successful commit is a programming error, not a wait. An implementation
 * may return a meaningless value or throw; this component never asks before then.
 */
@FunctionalInterface
public interface PendingPosition {
    /** The log position this record was assigned. */
    long position();
}
