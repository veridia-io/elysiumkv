package io.veridia.elysiumkv.partitioned;

/**
 * The commit may or may not have completed, so the caller may not abort and must close its producer.
 *
 * <p>This component applies nothing and advances nothing, which is the whole of its response: if the
 * transaction did commit, the log holds records the store does not, and if it did not, neither holds
 * them. Both are the recoverable direction, and a watermark that never moved is what closes the gap
 * at the next restore. The instinctive answer — throw the local state away and replay everything — is
 * the expensive fix for a problem the ordering already handles.
 *
 * <p>But the partitions do go {@linkplain PartitionedStore#behind() behind}, for the same reason as
 * an apply failure: the transaction may have carried records the store does not hold, so serving them
 * would fold new input against state that is missing a committed update. Unknown means unknown in
 * both directions, and the readable direction is the one that has to be assumed.
 */
public final class OutcomeUnknown extends CommitFailure {
    private static final long serialVersionUID = 1L;

    public OutcomeUnknown(Throwable cause) {
        super("the commit may have completed; do not abort, close the producer", cause);
    }

    public OutcomeUnknown(String message, Throwable cause) {
        super(message, cause);
    }
}
