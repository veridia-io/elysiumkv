package io.veridia.elysiumkv.partitioned;

/**
 * The commit may or may not have completed, so the caller may not abort and must close its producer.
 *
 * <p>This component applies nothing and advances nothing: if the transaction did commit, the log
 * holds records the store does not, and if it did not, neither holds them. Both are recoverable, and
 * a watermark that never moved closes the gap at the next restore — discarding the local state and
 * replaying everything is not needed.
 *
 * <p>The partitions do go {@linkplain PartitionedStore#behind() behind}, for the same reason as an
 * apply failure: the transaction may have carried records the store does not hold, so serving them
 * would fold new input against state missing a committed update.
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
