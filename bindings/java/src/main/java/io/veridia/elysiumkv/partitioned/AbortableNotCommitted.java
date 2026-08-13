package io.veridia.elysiumkv.partitioned;

/**
 * The transaction definitely did not commit, and aborting it is permitted. Nothing was applied
 * locally and no watermark moved, so the partitions stay readable.
 *
 * <p>A failure in {@code sendOffsetsToTransaction} belongs here <em>including a timeout</em>: those
 * offsets only join the transaction at that call, so nothing was committed yet. The same exception
 * type out of {@code commitTransaction} means {@link OutcomeUnknown} instead, which is why the phase
 * matters and the type only narrows it.
 */
public final class AbortableNotCommitted extends CommitFailure {
    private static final long serialVersionUID = 1L;

    public AbortableNotCommitted(Throwable cause) {
        super("the transaction did not commit; abort is safe", cause);
    }

    public AbortableNotCommitted(String message, Throwable cause) {
        super(message, cause);
    }
}
