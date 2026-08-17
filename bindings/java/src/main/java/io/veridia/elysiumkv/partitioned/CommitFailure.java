package io.veridia.elysiumkv.partitioned;

/**
 * A commit that did not fully succeed. The subclasses exist to say <b>what the caller may legally do
 * next</b>, not what this component believes happened.
 *
 * <p>That distinction is the point. Classifying a producer-fencing error as "definitely not
 * committed" is true and useless: the caller then calls {@code abortTransaction()} on a producer for
 * which closing is the only remaining option, and the abort throws the same error back. A correct
 * belief that licenses an illegal action is a worse contract than no contract.
 *
 * <ul>
 *   <li>{@link AbortableNotCommitted} — discard and abort.
 *   <li>{@link OutcomeUnknown} — discard and close; aborting is not permitted.
 *   <li>{@link ProducerDead} — discard and close; aborting would throw.
 *   <li>{@link ApplyFailed} — the commit succeeded; repair the partitions it names.
 * </ul>
 */
public abstract class CommitFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    CommitFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
