package io.veridia.elysiumkv.partitioned;

/**
 * A fatal transport error: closing is the only legal move, because aborting would throw the same
 * error back.
 *
 * <p>Fencing, an invalid producer epoch, an out-of-order sequence and authorization failures all land
 * here. An invalid epoch is the case that shows the classification rule earning its keep — it is not
 * obviously fatal, but {@code abortTransaction()} can itself throw it, so a caller told "abortable"
 * would take an action that fails.
 */
public final class ProducerDead extends CommitFailure {
    private static final long serialVersionUID = 1L;

    public ProducerDead(Throwable cause) {
        super("the transport is unusable; close it rather than aborting", cause);
    }

    public ProducerDead(String message, Throwable cause) {
        super(message, cause);
    }
}
