package io.veridia.elysiumkv.partitioned;

/**
 * Everything the caller's transaction must do to commit, as one call.
 *
 * <p>It is one call because the two things inside it — checkpointing input positions and committing —
 * throw an overlapping taxonomy with different meanings, and classifying them belongs in one place.
 * Anything left outside is unclassified: this component's catch blocks never see it, and a
 * transaction can be left open with batches staged.
 *
 * <p>An implementation signals its outcome by throwing {@link AbortableNotCommitted},
 * {@link OutcomeUnknown} or {@link ProducerDead}. Any other exception is treated as
 * {@link OutcomeUnknown}, because assuming a commit did not happen is the unsound direction.
 */
@FunctionalInterface
public interface CommitAction {
    void commit();
}
