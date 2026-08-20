package io.veridia.elysiumkv.partitioned;

/**
 * Everything the caller's transaction must do to commit, as one call — for callers that own their
 * transaction rather than delegating it to a container.
 *
 * <p>Only {@link PartitionedStore#commit} takes one, and that method is sugar over
 * {@link PartitionedStore#applyCommitted}, {@link PartitionedStore#discard} and
 * {@link PartitionedStore#discardUnknown}. Those three are the contract, because a transaction
 * manager that commits on the application's behalf offers an after-commit hook and no callback
 * position at all.
 *
 * <p>Where a callback <em>is</em> possible it is one place to classify from, and the two things
 * inside it — checkpointing input positions and committing — throw an overlapping taxonomy with
 * different meanings. Anything left outside is unclassified: this component's catch blocks never
 * see it, and a transaction can be left open with batches staged.
 *
 * <p>An implementation signals its outcome by throwing {@link AbortableNotCommitted},
 * {@link OutcomeUnknown} or {@link ProducerDead}. Any other exception is treated as
 * {@link OutcomeUnknown}, because assuming a commit did not happen is the unsound direction.
 */
@FunctionalInterface
public interface CommitAction {
    void commit();
}
