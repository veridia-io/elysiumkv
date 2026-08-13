package io.veridia.elysiumkv.partitioned;

/**
 * A read or a stage touched a partition that may lag the log.
 *
 * <p>This is the invariant's second clause enforced rather than documented. Lag is recoverable
 * exactly because the log holds the truth; serving a lagging partition is how the lag gets laundered
 * into the authority — the fold reads state that is missing a committed update, and the value it
 * derives is produced into the changelog and committed. No later restore repairs that, because the
 * wrong value <em>is</em> in the log.
 *
 * <p>The remedy is {@link PartitionedStore#repair}, which replays from a watermark that never moved.
 */
public final class PartitionBehindException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private final int partition;

    PartitionBehindException(int partition) {
        super("partition " + partition + " may lag the log and must be repaired before it is served");
        this.partition = partition;
    }

    public int partition() {
        return partition;
    }
}
