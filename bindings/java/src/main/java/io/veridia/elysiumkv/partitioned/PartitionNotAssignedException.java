package io.veridia.elysiumkv.partitioned;

/** A call named a partition this store does not hold. */
public final class PartitionNotAssignedException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private final int partition;

    PartitionNotAssignedException(int partition) {
        super("partition " + partition + " is not assigned to this store");
        this.partition = partition;
    }

    public int partition() {
        return partition;
    }
}
