package io.veridia.elysiumkv.partitioned;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * The transaction committed and one or more local applies did not.
 *
 * <p>Aborting is wrong here — the input offsets are gone — but the store is not inconsistent. A batch
 * is applied as a unit, so a partition's writes landed or they did not; the watermark rides in the
 * same memtable, so it advanced only if they did; and partitions are independent, so one failing says
 * nothing about the others. Every partition this names is simply <em>behind</em>, which
 * {@link PartitionedStore#repair} fixes from a watermark that never moved.
 *
 * <p>{@link #partitions()} means every partition whose committed changelog may not be
 * materialised, not merely the stores that threw: if a transaction covers {@code {P0, P1, P2}} and
 * applying P1 fails, P2's changelog is committed too, so P2 must be named unless it was applied
 * successfully. Exiting the apply loop on the first failure would leave a partition
 * committed-but-unwritten while still marked ready.
 *
 * <p>{@link #isTerminal()} asks a different question, and it is about the engine rather than about
 * consistency: a retryable I/O failure means ask again later, while a terminal one means this store
 * instance is unusable and the partition has to be reopened regardless.
 */
public final class ApplyFailed extends CommitFailure {
    private static final long serialVersionUID = 1L;

    /** A {@link TreeSet} rather than the interface so that a serialized exception carries it. */
    private final TreeSet<Integer> partitions;
    private final boolean terminal;

    ApplyFailed(SortedSet<Integer> partitions, boolean terminal, Throwable cause) {
        super("committed, but " + partitions.size() + " partition(s) may not hold it: " + partitions,
                cause);
        this.partitions = new TreeSet<>(partitions);
        this.terminal = terminal;
    }

    /** Every partition that may not hold what the transaction committed for it. */
    public Set<Integer> partitions() {
        return Collections.unmodifiableSortedSet(partitions);
    }

    /**
     * Whether the engine's failure was terminal rather than retryable. A terminal one means the store
     * must be closed and reopened, not merely repaired.
     */
    public boolean isTerminal() {
        return terminal;
    }
}
