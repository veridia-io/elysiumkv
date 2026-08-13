package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.OpenResult;
import io.veridia.elysiumkv.RetryableException;
import io.veridia.elysiumkv.WriteBatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * A partitioned local store backed by a log the caller already replays.
 *
 * <h2>The invariant everything rests on</h2>
 *
 * <blockquote><b>The log is authoritative. The store is derived. The store may only ever lag — and a
 * lagging partition may not be served.</b></blockquote>
 *
 * <p>The first clause is enforced structurally: {@link #stage} does not write. It stages, and
 * {@link #commit} applies the staged set as one batch after the caller's transaction has committed.
 * A write cannot reach the store except through a commit, because no other path exists. So a process
 * that dies in between leaves the store <em>behind</em> the log, which is the recoverable direction —
 * the watermark did not advance, and the next restore replays what was missed.
 *
 * <p>The second clause is why {@link #getCommittedBatch} and {@link #stage} reject a
 * {@linkplain #behind() behind} partition. Lag is recoverable exactly because the log holds the
 * truth; serving a lagging partition is how the lag gets laundered into the authority:
 *
 * <pre>
 * state = S0
 * transaction A:  the log commits U1;  the local apply of U1 fails    -&gt; the store is behind
 * transaction B:  getCommittedBatch returns S0, not S0 + U1
 *                 the fold produces a value derived from stale state
 *                 that value is produced into the log and committed
 * </pre>
 *
 * <p>No later restore repairs that, because the wrong value <em>is</em> in the log. The two clauses
 * are not independent: the second is what keeps the first true.
 *
 * <pre>
 *    READY ---- an apply fails, or a commit outcome is unknown ----&gt; BEHIND
 *      ^                                                              |
 *      +--------------- repair() from the unchanged watermark --------+
 * </pre>
 *
 * <h2>Two checkpoints that never meet</h2>
 *
 * <p>The <em>input</em> checkpoint is the caller's and travels in its transaction. The <em>state</em>
 * checkpoint is this component's: {@link #stage} keeps the {@link PendingPosition} each changelog
 * send returns, and after a successful commit the maximum per partition becomes that partition's
 * watermark. They diverge immediately — one state record per changed key, not per input record — so
 * {@link #commit} takes no positions at all and the caller never handles the second.
 *
 * <h2>Threading</h2>
 *
 * <p>Reads, stages and commits belong to one thread, the one that owns the caller's transaction.
 * {@link #behind()} is safe to call from another.
 *
 * @param <K> the key type. Not {@code byte[]}: Java arrays use identity equality, so two separately
 *            deserialised arrays holding the same bytes are different map keys — a
 *            {@code Map<byte[], ?>} looks natural and is silently wrong.
 */
public final class PartitionedStore<K> implements AutoCloseable {

    private final IntFunction<ElysiumKVOptions> options;
    private final Function<K, byte[]> keyBytes;
    private final Changelog<K> changelog;
    private final Restore<K> restore;

    /** Assigned partitions, sorted so that iteration order is the partition order. */
    private final Map<Integer, Partition> partitions = Collections.synchronizedMap(new TreeMap<>());

    /** What {@link #stage} has accumulated since the last commit or discard. */
    private final Map<Integer, Staged> staged = new TreeMap<>();

    private PartitionedStore(Builder<K> builder) {
        this.options = builder.options;
        this.keyBytes = builder.keyBytes;
        this.changelog = builder.changelog;
        this.restore = builder.restore;
    }

    public static <K> Builder<K> builder() {
        return new Builder<>();
    }

    // --- assignment ----------------------------------------------------------

    /**
     * Opens and restores each partition, then serves it. Synchronous, which is the contract that
     * makes correctness easy to state — a partition is either ready or not assigned.
     *
     * <p>Operationally that is a requirement rather than a detail: a restore of gigabytes runs on the
     * calling thread, and a consumer that is not polling is one its coordinator will eventually
     * evict. Whatever bounds the gap between polls must cover the worst-case replay. The same applies
     * to {@link #repair}, which is the one that happens when something has already gone wrong.
     *
     * <p>A restore that throws fails the assignment and leaves nothing open: serving a partially
     * restored partition is the worst outcome available.
     */
    public void assign(Collection<Integer> ids) {
        for (int id : new TreeSet<>(Objects.requireNonNull(ids, "ids"))) {
            if (partitions.containsKey(id)) {
                continue;
            }
            // openWithResult, not open: open() refuses a transient tier outright, so using it here
            // would have quietly excluded every tiered configuration this engine exists for. It also
            // reports what a lost transient store cost, and the recovery flag it sets is discharged
            // by exactly the replay this method already does before serving anything.
            OpenResult opened = ElysiumKV.openWithResult(options.apply(id));
            ElysiumKV db = opened.db();
            Partition partition = new Partition(id, db);
            try {
                replay(partition);
                db.markRecoveryComplete();
            } catch (RuntimeException | Error failure) {
                // Closed *with* a flush, deliberately. Whatever the restore managed to apply came
                // from the log and its watermark covers exactly that, so keeping it is both safe and
                // the reason a retried restore resumes rather than starting over.
                db.close();
                throw failure;
            }
            partitions.put(id, partition);
        }
    }

    /** The partitions this store currently holds. */
    public Set<Integer> assignment() {
        synchronized (partitions) {
            return new TreeSet<>(partitions.keySet());
        }
    }

    /**
     * Partitions that may lag the log. Every one of them rejects reads and stages until
     * {@link #repair} replays it.
     */
    public Set<Integer> behind() {
        SortedSet<Integer> result = new TreeSet<>();
        synchronized (partitions) {
            for (Partition partition : partitions.values()) {
                if (partition.behind) {
                    result.add(partition.id);
                }
            }
        }
        return result;
    }

    /**
     * Replays each named partition from the watermark it actually reached, returning it to service.
     *
     * <p>The watermark it restores from is by construction the one that did not move when the apply
     * failed, so the replay covers exactly what was missed. A partition whose repair throws stays
     * behind.
     */
    public void repair(Collection<Integer> ids) {
        for (int id : new TreeSet<>(Objects.requireNonNull(ids, "ids"))) {
            replay(require(id, false));
        }
    }

    /** Flushes and closes each partition — the handover case, where being as far along as possible
     * is the point. */
    public void revoke(Collection<Integer> ids) {
        for (int id : new TreeSet<>(Objects.requireNonNull(ids, "ids"))) {
            Partition partition = partitions.remove(id);
            if (partition == null) {
                continue;
            }
            dropStaged(id);
            partition.db.flush();
            partition.db.close();
        }
    }

    /**
     * Closes each partition <b>without</b> flushing, because another instance may already hold it.
     *
     * <p>Not the same as {@link #revoke}: flushing into a store someone else has opened is something
     * the engine's compare-and-set would catch, but catching it is worse than not doing it. Whatever
     * had not reached disk is redelivered from the log to whoever owns it now.
     */
    public void lost(Collection<Integer> ids) {
        for (int id : new TreeSet<>(Objects.requireNonNull(ids, "ids"))) {
            Partition partition = partitions.remove(id);
            if (partition == null) {
                continue;
            }
            dropStaged(id);
            partition.db.closeWithoutFlush();
        }
    }

    // --- the transaction -----------------------------------------------------

    /**
     * Reads committed state for a set of keys at once.
     *
     * <p>It cannot be misunderstood the way a bare {@code get} can: it returns what the store holds,
     * never what this transaction has staged. And it reads a <em>set</em> because that is what the
     * work does — the keys for a batch are known before any of it is processed, so one call fetches
     * the working set and the parallel phase touches no store at all.
     *
     * @return the keys that are present, with absence expressed by omission
     */
    public Map<K, byte[]> getCommittedBatch(int partition, Collection<K> keys) {
        Partition held = require(partition, true);
        Map<K, byte[]> committed = new LinkedHashMap<>();
        for (K key : Objects.requireNonNull(keys, "keys")) {
            byte[] value = held.db.getCopy(keyBytes.apply(Objects.requireNonNull(key, "key")));
            if (value != null) {
                committed.put(key, value);
            }
        }
        return committed;
    }

    /**
     * Enqueues the changelog sends and appends to this partition's pending batch. <b>Writes
     * nothing.</b>
     *
     * <p>Sending on stage looks like it breaks the invariant — the log written before the store, with
     * no commit between — and it does not, because a produce inside an open transaction is itself
     * staged. The transaction is the log's staging mechanism exactly as the pending batch is the
     * store's, and neither becomes real until the same commit.
     *
     * @param mutations a {@code null} value is rejected rather than guessed at; deletion is
     *                  {@link Mutation#delete()}, which reaches the log as a value the caller encodes
     */
    public void stage(int partition, Map<K, Mutation> mutations) {
        Partition held = require(partition, true);
        Objects.requireNonNull(mutations, "mutations");
        if (mutations.isEmpty()) {
            return;
        }
        Staged pending = staged.computeIfAbsent(partition, id -> new Staged());
        for (Map.Entry<K, Mutation> entry : mutations.entrySet()) {
            K key = Objects.requireNonNull(entry.getKey(), "key");
            Mutation mutation = entry.getValue();
            if (mutation == null) {
                throw new NullPointerException("null value for key " + key
                        + "; use Mutation.delete(), which survives log compaction");
            }
            PendingPosition position = Objects.requireNonNull(
                    changelog.send(held.id, key, mutation), "the changelog returned no position");
            pending.positions.add(position);
            byte[] encoded = keyBytes.apply(key);
            if (mutation.isDelete()) {
                pending.batch.delete(encoded);
            } else {
                pending.batch.put(encoded, mutation.value());
            }
        }
    }

    /**
     * Commits the caller's transaction and then applies everything staged.
     *
     * <p>The order is the whole design and it cannot be got wrong by rearranging call sites, because
     * the two calls that could be misordered are one call. Everything the caller's transaction must
     * do belongs <em>inside</em> {@code action} — checkpointing input positions included — so that a
     * failure anywhere in it is classified rather than escaping as an unmapped exception that leaves
     * the transaction open and the batches staged.
     *
     * <p>On success, per partition: read the pending positions, apply the batch, then stamp the
     * maximum position as the watermark. The engine carries the watermark in the same memtable as the
     * writes it covers, so both become durable in one flush — a crash cannot leave a watermark ahead
     * of the state it claims.
     *
     * @throws AbortableNotCommitted the transaction did not commit; abort it
     * @throws OutcomeUnknown        it may have; do not abort, close the transport, and the staged
     *                               partitions are now behind
     * @throws ProducerDead          the transport is unusable; close it
     * @throws ApplyFailed           it committed and one or more applies did not
     */
    public void commit(CommitAction action) {
        Objects.requireNonNull(action, "action");
        try {
            action.commit();
        } catch (AbortableNotCommitted | ProducerDead definite) {
            discard();
            throw definite;
        } catch (OutcomeUnknown unknown) {
            markBehind(staged.keySet());
            discard();
            throw unknown;
        } catch (RuntimeException unclassified) {
            // Assuming "not committed" is the unsound direction: it licenses an abort that may not
            // be legal, and leaves a partition readable that the log may already be ahead of.
            markBehind(staged.keySet());
            discard();
            throw new OutcomeUnknown(
                    "the commit action threw an unclassified exception, so its outcome is unknown",
                    unclassified);
        }
        applyStaged();
    }

    /**
     * Drops whatever is staged. Idempotent, and a no-op after a commit that applied — which is what
     * lets a caller put it in a {@code finally} and stop reasoning about which paths need it.
     *
     * <p>It does <b>not</b> abort. Symmetry argues that it should, and that is wrong: after a commit
     * whose outcome is unknown, aborting is not merely unnecessary but illegal, and only the caller
     * can know which case it is in.
     */
    public void discard() {
        for (Staged pending : staged.values()) {
            pending.batch.close();
        }
        staged.clear();
    }

    /** Flushes and closes every partition still held. */
    @Override
    public void close() {
        discard();
        revoke(assignment());
    }

    // --- internals -----------------------------------------------------------

    /**
     * Applies every staged partition and aggregates the failures.
     *
     * <p>Attempting every partition is the correctness requirement, not tidiness. The transaction
     * covered all of them, so exiting on the first failure would leave the untried ones committed in
     * the log, unwritten locally, and still marked ready — which is precisely the state the invariant
     * exists to make unreachable. {@link ApplyFailed#partitions()} therefore means every partition
     * whose committed changelog may not be materialised.
     */
    private void applyStaged() {
        SortedSet<Integer> failed = new TreeSet<>();
        boolean terminal = false;
        RuntimeException first = null;
        try {
            for (Map.Entry<Integer, Staged> entry : staged.entrySet()) {
                int id = entry.getKey();
                Staged pending = entry.getValue();
                try {
                    Partition partition = partitions.get(id);
                    if (partition == null) {
                        throw new PartitionNotAssignedException(id);
                    }
                    partition.apply(pending.batch, maxPosition(pending.positions));
                } catch (RuntimeException failure) {
                    failed.add(id);
                    terminal |= !(failure instanceof RetryableException);
                    if (first == null) {
                        first = failure;
                    }
                }
            }
        } finally {
            discard();
        }
        if (!failed.isEmpty()) {
            markBehind(failed);
            throw new ApplyFailed(failed, terminal, first);
        }
    }

    private static long maxPosition(List<PendingPosition> positions) {
        if (positions.isEmpty()) {
            // Unreachable today — stage() returns before creating anything for an empty map — but the
            // fallback would be to stamp Long.MIN_VALUE as a watermark, which is the kind of quiet
            // catastrophe worth one branch.
            throw new IllegalStateException("a staged batch with no changelog positions");
        }
        long highest = Long.MIN_VALUE;
        for (PendingPosition position : positions) {
            highest = Math.max(highest, position.position());
        }
        return highest;
    }

    private void replay(Partition partition) {
        restore.restore(partition.id, partition.materializedThrough(), partition::applyRestored);
        partition.behind = false;
    }

    private void markBehind(Collection<Integer> ids) {
        for (int id : ids) {
            Partition partition = partitions.get(id);
            if (partition != null) {
                partition.behind = true;
            }
        }
    }

    private void dropStaged(int id) {
        Staged pending = staged.remove(id);
        if (pending != null) {
            pending.batch.close();
        }
    }

    private Partition require(int id, boolean mustBeReady) {
        Partition partition = partitions.get(id);
        if (partition == null) {
            throw new PartitionNotAssignedException(id);
        }
        if (mustBeReady && partition.behind) {
            throw new PartitionBehindException(id);
        }
        return partition;
    }

    /** One partition: its store, how far it has materialised, and whether it may be served. */
    private final class Partition {
        final int id;
        final ElysiumKV db;
        volatile boolean behind;
        long materialized;
        boolean hasWatermark;

        Partition(int id, ElysiumKV db) {
            this.id = id;
            this.db = db;
            OptionalLong recovered = db.recoveredWatermark();
            this.hasWatermark = recovered.isPresent();
            this.materialized = recovered.orElse(0L);
        }

        OptionalLong materializedThrough() {
            return hasWatermark ? OptionalLong.of(materialized) : OptionalLong.empty();
        }

        /** Writes, then stamps. A failure between the two leaves the store ahead of its watermark,
         * which replays harmlessly; the reverse would not. */
        void apply(WriteBatch batch, long through) {
            db.write(batch);
            db.setWatermark(through);
            record(through);
        }

        void applyRestored(long through, Map<K, Mutation> mutations) {
            if (hasWatermark && through < materialized) {
                throw new IllegalArgumentException("restore of partition " + id + " went backwards: "
                        + through + " is below the materialised position " + materialized);
            }
            Objects.requireNonNull(mutations, "mutations");
            try (WriteBatch batch = new WriteBatch()) {
                for (Map.Entry<K, Mutation> entry : mutations.entrySet()) {
                    K key = Objects.requireNonNull(entry.getKey(), "key");
                    Mutation mutation = entry.getValue();
                    if (mutation == null) {
                        throw new NullPointerException("null value for key " + key + " in a restore; "
                                + "a log tombstone cannot be replayed safely, so decode it into "
                                + "Mutation.delete() or fail");
                    }
                    byte[] encoded = keyBytes.apply(key);
                    if (mutation.isDelete()) {
                        batch.delete(encoded);
                    } else {
                        batch.put(encoded, mutation.value());
                    }
                }
                db.write(batch);
            }
            db.setWatermark(through);
            record(through);
        }

        private void record(long through) {
            materialized = through;
            hasWatermark = true;
        }
    }

    /** A partition's pending writes, and where each of their log records landed. */
    private static final class Staged {
        final WriteBatch batch = new WriteBatch();
        final List<PendingPosition> positions = new ArrayList<>();
    }

    /** @param <K> the key type */
    public static final class Builder<K> {
        private IntFunction<ElysiumKVOptions> options;
        private Function<K, byte[]> keyBytes;
        private Changelog<K> changelog;
        private Restore<K> restore;

        /** Fresh options per partition: an {@link ElysiumKVOptions} is consumed by one open. */
        public Builder<K> options(IntFunction<ElysiumKVOptions> options) {
            this.options = Objects.requireNonNull(options, "options");
            return this;
        }

        /** How a key becomes store bytes. The key type supplies equality; this supplies order. */
        public Builder<K> keyBytes(Function<K, byte[]> keyBytes) {
            this.keyBytes = Objects.requireNonNull(keyBytes, "keyBytes");
            return this;
        }

        public Builder<K> changelog(Changelog<K> changelog) {
            this.changelog = Objects.requireNonNull(changelog, "changelog");
            return this;
        }

        public Builder<K> restore(Restore<K> restore) {
            this.restore = Objects.requireNonNull(restore, "restore");
            return this;
        }

        public PartitionedStore<K> build() {
            Objects.requireNonNull(options, "options");
            Objects.requireNonNull(keyBytes, "keyBytes");
            Objects.requireNonNull(changelog, "changelog");
            Objects.requireNonNull(restore, "restore");
            return new PartitionedStore<>(this);
        }
    }
}
