package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.BatchedIterator;
import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.ElysiumKVStats;
import io.veridia.elysiumkv.OpenResult;
import io.veridia.elysiumkv.RetryableException;
import io.veridia.elysiumkv.WriteBatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * A partitioned local store backed by a log the caller already replays.
 *
 * <blockquote>The log is authoritative. The store is derived. The store may only ever lag — and a
 * lagging partition may not be served.</blockquote>
 *
 * <p>The first clause is structural: {@link #put} does not write. It stages, and
 * {@link #applyCommitted} applies the staged set as one batch after the caller's transaction has
 * committed, and no other path into the store exists. A process dying in between leaves the store
 * behind the log, which is the recoverable direction — the watermark did not advance, so the next
 * restore replays what was missed.
 *
 * <p>The second clause is why every read and {@link #put} reject a {@linkplain #behind() behind}
 * partition. Serving one would let a value derived from stale state be produced back into the log and
 * committed, at which point no restore can repair it: the wrong value is in the log. The second
 * clause is what keeps the first true.
 *
 * <pre>
 *    READY ---- an apply fails, or a commit outcome is unknown ----&gt; BEHIND
 *      ^                                                              |
 *      +--------------- repair() from the unchanged watermark --------+
 * </pre>
 *
 * <p>Which of the three outcomes applies is the caller's to say, in one call each —
 * {@link #applyCommitted}, {@link #discard}, {@link #discardUnknown} — rather than one callback,
 * because a container that owns the transaction offers only an after-commit hook and there is no
 * position in which to wrap its commit. {@link #commit} rebuilds the single-call form for callers
 * that do own their producer.
 *
 * <p>{@link #put} and {@link #deleteRange} are named for what they mean rather than for when they
 * happen: neither writes. Both stage, and {@link #applyCommitted} is what makes them real. {@link #get}
 * and the four scans fold the staged set in, so a second fold over the same key sees the first one's
 * output.
 *
 * <p>The methods that name the committed view — {@link #getCommitted} and the batched scans — do not,
 * and they exist for a reader outside the fold. An interactive query wants exactly that: a row this
 * transaction staged belongs to a transaction that may abort, so serving it would publish state that
 * never existed. They take no part in the staged set, so a query thread neither sees it nor waits on
 * a thread writing it.
 *
 * <p>Two checkpoints, and they never meet. The input checkpoint is the caller's and travels in its
 * transaction; the state checkpoint is this component's, taken from the {@link PendingPosition} each
 * changelog send returns, with the per-partition maximum becoming that partition's watermark after a
 * successful commit. There is one state record per changed key rather than per input record, so none
 * of the outcome calls takes a position and the caller never handles the second.
 *
 * <p>Threading, in two halves. {@link #get}, {@link #put} and {@link #deleteRange} may be called
 * from several threads at once, which is what lets a fold parallelise over its keys: each partition
 * carries a read-write lock, held for writing across a changelog send and the staging that records it
 * so that the order two threads reach the log is the order the store applies, and held for reading by
 * every read so that a point mutation and a range delete cannot be seen half-applied. Threads staging
 * into different partitions never contend. A {@link Changelog} send runs under that write lock and
 * may read its own partition from the same thread, which {@link Changelog} states as a guarantee.
 *
 * <p>The other half is exclusive, and unchecked. {@link #begin}, {@link #applyCommitted},
 * {@link #commit}, {@link #discard}, {@link #discardUnknown} and {@link #close} are transaction
 * boundaries: they resolve everything staged, so the caller must have joined its parallel phase
 * before calling one. {@link #assign}, {@link #repair}, {@link #revoke} and {@link #lost} are the
 * same. A {@link StagedIterator} belongs to the thread that created it, as the engine's own iterators
 * do. {@link #behind()}, {@link #assignment()} and {@link #stats()} are safe from any thread.
 *
 * @param <K> the key type. Not {@code byte[]}: Java arrays use identity equality, so two separately
 *            deserialised arrays holding the same bytes are different map keys.
 */
public final class PartitionedStore<K> implements AutoCloseable {

    private final IntFunction<ElysiumKVOptions> options;
    private final Function<K, byte[]> keyBytes;
    private final Changelog<K> changelog;
    private final Restore<K> restore;

    /** Assigned partitions, sorted so that iteration order is the partition order. */
    private final Map<Integer, Partition> partitions = Collections.synchronizedMap(new TreeMap<>());

    /**
     * What {@link #put} has accumulated since the last commit or discard, in partition order.
     *
     * <p>Concurrent because several threads may stage at once; ordered because {@link #applyCommitted}
     * walks it. Each entry's contents are guarded by that partition's {@code stageLock}.
     */
    private final ConcurrentNavigableMap<Integer, StagedOverlay> staged = new ConcurrentSkipListMap<>();

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
                //
                // Attached rather than substituted, because this partition is not in `partitions`
                // and never will be: a close that throws would otherwise replace the restore's
                // failure — the one the caller can act on — with its own consequence, and leave
                // nothing to say which partition it belonged to.
                try {
                    db.close();
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
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
     * What each held partition is doing, for an embedder's instruments.
     *
     * <p>{@code materializedThrough} is the one to graph. The design's invariant is that a
     * store may only ever lag the log; against the log's end offset this says by how much, which is
     * the difference between believing that and showing it.
     *
     * <p>A partition whose stats cannot be read is omitted rather than reported as zero: this runs
     * on whatever thread collects metrics while another may be closing the store, and a snapshot
     * that arrives a moment too late is worth nothing.
     */
    public Map<Integer, PartitionStats> stats() {
        List<Partition> held;
        synchronized (partitions) {
            held = new ArrayList<>(partitions.values());
        }
        // Outside the lock: stats() is O(files), and holding it would block a rebalance.
        Map<Integer, PartitionStats> out = new TreeMap<>();
        for (Partition partition : held) {
            try {
                out.put(partition.id, new PartitionStats(partition.id, partition.behind,
                        partition.materializedThrough(), partition.db.stats()));
            } catch (RuntimeException closing) {
                // Skipped, deliberately — see above.
            }
        }
        return out;
    }

    /** One partition's position and the engine's own counters underneath it. */
    public static final class PartitionStats {
        private final int partition;
        private final boolean behind;
        private final OptionalLong materializedThrough;
        private final ElysiumKVStats engine;

        PartitionStats(int partition, boolean behind, OptionalLong materializedThrough,
                       ElysiumKVStats engine) {
            this.partition = partition;
            this.behind = behind;
            this.materializedThrough = materializedThrough;
            this.engine = engine;
        }

        public int partition() {
            return partition;
        }

        /** True while this partition may lag the log, so it is not served. */
        public boolean behind() {
            return behind;
        }

        /** The changelog offset this store has materialised, empty before the first apply. */
        public OptionalLong materializedThrough() {
            return materializedThrough;
        }

        public ElysiumKVStats engine() {
            return engine;
        }
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
            staged.remove(id);
            closeFlushing(partition.db);
        }
    }

    /**
     * Flushes and closes, and closes even when the flush fails.
     *
     * <p>The flush is the fallible half — it writes to the durable tier and takes the manifest's
     * compare-and-set, so a fenced writer or an object-store error surfaces here. Letting that
     * failure skip the close is not a smaller problem than the failure: this partition has already
     * been removed from {@code partitions}, so nothing can reach the database again and no caller
     * can close it. Its compaction and migration threads keep running against handles the caller is
     * entitled to release the moment this returns, and the only way to reclaim them becomes ending
     * the process.
     *
     * <p>Closing without the flush is safe in a way that is specific to this class: a partitioned
     * store is fed from a log, and the durable watermark covers only what was flushed — so
     * everything the memtable still holds is replayed from that watermark by whoever takes the
     * partition next. Losing it costs a longer replay, not a value.
     *
     * <p>The flush failure is what propagates; a close that also fails is attached to it. The caller
     * needs to know the flush did not happen, and the close is the consequence, not the cause.
     */
    private static void closeFlushing(ElysiumKV db) {
        try {
            db.flush();
        } catch (RuntimeException | Error failure) {
            try {
                db.closeWithoutFlush();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        db.close();
    }

    /**
     * Closes each partition without flushing, because another instance may already hold it.
     *
     * <p>Not the same as {@link #revoke}: flushing into a store someone else has opened is something
     * the engine's compare-and-set would catch, but catching it is worse than not doing it. Whatever
     * had not reached disk is redelivered from the log to whoever owns it now.
     */
    public void lost(Collection<Integer> ids) {
        Throwable lostFailure = null;
        for (int id : new TreeSet<>(Objects.requireNonNull(ids, "ids"))) {
            Partition partition = partitions.remove(id);
            if (partition == null) {
                continue;
            }
            staged.remove(id);
            try {
                partition.db.closeWithoutFlush();
            } catch (RuntimeException | Error failure) {
                // One partition's failure must not abandon the rest: every one of them is already
                // out of `partitions`, so a loop that stops here leaves the remainder unreachable
                // and unclosed. Collected and thrown together once every partition has had its turn.
                if (lostFailure == null) {
                    lostFailure = failure;
                } else {
                    lostFailure.addSuppressed(failure);
                }
            }
        }
        // Rethrown by kind, because a precise rethrow does not survive being stored in a variable.
        if (lostFailure instanceof RuntimeException) throw (RuntimeException) lostFailure;
        if (lostFailure instanceof Error) throw (Error) lostFailure;
    }

    // --- reading -------------------------------------------------------------

    /**
     * Reads this transaction's view of a key: what it has staged for that key, or committed state
     * where it has staged nothing.
     *
     * <p>Read-your-writes, and that is the point rather than a convenience. Two input records in one
     * transaction may touch the same key, and the second fold has to see the first's output — reading
     * committed state there would produce the pre-transaction value, derive an update from it and
     * commit that to the log, which is a lost update no restore can repair.
     *
     * @return the value, or {@code null} where the key is absent — and only then. Always a copy, so
     *         a staged read and a committed read give the caller the same ownership
     */
    public byte[] get(int partition, K key) {
        Partition held = require(partition, true);
        byte[] encoded = keyBytes.apply(Objects.requireNonNull(key, "key"));
        StagedOverlay pending = staged.get(partition);
        if (pending != null) {
            held.stageLock.readLock().lock();
            try {
                StagedOverlay.Resolution resolved = pending.resolve(encoded);
                if (resolved.staged) {
                    return resolved.value;
                }
            } finally {
                held.stageLock.readLock().unlock();
            }
        }
        return held.db.getCopy(encoded);
    }

    /**
     * Committed state for one key, ignoring whatever this transaction has staged — for a reader
     * outside the fold, where staged rows are the wrong answer because the transaction holding them
     * may still abort.
     *
     * <p>Takes no part in the staged set: it neither sees it nor contends with a thread writing it, so
     * it is safe from any thread and never waits on a fold. It still rejects an unassigned or
     * {@linkplain #behind() behind} partition — the invariant's second clause is about who may be
     * served, and a query is being served.
     *
     * @return the value, or {@code null} where the key is absent — and only then
     */
    public byte[] getCommitted(int partition, K key) {
        Partition held = require(partition, true);
        return held.db.getCopy(keyBytes.apply(Objects.requireNonNull(key, "key")));
    }

    // --- scanning ------------------------------------------------------------

    /**
     * Half-open range scan of this transaction's view; null bounds are unbounded.
     *
     * <p>Bounds are store bytes rather than {@code K}, because a bound need not be a whole key. So are
     * the keys the scan delivers: the committed side supplies bytes and this component holds no
     * decoder.
     */
    public StagedIterator iterator(int partition, byte[] lo, byte[] hi) {
        Partition held = require(partition, true);
        return new StagedIterator(held.db.iterator(lo, hi), snapshot(held, lo, hi, false), false);
    }

    /** Prefix scan of this transaction's view. */
    public StagedIterator prefixIterator(int partition, byte[] prefix) {
        Partition held = require(partition, true);
        Objects.requireNonNull(prefix, "prefix");
        return new StagedIterator(held.db.prefixIterator(prefix),
                prefixSnapshot(held, prefix, false), false);
    }

    /**
     * The same two scans, descending. Bounds keep their forward meaning — {@code lo} inclusive,
     * {@code hi} exclusive — so a direction change reorders the answer without changing it.
     */
    public StagedIterator reverseIterator(int partition, byte[] lo, byte[] hi) {
        Partition held = require(partition, true);
        return new StagedIterator(held.db.reverseIterator(lo, hi), snapshot(held, lo, hi, true),
                true);
    }

    /** Prefix scan of this transaction's view, descending. */
    public StagedIterator reversePrefixIterator(int partition, byte[] prefix) {
        Partition held = require(partition, true);
        Objects.requireNonNull(prefix, "prefix");
        return new StagedIterator(held.db.reversePrefixIterator(prefix),
                prefixSnapshot(held, prefix, true), true);
    }

    /**
     * Half-open range scan of committed state, batched; null bounds are unbounded. The bulk path, and
     * the one for a query served from outside the fold. Borrows the engine's own batches rather than
     * copying, which a staged scan cannot — there is no merge here to draw from two sources. Close it.
     *
     * <p>Not a snapshot, and which half is fixed matters. The file set is taken when the scan is
     * created, so compaction, migration and a truncation cannot disturb it. The memtable is live: a
     * key deleted before the scan reaches it is skipped, one already yielded stays yielded, and a scan
     * running while {@link #applyCommitted} writes can observe part of that batch. A concurrent range
     * delete is invisible, its tombstones having been copied when the scan started, where a point
     * delete may not be — so the two shapes of eviction do not look alike to a scan in flight.
     */
    public BatchedIterator committedBatchedIterator(int partition, byte[] lo, byte[] hi) {
        return require(partition, true).db.batchedIterator(lo, hi);
    }

    /** Prefix scan of committed state, batched. Same guarantees as {@link #committedBatchedIterator}. */
    public BatchedIterator committedBatchedPrefixIterator(int partition, byte[] prefix) {
        return require(partition, true).db
                .batchedPrefixIterator(Objects.requireNonNull(prefix, "prefix"));
    }

    /** The same batched range scan, descending. Bounds keep their forward meaning. */
    public BatchedIterator committedBatchedReverseIterator(int partition, byte[] lo, byte[] hi) {
        return require(partition, true).db.batchedReverseIterator(lo, hi);
    }

    /** The same batched prefix scan, descending. */
    public BatchedIterator committedBatchedReversePrefixIterator(int partition, byte[] prefix) {
        return require(partition, true).db
                .batchedReversePrefixIterator(Objects.requireNonNull(prefix, "prefix"));
    }

    private StagedSnapshot snapshot(Partition held, byte[] lo, byte[] hi, boolean reverse) {
        StagedOverlay pending = staged.get(held.id);
        if (pending == null) {
            return StagedSnapshot.EMPTY;
        }
        held.stageLock.readLock().lock();
        try {
            return pending.snapshot(lo, hi, reverse);
        } finally {
            held.stageLock.readLock().unlock();
        }
    }

    private StagedSnapshot prefixSnapshot(Partition held, byte[] prefix, boolean reverse) {
        StagedOverlay pending = staged.get(held.id);
        if (pending == null) {
            return StagedSnapshot.EMPTY;
        }
        held.stageLock.readLock().lock();
        try {
            return pending.prefixSnapshot(prefix, reverse);
        } finally {
            held.stageLock.readLock().unlock();
        }
    }

    // --- the transaction -----------------------------------------------------

    /**
     * Enqueues the changelog sends and appends to this partition's pending batch. Writes
     * nothing.
     *
     * <p>Sending on stage looks like it breaks the invariant — the log written before the store, with
     * no commit between — and it does not, because a produce inside an open transaction is itself
     * staged. The transaction is the log's staging mechanism exactly as the pending batch is the
     * store's, and neither becomes real until the same commit.
     *
     * @param mutations a {@code null} value is rejected rather than guessed at; deletion is
     *                  {@link Mutation#delete()}, which reaches the log as a value the caller encodes
     */
    public void put(int partition, Map<K, Mutation> mutations) {
        Partition held = require(partition, true);
        Objects.requireNonNull(mutations, "mutations");
        if (mutations.isEmpty()) {
            return;
        }
        StagedOverlay pending = staged.computeIfAbsent(partition, id -> new StagedOverlay());
        // One critical section for the whole map, so its records are contiguous in the log and it
        // lands as a unit against a concurrent stage.
        held.stageLock.writeLock().lock();
        try {
            for (Map.Entry<K, Mutation> entry : mutations.entrySet()) {
                K key = Objects.requireNonNull(entry.getKey(), "key");
                Mutation mutation = entry.getValue();
                if (mutation == null) {
                    throw new NullPointerException("null value for key " + key
                            + "; use Mutation.delete(), which survives log compaction");
                }
                PendingPosition position = Objects.requireNonNull(
                        changelog.send(held.id, key, mutation), "the changelog returned no position");
                pending.put(keyBytes.apply(key), mutation, position);
            }
        } finally {
            held.stageLock.writeLock().unlock();
        }
    }

    /**
     * Enqueues one changelog record deleting every key in {@code [lower, upper)}, and appends the
     * range to this partition's pending batch. Writes nothing.
     *
     * <p>One record rather than a tombstone per key, so the changelog's codec has to carry a range
     * record: {@link Changelog#sendDeleteRange} and {@link WriteSink#deleteRange} are the two
     * halves, and a codec that implements neither fails here, before the commit, where a failure is
     * still abortable. The record must survive compaction under a log key nothing later supersedes —
     * a rebuild that dropped it would resurrect every key the range covered.
     *
     * <p>The range lands in the same batch as the point mutations around it and in the order it was
     * staged, so a put staged afterwards survives the delete and one staged before it does not.
     *
     * <p>Bounds are store bytes rather than {@code K}, because a bound need not be a whole key. An
     * empty or inverted range stages nothing and is not an error, exactly as a scan over those bounds
     * yields nothing.
     */
    public void deleteRange(int partition, byte[] lower, byte[] upper) {
        Partition held = require(partition, true);
        Objects.requireNonNull(lower, "lower");
        Objects.requireNonNull(upper, "upper");
        if (StagedOverlay.BY_KEY.compare(lower, upper) >= 0) {
            return;
        }
        // Copied once and shared with the changelog, so a caller reusing its bound arrays cannot
        // change what applies after the record has been sent.
        byte[] from = lower.clone();
        byte[] to = upper.clone();
        StagedOverlay pending = staged.computeIfAbsent(partition, id -> new StagedOverlay());
        held.stageLock.writeLock().lock();
        try {
            PendingPosition position = Objects.requireNonNull(
                    changelog.sendDeleteRange(held.id, from, to), "the changelog returned no position");
            pending.deleteRange(from, to, position);
        } finally {
            held.stageLock.writeLock().unlock();
        }
    }

    /**
     * Marks the start of a transaction — the one call that makes a two-hook container safe.
     *
     * <p>Anything still staged here belonged to a transaction that never resolved, which happens
     * only when the caller could not establish the commit outcome: a container fires its
     * after-commit hook only on success and will not roll back what it could not commit, so both of
     * the usual hooks stay silent. This calls {@link #discardUnknown} for it, marking those
     * partitions behind before this transaction reads anything.
     *
     * <p>Optional, and a no-op when nothing is staged. Precondition: a real transaction boundary —
     * calling it mid-batch discards work in progress, which is safe, since the log still holds it,
     * but costs a repair.
     */
    public void begin() {
        if (!staged.isEmpty()) {
            discardUnknown();
        }
    }

    /**
     * The log committed: make it real. Applies every staged batch and advances each
     * partition's watermark to the highest position its changelog records reached.
     *
     * <p>This and the two {@code discard} methods are one call per outcome the caller can be in,
     * separate so the caller need not own the commit — a transaction manager committing on the
     * application's behalf offers an after-commit hook and nothing else:
     *
     * <pre>{@code
     * transactionHook.register(store::applyCommitted, store::discard);
     * }</pre>
     *
     * <p>Per partition: read the pending positions, apply the batch, then stamp the maximum position
     * as the watermark. The engine carries the watermark in the same memtable as the writes it
     * covers, so a crash cannot leave a watermark ahead of the state it claims.
     *
     * <p>Precondition: the log committed. Calling this otherwise puts the store ahead of the log,
     * which nothing can detect or repair — {@link #commit} exists for callers who do own their
     * transaction.
     *
     * <p>Every staged partition is attempted and the failures aggregated, which is a correctness
     * requirement: the transaction covered all of them, so exiting on the first failure would leave
     * the untried ones committed in the log, unwritten locally, and still marked ready.
     * {@link ApplyFailed#partitions()} is therefore every partition whose committed changelog may
     * not be materialised.
     *
     * @throws ApplyFailed one or more partitions may not hold what was committed for them
     */
    public void applyCommitted() {
        SortedSet<Integer> failed = new TreeSet<>();
        boolean terminal = false;
        RuntimeException first = null;
        requireQuiet("applyCommitted");
        try {
            for (Map.Entry<Integer, StagedOverlay> entry : staged.entrySet()) {
                int id = entry.getKey();
                StagedOverlay pending = entry.getValue();
                try {
                    Partition partition = partitions.get(id);
                    if (partition == null) {
                        throw new PartitionNotAssignedException(id);
                    }
                    // Under the write lock: the apply reads the overlay and then it is dropped, which
                    // makes this a writer under the same discipline as staging.
                    partition.stageLock.writeLock().lock();
                    try {
                        partition.apply(pending);
                    } finally {
                        partition.stageLock.writeLock().unlock();
                    }
                } catch (RuntimeException failure) {
                    failed.add(id);
                    terminal |= !(failure instanceof RetryableException);
                    if (first == null) {
                        first = failure;
                    }
                }
            }
        } finally {
            dropStaged();
        }
        if (!failed.isEmpty()) {
            markBehind(failed);
            throw new ApplyFailed(failed, terminal, first);
        }
    }

    /**
     * Commits through a callback the caller supplies, then applies — for callers that own their
     * transaction rather than delegating it to a container.
     *
     * <p>Sugar over {@link #applyCommitted}, {@link #discard} and {@link #discardUnknown} that makes
     * the ordering structural. Everything the caller's transaction must do belongs <em>inside</em>
     * {@code action} — checkpointing input positions included — so that a failure anywhere in it is
     * classified rather than escaping as an unmapped exception that leaves the transaction open and
     * the batches staged.
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
            discardUnknown();
            throw unknown;
        } catch (RuntimeException unclassified) {
            // Assuming "not committed" is the unsound direction: it licenses an abort that may not
            // be legal, and leaves a partition readable that the log may already be ahead of.
            discardUnknown();
            throw new OutcomeUnknown(
                    "the commit action threw an unclassified exception, so its outcome is unknown",
                    unclassified);
        }
        applyCommitted();
    }

    /**
     * Drops whatever is staged. Idempotent, and a no-op after a commit that applied — which is what
     * lets a caller put it in a {@code finally} and stop reasoning about which paths need it.
     *
     * <p>The log definitely did not commit. Nothing was applied and no watermark moved, so
     * every partition stays readable. Use {@link #discardUnknown} when that is not established.
     *
     * <p>It does not abort. Symmetry argues that it should, and that is wrong: after a commit
     * whose outcome is unknown, aborting is not merely unnecessary but illegal, and only the caller
     * can know which case it is in.
     */
    public void discard() {
        requireQuiet("discard");
        dropStaged();
    }

    /**
     * Drops the staged set without checking or locking, for the paths that must not fail: the
     * {@code finally} of an apply, where a refusal would mask the failure being reported, and
     * {@link #close}, which has to release the partitions whatever the caller's threads are doing.
     */
    private void dropStaged() {
        staged.clear();
    }

    /**
     * The log may or may not have committed. Drops what is staged and marks those partitions
     * {@linkplain #behind() behind}, because the transaction may have carried records the store does
     * not hold and serving them would fold new input against state that is missing a committed
     * update.
     *
     * <p>This is the case a container-driven commit makes easy to miss: a commit that times out
     * fires neither the after-commit nor the after-rollback hook, so a caller wiring only those two
     * leaves a staged batch behind and a partition readable that may already lag. Whatever position
     * the transaction manager surfaces that from, it belongs here.
     */
    public void discardUnknown() {
        requireQuiet("discardUnknown");
        markBehind(staged.keySet());
        dropStaged();
    }

    /**
     * Refuses a transaction boundary while a thread is still reading or staging, which is a caller
     * that has not joined its parallel phase.
     *
     * <p>Read from the lock's own counters, so it costs nothing on the paths it guards. Best-effort by
     * nature — a thread can begin a moment after the check — and worth having anyway, because the
     * failure it replaces is a write that quietly never lands.
     */
    private void requireQuiet(String boundary) {
        for (Integer id : staged.keySet()) {
            Partition partition = partitions.get(id);
            if (partition == null) {
                continue;
            }
            if (partition.stageLock.isWriteLocked() || partition.stageLock.getReadLockCount() > 0) {
                throw new IllegalStateException(boundary + " ran while partition " + id
                        + " was still being read or staged; join the parallel phase before a"
                        + " transaction boundary");
            }
        }
    }

    /** Flushes and closes every partition still held. */
    @Override
    public void close() {
        dropStaged();
        revoke(assignment());
    }

    // --- internals -----------------------------------------------------------

    private void replay(Partition partition) {
        restore.restore(partition.id, partition.materializedThrough(), partition);
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
    private final class Partition implements WriteSink<K> {
        final int id;
        final ElysiumKV db;
        volatile boolean behind;

        /**
         * Held for writing across a changelog send and the staging it records, and for reading by
         * every read. Both halves are load-bearing. Without the write half two threads staging the
         * same key can land in the overlay in the opposite order to the one their records took in the
         * log, and the store then applies a value the log says was superseded. Without the read half
         * a read can see a point mutation and miss a range delete staged between the two lookups that
         * resolve it, and answer with a value that never existed.
         */
        final ReentrantReadWriteLock stageLock = new ReentrantReadWriteLock();

        /** The materialised position, or -1 for none — one field, because two cannot be read atomically. */
        private volatile long materialized;

        Partition(int id, ElysiumKV db) {
            this.id = id;
            this.db = db;
            this.materialized = db.recoveredWatermark().orElse(-1L);
        }

        OptionalLong materializedThrough() {
            long through = materialized;
            return through < 0 ? OptionalLong.empty() : OptionalLong.of(through);
        }

        /**
         * Writes, then stamps. A failure between the two leaves the store ahead of its watermark,
         * which replays harmlessly; the reverse would not.
         *
         * <p>The position is read before anything is written, so a changelog that cannot resolve one
         * fails with the store untouched.
         */
        void apply(StagedOverlay pending) {
            long through = pending.maxPosition();
            try (WriteBatch batch = new WriteBatch()) {
                pending.into(batch);
                db.write(batch);
            }
            db.setWatermark(through);
            record(through);
        }

        @Override
        public void putBatch(long through, Map<K, Mutation> mutations) {
            requireForward(through);
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

        @Override
        public void deleteRange(long through, byte[] lower, byte[] upper) {
            requireForward(through);
            db.deleteRange(Objects.requireNonNull(lower, "lower"),
                    Objects.requireNonNull(upper, "upper"));
            db.setWatermark(through);
            record(through);
        }

        private void requireForward(long through) {
            if (materialized >= 0 && through < materialized) {
                throw new IllegalArgumentException("restore of partition " + id + " went backwards: "
                        + through + " is below the materialised position " + materialized);
            }
        }

        private void record(long through) {
            materialized = through;
        }
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
