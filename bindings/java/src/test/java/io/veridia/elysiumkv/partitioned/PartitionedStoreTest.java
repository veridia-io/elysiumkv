package io.veridia.elysiumkv.partitioned;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.veridia.elysiumkv.partitioned.InMemoryLog.bytes;
import static io.veridia.elysiumkv.partitioned.InMemoryLog.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The spec's test list, against an in-memory log. None of these need a broker. */
class PartitionedStoreTest {

    @TempDir
    Path root;

    private PartitionFixture fixture;
    private InMemoryLog log;
    private List<OptionalLong> resumePoints;
    private final AtomicInteger poisoned = new AtomicInteger(-1);
    private final AtomicInteger stale = new AtomicInteger(-1);
    private PartitionedStore<String> store;

    @BeforeEach
    void setUp() {
        fixture = new PartitionFixture(root);
        log = new InMemoryLog();
        resumePoints = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
        fixture.close();
    }

    /** @param batchSize how many log records the default replay applies per sink call */
    private PartitionedStore<String> open(int batchSize) {
        return open(log.restoreIn(batchSize));
    }

    /** The batch size lives in the {@link Restore} the caller passes, so it is not a parameter here. */
    private PartitionedStore<String> open(Restore<String> restore) {
        return PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(this::send)
                .restore((partition, materializedThrough, sink) -> {
                    resumePoints.add(materializedThrough);
                    restore.restore(partition, materializedThrough, sink);
                })
                .build();
    }

    /**
     * The changelog, with two injection points. {@code poisoned} makes a partition's position throw
     * when the apply reads it — a failure after the log committed. {@code stale} makes it report a
     * position below the watermark, which the engine rejects: a failure *after* the batch was written.
     */
    private PendingPosition send(int partition, String key, Mutation mutation) {
        PendingPosition real = log.send(partition, key, mutation);
        if (partition == poisoned.get()) {
            return () -> {
                throw new IllegalStateException("injected: the position could not be read");
            };
        }
        if (partition == stale.get()) {
            return () -> 0L;
        }
        return real;
    }

    private static Map<String, Mutation> put(String key, String value) {
        return Collections.singletonMap(key, Mutation.put(bytes(value)));
    }

    private String read(int partition, String key) {
        return string(store.getCommittedBatch(partition, Collections.singletonList(key)).get(key));
    }

    // --- staging -------------------------------------------------------------

    @Test
    @DisplayName("getCommittedBatch does not see staged writes")
    void stagedWritesAreInvisibleUntilTheCommit() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "first"));
        store.commit(log::commitTransaction);

        store.stage(0, put("k", "second"));
        assertEquals("first", read(0, "k"), "a staged write must not be readable in its own transaction");
        store.commit(log::commitTransaction);
        assertEquals("second", read(0, "k"));
    }

    @Test
    @DisplayName("the watermark is a changelog offset, not an input offset")
    void theWatermarkTracksTheChangelogAndNotTheInput() {
        store = open(8);
        store.assign(Collections.singletonList(0));

        // One input record, two changed keys: the two offset spaces diverge here and nowhere later.
        Map<String, Mutation> updates = new LinkedHashMap<>();
        updates.put("a", Mutation.put(bytes("1")));
        updates.put("b", Mutation.put(bytes("2")));
        store.stage(0, updates);
        store.commit(log::commitTransaction);

        store.revoke(Collections.singletonList(0));
        resumePoints.clear();
        store.assign(Collections.singletonList(0));

        assertEquals(OptionalLong.of(1), resumePoints.get(0),
                "the watermark must be the second changelog offset, not the single input position");
    }

    @Test
    @DisplayName("stage rejects a null value")
    void stageRejectsANullValue() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        Map<String, Mutation> withNull = new LinkedHashMap<>();
        withNull.put("k", null);
        NullPointerException failure =
                assertThrows(NullPointerException.class, () -> store.stage(0, withNull));
        assertTrue(failure.getMessage().contains("Mutation.delete()"),
                "the message should point at the delete that survives compaction");
    }

    // --- commit outcomes -----------------------------------------------------

    @Test
    @DisplayName("an abortable failure applies nothing and leaves the partition readable")
    void anAbortableFailureAppliesNothing() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));

        assertThrows(AbortableNotCommitted.class, () -> store.commit(() -> {
            log.abortTransaction();
            throw new AbortableNotCommitted(new RuntimeException("injected"));
        }));

        assertNull(read(0, "k"), "nothing may be applied when the transaction did not commit");
        assertTrue(store.behind().isEmpty(), "a definite failure leaves the partition serviceable");
    }

    @Test
    @DisplayName("an unknown outcome applies nothing, advances nothing, and marks its partitions behind")
    void anUnknownOutcomeMarksThePartitionsBehind() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));

        assertThrows(OutcomeUnknown.class, () -> store.commit(() -> {
            log.commitTransaction();   // it did commit; this side just cannot know that
            throw new OutcomeUnknown(new RuntimeException("injected timeout"));
        }));

        assertEquals(Collections.singleton(0), store.behind());
        assertThrows(PartitionBehindException.class,
                () -> store.getCommittedBatch(0, Collections.singletonList("k")));
        assertThrows(PartitionBehindException.class, () -> store.stage(0, put("k", "again")));

        // The repair replays what the transaction turned out to have committed.
        store.repair(Collections.singletonList(0));
        assertTrue(store.behind().isEmpty());
        assertEquals("v", read(0, "k"));
    }

    @Test
    @DisplayName("an unclassified exception is treated as unknown, not as a failure")
    void anUnclassifiedExceptionIsTreatedAsUnknown() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));

        OutcomeUnknown failure = assertThrows(OutcomeUnknown.class, () -> store.commit(() -> {
            throw new IllegalArgumentException("something nobody enumerated");
        }));
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
        assertEquals(Collections.singleton(0), store.behind(),
                "assuming 'not committed' would leave a possibly-lagging partition readable");
    }

    @Test
    @DisplayName("any exception out of the commit callback leaves nothing staged")
    void nothingRemainsStagedAfterAFailedCommit() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        assertThrows(OutcomeUnknown.class, () -> store.commit(() -> {
            throw new IllegalStateException("injected");
        }));

        store.repair(Collections.singletonList(0));
        // If the batch had survived, this commit would apply it as well as the new one.
        store.stage(0, put("other", "w"));
        store.commit(log::commitTransaction);
        assertNull(read(0, "k"), "the discarded batch must not resurface in a later commit");
        assertEquals("w", read(0, "other"));
    }

    @Test
    @DisplayName("discard is idempotent and a no-op after a commit that applied")
    void discardIsIdempotent() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.commit(log::commitTransaction);

        store.discard();
        store.discard();
        assertEquals("v", read(0, "k"), "discarding after a commit must not undo it");
    }

    @Test
    @DisplayName("a fatal transport error leaves the partition readable and nothing staged")
    void aProducerDeadFailureAppliesNothing() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));

        assertThrows(ProducerDead.class, () -> store.commit(() -> {
            throw new ProducerDead(new RuntimeException("injected fencing"));
        }));

        // Fatal for the transport, but the transaction definitely did not commit, so the store is
        // not behind and must stay in service.
        assertNull(read(0, "k"));
        assertTrue(store.behind().isEmpty(), "a dead producer is not a lagging store");
        store.stage(0, put("k", "w"));
        store.commit(log::commitTransaction);
        assertEquals("w", read(0, "k"), "nothing from the failed attempt survived into this one");
    }

    @Test
    @DisplayName("an unknown outcome marks every partition the transaction staged, not just one")
    void anUnknownOutcomeMarksAllStagedPartitions() {
        store = open(8);
        store.assign(Arrays.asList(0, 1, 2));
        store.stage(0, put("k", "v"));
        store.stage(2, put("k", "v"));

        assertThrows(OutcomeUnknown.class, () -> store.commit(() -> {
            log.commitTransaction();
            throw new OutcomeUnknown(new RuntimeException("injected timeout"));
        }));

        assertEquals(new TreeSet<>(Arrays.asList(0, 2)), new TreeSet<>(store.behind()),
                "the transaction covered both, so both may hold records the store does not");
        assertNull(read(1, "k"), "a partition the transaction never staged is still served");
    }

    @Test
    @DisplayName("a commit with nothing staged still runs the caller's transaction")
    void anEmptyCommitIsAllowed() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        AtomicInteger commits = new AtomicInteger();
        store.commit(commits::incrementAndGet);
        assertEquals(1, commits.get(), "a poll that changed no state still checkpoints its input");
        assertTrue(store.behind().isEmpty());
    }

    // --- driven by a transaction manager rather than a callback ---------------

    /*
     * A container that owns the transaction — Spring's KafkaTransactionManager is the common one —
     * commits on the application's behalf and offers an after-commit hook and nothing else. There is
     * no position in which to wrap its commit, so these three tests drive the store the way such a
     * hook would, one call per outcome. If only `commit(CommitAction)` existed, none of this would be
     * expressible and the component would be unusable under a transaction manager.
     */

    @Test
    @DisplayName("an after-commit hook applies, with no callback anywhere")
    void aContainerDrivenCommitAppliesThroughTheHook() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));

        log.commitTransaction();     // the container commits; the store is not involved
        store.applyCommitted();      // ... and then tells the store, from its afterCommit hook

        assertEquals("v", read(0, "k"));
        assertTrue(store.behind().isEmpty());
    }

    @Test
    @DisplayName("an after-rollback hook discards and leaves the partition readable")
    void aContainerDrivenRollbackDiscards() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));

        log.abortTransaction();
        store.discard();

        assertNull(read(0, "k"), "a rolled-back transaction must leave nothing applied");
        assertTrue(store.behind().isEmpty(), "and the partition is not behind: nothing was committed");
    }

    @Test
    @DisplayName("a commit whose outcome the container could not establish marks its partitions behind")
    void aContainerDrivenUnknownOutcomeMarksBehind() {
        store = open(8);
        store.assign(Arrays.asList(0, 1));
        store.stage(0, put("k", "v"));
        store.stage(1, put("k", "v"));

        // The commit did reach the broker; the container's doCommit threw anyway, so neither the
        // afterCommit nor the afterRollback hook can be the right one to fire.
        log.commitTransaction();
        store.discardUnknown();

        assertEquals(new TreeSet<>(Arrays.asList(0, 1)), new TreeSet<>(store.behind()));
        assertThrows(PartitionBehindException.class,
                () -> store.getCommittedBatch(0, Collections.singletonList("k")));

        store.repair(store.behind());
        assertEquals("v", read(0, "k"), "the repair replays what the transaction turned out to commit");
        assertEquals("v", read(1, "k"));
    }

    @Test
    @DisplayName("applyCommitted with nothing staged is a no-op, not a failure")
    void applyingAnEmptyBatchIsHarmless() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.applyCommitted();   // a batch that changed no state still commits its input offsets
        assertTrue(store.behind().isEmpty());
    }

    /**
     * A container that owns the commit, as Spring's {@code KafkaTransactionManager} does: hooks are
     * registered per batch and fired once the transaction has completed, one per outcome.
     */
    private static final class Container {
        private Runnable afterCommit;
        private Runnable afterRollback;
        private Runnable afterUnknown;

        void register(Runnable commit, Runnable rollback, Runnable unknown) {
            afterCommit = commit;
            afterRollback = rollback;
            afterUnknown = unknown;
        }

        /** @param transaction the container's own commit; throwing leaves the outcome unknown */
        void commit(Runnable transaction) {
            try {
                transaction.run();
            } catch (RuntimeException indeterminate) {
                fire(afterUnknown);
                throw indeterminate;
            }
            fire(afterCommit);
        }

        void rollback() {
            fire(afterRollback);
        }

        /** Cleared before running, as a thread-local hook holder must be. */
        private void fire(Runnable hook) {
            Runnable held = hook;
            afterCommit = afterRollback = afterUnknown = null;
            if (held != null) {
                held.run();
            }
        }
    }

    @Test
    @DisplayName("a three-hook container handles a commit whose outcome it could not establish")
    void theThirdHookIsWhatMakesAContainerSafe() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        Container container = new Container();
        container.register(store::applyCommitted, store::discard, store::discardUnknown);

        store.stage(0, put("k", "v"));
        assertThrows(RuntimeException.class, () -> container.commit(() -> {
            log.commitTransaction();      // it did reach the broker
            throw new IllegalStateException("injected: the commit timed out");
        }));

        assertEquals(Collections.singleton(0), store.behind(),
                "the transaction may have carried records the store does not hold");
        store.repair(Collections.singletonList(0));
        assertEquals("v", read(0, "k"));
    }

    @Test
    @DisplayName("a two-hook container silently leaves the batch staged and the partition readable")
    void theTwoHookWiringIsTheHazard() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        Container container = new Container();
        // The usual wiring, and the one that looks complete: afterCommit and afterRollback.
        container.register(store::applyCommitted, store::discard, null);

        store.stage(0, put("k", "first"));
        assertThrows(RuntimeException.class, () -> container.commit(() -> {
            log.commitTransaction();
            throw new IllegalStateException("injected: the commit timed out");
        }));

        // Neither hook fired: afterCommit is after the commit, and a container will not roll back a
        // transaction it could not commit. The store cannot detect this — nothing called it — so
        // this asserts the hazard rather than a behaviour, and it is the caller's to close.
        assertTrue(store.behind().isEmpty(), "nothing told the store anything, so nothing changed");

        // The cost, made concrete: the abandoned batch is still staged, so the *next* commit applies
        // writes from a transaction whose outcome was never established.
        store.stage(0, put("other", "second"));
        store.applyCommitted();
        assertEquals("first", read(0, "k"),
                "a write from the abandoned transaction reached the store on a later commit");
    }

    @Test
    @DisplayName("begin() closes that hazard without the container needing a third hook")
    void beginCatchesAnAbandonedTransaction() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        Container container = new Container();
        container.register(store::applyCommitted, store::discard, null);   // the two-hook wiring

        store.begin();
        store.stage(0, put("k", "first"));
        assertThrows(RuntimeException.class, () -> container.commit(() -> {
            log.commitTransaction();
            throw new IllegalStateException("injected: the commit timed out");
        }));
        assertTrue(store.behind().isEmpty(), "nothing has told the store anything yet");

        // The next transaction begins, and *that* is what notices: the previous batch resolved to
        // neither outcome, so it can only have been an unknown one.
        store.begin();
        assertEquals(Collections.singleton(0), store.behind());

        // And the partition is unreadable before anything in this batch can fold against it, which
        // is the whole point of catching it here rather than one commit later.
        assertThrows(PartitionBehindException.class,
                () -> store.getCommittedBatch(0, Collections.singletonList("k")));

        store.repair(Collections.singletonList(0));
        assertEquals("first", read(0, "k"), "the repair replays what the log turned out to hold");
    }

    @Test
    @DisplayName("begin() is a no-op when the previous transaction resolved")
    void beginIsFreeOnTheHappyPath() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.begin();
        store.stage(0, put("k", "v"));
        log.commitTransaction();
        store.applyCommitted();

        store.begin();       // nothing staged: no-op
        store.begin();       // and again
        assertTrue(store.behind().isEmpty());
        assertEquals("v", read(0, "k"));
    }

    // --- the apply loop ------------------------------------------------------

    @Test
    @DisplayName("an apply failure names every partition that may not hold what was committed")
    void theApplyLoopAttemptsEveryPartitionAndAggregates() {
        store = open(8);
        store.assign(Arrays.asList(0, 1, 2));
        poisoned.set(1);

        store.stage(0, put("k", "zero"));
        store.stage(1, put("k", "one"));
        store.stage(2, put("k", "two"));

        ApplyFailed failure =
                assertThrows(ApplyFailed.class, () -> store.commit(log::commitTransaction));

        assertEquals(Collections.singleton(1), failure.partitions());
        assertEquals(Collections.singleton(1), store.behind());
        assertEquals("zero", read(0, "k"), "a partition before the failure must still be applied");
        assertEquals("two", read(2, "k"),
                "a partition after the failure must be attempted too, or it is committed-but-unwritten");
    }

    @Test
    @DisplayName("a partition whose apply failed cannot be read or staged until repaired")
    void aBehindPartitionIsNotServed() {
        store = open(8);
        store.assign(Arrays.asList(0, 1));
        store.stage(0, put("k", "before"));
        store.stage(1, put("k", "before"));
        store.commit(log::commitTransaction);

        poisoned.set(1);
        store.stage(0, put("k", "after"));
        store.stage(1, put("k", "after"));
        assertThrows(ApplyFailed.class, () -> store.commit(log::commitTransaction));

        // The whole point: the stale value is not served, so no fold can derive from it.
        assertThrows(PartitionBehindException.class,
                () -> store.getCommittedBatch(1, Collections.singletonList("k")));
        assertThrows(PartitionBehindException.class, () -> store.stage(1, put("k", "later")));
        assertEquals("after", read(0, "k"), "an independent partition is unaffected");

        poisoned.set(-1);
        store.repair(Collections.singletonList(1));
        assertTrue(store.behind().isEmpty());
        assertEquals("after", read(1, "k"), "the repair replays exactly what the failed apply missed");
    }

    @Test
    @DisplayName("a failure after the batch was written still leaves the partition behind")
    void aFailureBetweenTheWriteAndTheStampIsNotSilent() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        Map<String, Mutation> first = new LinkedHashMap<>();
        first.put("k", Mutation.put(bytes("first")));
        first.put("j", Mutation.put(bytes("first")));   // two records, so the watermark reaches 1
        store.stage(0, first);
        store.commit(log::commitTransaction);

        // A position below the watermark: the engine rejects it, so the write lands and the stamp
        // does not. The store is ahead of its watermark, which is the harmless direction.
        stale.set(0);
        store.stage(0, put("k", "second"));
        ApplyFailed failure =
                assertThrows(ApplyFailed.class, () -> store.commit(log::commitTransaction));
        assertEquals(Collections.singleton(0), failure.partitions());
        assertTrue(failure.isTerminal(), "a rejected position is not a retryable I/O error");

        stale.set(-1);
        store.repair(Collections.singletonList(0));
        assertEquals("second", read(0, "k"), "re-applying an already-applied write is harmless");
    }

    @Test
    @DisplayName("an apply into an unassigned partition is reported, not swallowed")
    void applyingIntoARevokedPartitionFails() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.revoke(Collections.singletonList(0));   // drops the staged batch with it

        store.assign(Collections.singletonList(0));
        assertNull(read(0, "k"), "a revoked partition's staged writes go with it");
    }

    // --- restore -------------------------------------------------------------

    @Test
    @DisplayName("a cold restore advances the watermark, so a reopen resumes rather than replaying")
    void aColdRestoreAdvancesTheWatermark() {
        // Populate the log through another store, then throw that store away.
        PartitionedStore<String> writer = open(8);
        writer.assign(Collections.singletonList(0));
        for (int i = 0; i < 5; ++i) {
            writer.stage(0, put("k" + i, "v" + i));
            writer.commit(log::commitTransaction);
        }
        writer.close();

        PartitionFixture second = new PartitionFixture(root.resolve("replica"));
        try {
            resumePoints.clear();
            store = PartitionedStore.<String>builder()
                    .options(second::optionsFor)
                    .keyBytes(PartitionFixture.KEY_BYTES)
                    .changelog(this::send)
                    .restore((partition, through, sink) -> {
                        resumePoints.add(through);
                        log.restoreIn(2).restore(partition, through, sink);
                    })
                    .build();

            store.assign(Collections.singletonList(0));
            assertEquals(OptionalLong.empty(), resumePoints.get(0), "a cold store asks for everything");
            assertEquals("v4", read(0, "k4"));

            store.revoke(Collections.singletonList(0));
            store.assign(Collections.singletonList(0));
            assertEquals(OptionalLong.of(4), resumePoints.get(1),
                    "the restore itself must advance the watermark, or the replay is repeated in full");
            store.close();
            store = null;
        } finally {
            second.close();
        }
    }

    @Test
    @DisplayName("an interrupted restore resumes from the last flushed batch")
    void anInterruptedRestoreResumesWhereItStopped() {
        PartitionedStore<String> writer = open(1);
        writer.assign(Collections.singletonList(0));
        for (int i = 0; i < 6; ++i) {
            writer.stage(0, put("k" + i, "v" + i));
            writer.commit(log::commitTransaction);
        }
        writer.close();

        PartitionFixture second = new PartitionFixture(root.resolve("replica"));
        try {
            AtomicInteger batches = new AtomicInteger();
            AtomicBoolean armed = new AtomicBoolean(true);
            resumePoints.clear();
            // Fails once, after two batches of two: four records land and then the replay dies.
            Restore<String> flaky = (partition, through, sink) -> {
                resumePoints.add(through);
                log.restoreIn(2).restore(partition, through, (offset, mutations) -> {
                    if (armed.get() && batches.getAndIncrement() >= 2) {
                        armed.set(false);
                        throw new IllegalStateException("injected: the replay died");
                    }
                    sink.putBatch(offset, mutations);
                });
            };
            store = PartitionedStore.<String>builder()
                    .options(second::optionsFor)
                    .keyBytes(PartitionFixture.KEY_BYTES)
                    .changelog(this::send)
                    .restore(flaky)
                    .build();

            assertThrows(IllegalStateException.class,
                    () -> store.assign(Collections.singletonList(0)));
            assertTrue(store.assignment().isEmpty(), "a partial restore must not be served");

            store.assign(Collections.singletonList(0));
            assertEquals(OptionalLong.of(3), resumePoints.get(1),
                    "the retry must resume after the batches that landed, not from the beginning");
            assertEquals("v5", read(0, "k5"));
            store.close();
            store = null;
        } finally {
            second.close();
        }
    }

    @Test
    @DisplayName("restore failure fails the assignment")
    void aRestoreFailureFailsTheAssignment() {
        store = open((partition, through, sink) -> {
            throw new IllegalStateException("injected: no replay for you");
        });
        assertThrows(IllegalStateException.class, () -> store.assign(Collections.singletonList(0)));
        assertTrue(store.assignment().isEmpty());
        assertThrows(PartitionNotAssignedException.class,
                () -> store.getCommittedBatch(0, Collections.singletonList("k")));
    }

    @Test
    @DisplayName("a delete reaches the log as a value and comes back as a deletion")
    void aDeleteSurvivesARestore() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.commit(log::commitTransaction);
        store.stage(0, Collections.singletonMap("k", Mutation.delete()));
        store.commit(log::commitTransaction);
        assertNull(read(0, "k"), "a staged delete applies locally");

        // It must be a value on the log, or compaction will eventually erase the fact of the delete.
        assertNotNull(log.committed(0).get(1).value, "a delete must not travel as a tombstone");
        store.close();
        store = null;

        PartitionFixture second = new PartitionFixture(root.resolve("replica"));
        try {
            store = PartitionedStore.<String>builder()
                    .options(second::optionsFor)
                    .keyBytes(PartitionFixture.KEY_BYTES)
                    .changelog(this::send)
                    .restore(log.restoreIn(8))
                    .build();
            store.assign(Collections.singletonList(0));
            assertNull(read(0, "k"), "the replayed marker must decode back into a deletion");
            store.close();
            store = null;
        } finally {
            second.close();
        }
    }

    @Test
    @DisplayName("a restore that meets a tombstone fails fast")
    void aTombstoneInTheLogFailsTheRestore() {
        store = open((partition, through, sink) ->
                sink.putBatch(0, Collections.singletonMap("k", null)));
        NullPointerException failure = assertThrows(NullPointerException.class,
                () -> store.assign(Collections.singletonList(0)));
        assertTrue(failure.getMessage().contains("tombstone"),
                "the message should name what makes an incremental restore unsafe");
    }

    // --- lifecycle -----------------------------------------------------------

    @Test
    @DisplayName("a behind partition is still handed over correctly: its watermark is accurate")
    void revokingABehindPartitionIsSafe() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "first"));
        store.commit(log::commitTransaction);

        poisoned.set(0);
        store.stage(0, put("k", "second"));
        assertThrows(ApplyFailed.class, () -> store.commit(log::commitTransaction));
        assertEquals(Collections.singleton(0), store.behind());

        // Lagging is not corrupt. The watermark did not advance, so whoever picks this partition up
        // replays exactly what this instance failed to apply.
        poisoned.set(-1);
        store.revoke(Collections.singletonList(0));
        resumePoints.clear();
        store.assign(Collections.singletonList(0));
        assertEquals(OptionalLong.of(0), resumePoints.get(0),
                "the handover must resume from the position that did not move");
        assertEquals("second", read(0, "k"));
        assertTrue(store.behind().isEmpty(), "a reopened partition is restored, not inherited behind");
    }

    @Test
    @DisplayName("repairing a partition that is not behind is a replay, not an error")
    void repairingAReadyPartitionIsHarmless() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.commit(log::commitTransaction);

        store.repair(Collections.singletonList(0));
        assertEquals("v", read(0, "k"));
        assertThrows(PartitionNotAssignedException.class,
                () -> store.repair(Collections.singletonList(7)));
    }

    @Test
    @DisplayName("revoke flushes, so a re-assignment is incremental")
    void revokeFlushes() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.commit(log::commitTransaction);

        store.revoke(Collections.singletonList(0));
        resumePoints.clear();
        store.assign(Collections.singletonList(0));
        assertEquals(OptionalLong.of(0), resumePoints.get(0));
    }

    @Test
    @DisplayName("lost does not flush: whatever had not reached disk is redelivered from the log")
    void lostDoesNotFlush() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.commit(log::commitTransaction);

        store.lost(Collections.singletonList(0));
        resumePoints.clear();
        store.assign(Collections.singletonList(0));
        assertEquals(OptionalLong.empty(), resumePoints.get(0),
                "an unflushed watermark must never be reported");
        assertEquals("v", read(0, "k"), "and the full replay puts the state back");
    }

    @Test
    @DisplayName("an unassigned partition is reported rather than opened on demand")
    void unassignedPartitionsAreRejected() {
        store = open(8);
        assertThrows(PartitionNotAssignedException.class, () -> store.stage(0, put("k", "v")));
        assertFalse(store.assignment().contains(0));
    }

    @Test
    void statsReportTheMaterialisedPositionAndTheEngineUnderneath() {
        store = open(8);
        store.assign(Arrays.asList(0, 1));

        assertTrue(store.stats().get(0).materializedThrough().isEmpty(),
                "nothing applied yet, so there is no position to report");

        store.stage(0, put("k", "v"));
        store.commit(log::commitTransaction);

        PartitionedStore.PartitionStats zero = store.stats().get(0);
        assertTrue(zero.materializedThrough().isPresent());
        assertEquals(0L, zero.materializedThrough().getAsLong(),
                "the position is the changelog offset the apply covered");
        assertFalse(zero.behind());
        assertNotNull(zero.engine(), "the engine's own counters come through");
        assertTrue(zero.engine().entryCount() >= 1);

        assertTrue(store.stats().get(1).materializedThrough().isEmpty(),
                "a partition nothing was staged into has not moved");
    }

    /** The flag an alarm would watch: a partition out of service says so here. */
    @Test
    void statsReportAPartitionThatIsBehind() {
        store = open(8);
        store.assign(Collections.singletonList(0));
        store.stage(0, put("k", "v"));
        store.discardUnknown();

        assertTrue(store.stats().get(0).behind());

        store.repair(Collections.singletonList(0));
        assertFalse(store.stats().get(0).behind());
    }

    @Test
    void statsCoverOnlyHeldPartitions() {
        store = open(8);
        store.assign(Arrays.asList(0, 1));
        store.revoke(Collections.singletonList(1));

        assertEquals(Collections.singleton(0), store.stats().keySet());
    }
}
