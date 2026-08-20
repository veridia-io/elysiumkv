package io.veridia.elysiumkv.partitioned;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static io.veridia.elysiumkv.partitioned.InMemoryLog.bytes;
import static io.veridia.elysiumkv.partitioned.InMemoryLog.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Random operation streams against the log itself as the oracle.
 *
 * <p>The invariant checked after <em>every</em> step is the component's whole contract restated as
 * something mechanically checkable:
 *
 * <blockquote>A partition that is not behind holds exactly what replaying its entire log
 * produces.</blockquote>
 *
 * <p>That covers both halves at once. If a commit applied something the log does not have, the store
 * is ahead and the comparison fails. If a commit failed to apply something the log does have and the
 * partition was left readable anyway, the store is behind while claiming to be current — which is the
 * failure that poisons the log downstream, and the one no single-scenario test is likely to reach by
 * hand.
 *
 * <p>The keyspace is deliberately small so that puts, overwrites and deletes collide constantly.
 */
class PartitionedStoreDifferentialTest {

    private static final int PARTITIONS = 3;
    private static final int KEYSPACE = 20;
    private static final int STEPS = 80;

    @TempDir
    Path root;

    private PartitionFixture fixture;
    private InMemoryLog log;
    private PartitionedStore<String> store;
    private int poisoned = -1;

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
        if (fixture != null) {
            fixture.close();
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233})
    @DisplayName("a partition that is not behind matches its log, whatever the operation stream")
    void theStoreNeverDivergesFromTheLog(long seed) {
        fixture = new PartitionFixture(root);
        log = new InMemoryLog();
        store = PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(this::send)
                .restore(log.restoreIn(16))
                .build();

        List<Integer> all = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            all.add(partition);
        }
        store.assign(all);

        Random random = new Random(seed);
        for (int step = 0; step < STEPS; ++step) {
            String what = act(random, step);
            try {
                verify();
            } catch (AssertionError divergence) {
                fail("seed " + seed + " diverged at step " + step + " (" + what + "): "
                        + divergence.getMessage());
            }
        }
    }

    // --- the operations ------------------------------------------------------

    private String act(Random random, int step) {
        int roll = random.nextInt(100);
        if (roll < 45) {
            return commitSuccessfully(random);
        }
        if (roll < 55) {
            return commitAbortably(random);
        }
        if (roll < 65) {
            return commitWithUnknownOutcome(random, step % 2 == 0);
        }
        if (roll < 75) {
            return commitWithAFailingApply(random);
        }
        if (roll < 82) {
            return stageThenDiscard(random);
        }
        if (roll < 90) {
            return repairEverythingBehind();
        }
        if (roll < 96) {
            return cycle(random, true);
        }
        return cycle(random, false);
    }

    private String commitSuccessfully(Random random) {
        if (!stageInto(random)) {
            return "nothing to stage";
        }
        store.commit(log::commitTransaction);
        return "commit";
    }

    private String commitAbortably(Random random) {
        stageInto(random);
        try {
            store.commit(() -> {
                log.abortTransaction();
                throw new AbortableNotCommitted(new RuntimeException("differential"));
            });
            fail("the injected failure did not surface");
        } catch (AbortableNotCommitted expected) {
            // Nothing committed, nothing applied, nothing behind.
        }
        return "abortable failure";
    }

    /** @param committed whether the transaction actually went through before the timeout */
    private String commitWithUnknownOutcome(Random random, boolean committed) {
        stageInto(random);
        try {
            store.commit(() -> {
                if (committed) {
                    log.commitTransaction();
                } else {
                    log.abortTransaction();
                }
                throw new OutcomeUnknown(new RuntimeException("differential"));
            });
            fail("the injected failure did not surface");
        } catch (OutcomeUnknown expected) {
            // Whatever happened, the staged partitions are now unreadable.
        }
        return "unknown outcome (log " + (committed ? "committed" : "aborted") + ")";
    }

    private String commitWithAFailingApply(Random random) {
        List<Integer> ready = ready();
        if (ready.isEmpty()) {
            return "nothing ready to poison";
        }
        // Chosen *before* staging, because the poisoned position is decided when the record is sent
        // and only throws when it is read. An earlier draft set it afterwards, so nothing was ever
        // poisoned and this branch tested a plain successful commit — which is why the suite passed
        // against implementations that were known to be wrong.
        poisoned = ready.get(random.nextInt(ready.size()));
        try {
            stageInto(random);
            // Guarantee the poisoned partition is in this transaction rather than hoping.
            store.stage(poisoned, Collections.singletonMap("k" + random.nextInt(KEYSPACE),
                    Mutation.put(bytes("v" + random.nextInt(1000)))));
            store.commit(log::commitTransaction);
            fail("partition " + poisoned + " was staged with a poisoned position and still applied");
            return "unreachable";
        } catch (ApplyFailed expected) {
            assertEquals(Collections.singleton(poisoned), expected.partitions(),
                    "exactly the poisoned partition should have failed to apply");
            assertTrue(store.behind().contains(poisoned), "a failed apply must leave it unreadable");
            return "apply failure on " + expected.partitions();
        } finally {
            poisoned = -1;
        }
    }

    /** Assigned and readable — the only partitions an operation may touch. */
    private List<Integer> ready() {
        List<Integer> ready = new ArrayList<>();
        Set<Integer> behind = store.behind();
        for (int partition : store.assignment()) {
            if (!behind.contains(partition)) {
                ready.add(partition);
            }
        }
        return ready;
    }

    private String stageThenDiscard(Random random) {
        stageInto(random);
        log.abortTransaction();     // the caller's transaction goes with the staged set
        store.discard();
        return "stage then discard";
    }

    private String repairEverythingBehind() {
        Set<Integer> behind = store.behind();
        store.repair(behind);
        return "repair " + behind;
    }

    /** A handover (flush) or a crash (no flush), then take the partition back. */
    private String cycle(Random random, boolean clean) {
        int partition = random.nextInt(PARTITIONS);
        List<Integer> one = Collections.singletonList(partition);
        if (clean) {
            store.revoke(one);
        } else {
            store.lost(one);
        }
        store.assign(one);
        return (clean ? "revoke" : "lose") + " and reassign " + partition;
    }

    // --- staging -------------------------------------------------------------

    /** @return whether anything was staged at all */
    private boolean stageInto(Random random) {
        boolean staged = false;
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            if (store.behind().contains(partition) || random.nextInt(3) == 0) {
                continue;
            }
            Map<String, Mutation> mutations = new LinkedHashMap<>();
            int count = 1 + random.nextInt(4);
            for (int i = 0; i < count; ++i) {
                String key = "k" + random.nextInt(KEYSPACE);
                mutations.put(key, random.nextInt(4) == 0
                        ? Mutation.delete()
                        : Mutation.put(bytes("v" + random.nextInt(1000))));
            }
            store.stage(partition, mutations);
            staged = true;
        }
        return staged;
    }

    private PendingPosition send(int partition, String key, Mutation mutation) {
        PendingPosition real = log.send(partition, key, mutation);
        return partition == poisoned
                ? () -> {
                    throw new IllegalStateException("differential: injected apply failure");
                }
                : real;
    }

    // --- the oracle ----------------------------------------------------------

    private void verify() {
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            if (store.behind().contains(partition) || !store.assignment().contains(partition)) {
                continue;
            }
            assertEquals(replayed(partition), held(partition),
                    "partition " + partition + " does not match its log");
        }
    }

    /** The whole log, folded. The definition of what the partition should hold. */
    private Map<String, String> replayed(int partition) {
        Map<String, String> state = new TreeMap<>();
        for (InMemoryLog.Record record : log.committed(partition)) {
            Mutation mutation = InMemoryLog.decode(record.value);
            if (mutation.isDelete()) {
                state.remove(record.key);
            } else {
                state.put(record.key, string(mutation.value()));
            }
        }
        return state;
    }

    private Map<String, String> held(int partition) {
        Set<String> keys = new TreeSet<>();
        for (int i = 0; i < KEYSPACE; ++i) {
            keys.add("k" + i);
        }
        Map<String, String> state = new TreeMap<>();
        for (Map.Entry<String, byte[]> entry
                : store.getCommittedBatch(partition, new ArrayList<>(keys)).entrySet()) {
            state.put(entry.getKey(), string(entry.getValue()));
        }
        return state;
    }
}
