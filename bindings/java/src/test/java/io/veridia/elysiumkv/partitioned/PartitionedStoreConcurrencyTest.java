package io.veridia.elysiumkv.partitioned;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static io.veridia.elysiumkv.partitioned.InMemoryLog.bytes;
import static io.veridia.elysiumkv.partitioned.InMemoryLog.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A fold that parallelises over its keys, which is what the store is embedded in.
 *
 * <p>The oracle is the same one the differential uses: a partition that is not behind holds exactly
 * what replaying its log produces. Concurrency cannot be gated on timing, so every assertion here is
 * over counts and content — the barrier makes the threads overlap, and the log says what the answer
 * must be regardless of the order they got there in.
 *
 * <p>What this cannot promise: a race shows itself on some schedules and not others, so the two tests
 * that hunt one repeat. Against a build with the staging lock removed they fail on most runs and pass
 * on some, which is the bound on what a test without a controlled scheduler can say.
 */
class PartitionedStoreConcurrencyTest {

    private static final int PARTITIONS = 4;
    private static final int THREADS = 8;
    private static final int KEYS_PER_THREAD = 50;

    @TempDir
    Path root;

    private PartitionFixture fixture;
    private InMemoryLog log;
    private PartitionedStore<String> store;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        fixture = new PartitionFixture(root);
        log = new InMemoryLog();
        pool = Executors.newFixedThreadPool(THREADS);
        store = PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(new Changelog<String>() {
                    @Override
                    public synchronized PendingPosition send(int partition, String key,
                                                             Mutation mutation) {
                        return log.send(partition, key, mutation);
                    }

                    @Override
                    public synchronized PendingPosition sendDeleteRange(int partition, byte[] lo,
                                                                        byte[] hi) {
                        return log.sendDeleteRange(partition, lo, hi);
                    }
                })
                .restore(log.restoreIn(64))
                .build();
        List<Integer> all = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            all.add(partition);
        }
        store.assign(all);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
        if (store != null) {
            store.close();
        }
        fixture.close();
    }

    @Test
    @DisplayName("keys staged from several threads all reach the store, in the order the log took them")
    void aParallelFoldStagesAndReadsWithoutLosingWrites() throws Exception {
        run(thread -> () -> {
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                int partition = (thread + i) % PARTITIONS;
                String key = "t" + thread + ":k" + i;
                store.put(partition, Collections.singletonMap(key, Mutation.put(bytes(key))));
                // Read-your-writes from the staging thread, which is the point of the read lock.
                assertEquals(key, string(store.get(partition, key)),
                        "a thread must see its own staged write");
            }
            return null;
        });

        store.commit(log::commitTransaction);
        assertTrue(store.behind().isEmpty(), "nothing should be behind");
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            assertEquals(replayed(partition), held(partition),
                    "partition " + partition + " does not match its log");
        }
        assertEquals(THREADS * KEYS_PER_THREAD, totalRecords(), "every staged key reached the log");
    }

    @RepeatedTest(4)
    @DisplayName("threads racing the same keys leave the store agreeing with the log")
    void threadsRacingTheSameKeysDoNotDiverge() throws Exception {
        AtomicInteger sequence = new AtomicInteger();
        run(thread -> () -> {
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                // A small keyspace shared by every thread, so the same key is staged repeatedly from
                // different threads: the case where the overlay's order and the log's could disagree.
                String key = "shared:k" + (i % 8);
                int partition = i % PARTITIONS;
                store.put(partition, Collections.singletonMap(key,
                        Mutation.put(bytes("v" + sequence.incrementAndGet()))));
            }
            return null;
        });

        store.commit(log::commitTransaction);
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            assertEquals(replayed(partition), held(partition),
                    "partition " + partition + " applied a value the log says was superseded");
        }
    }

    @RepeatedTest(4)
    @DisplayName("a range delete staged while other threads stage points still resolves by order")
    void aRangeDeleteRacingPointsResolvesByOrder() throws Exception {
        store.put(0, Collections.singletonMap("shared:k0", Mutation.put(bytes("before"))));
        store.commit(log::commitTransaction);

        run(thread -> () -> {
            if (thread == 0) {
                store.deleteRange(0, bytes("shared:"), bytes("shared;"));
                return null;
            }
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                store.put(0, Collections.singletonMap("other:k" + thread + ":" + i,
                        Mutation.put(bytes("v"))));
            }
            return null;
        });

        store.commit(log::commitTransaction);
        assertEquals(replayed(0), held(0),
                "the band and the points must resolve the way the log orders them");
        assertTrue(store.behind().isEmpty());
    }

    // --- support -------------------------------------------------------------

    /** Runs one task per thread, all released together so that they actually overlap. */
    private void run(java.util.function.IntFunction<Callable<Void>> task) throws Exception {
        CyclicBarrier start = new CyclicBarrier(THREADS);
        List<Future<Void>> running = new ArrayList<>();
        for (int thread = 0; thread < THREADS; ++thread) {
            Callable<Void> body = task.apply(thread);
            running.add(pool.submit(() -> {
                start.await();
                return body.call();
            }));
        }
        for (Future<Void> future : running) {
            future.get();          // rethrows whatever a thread threw, including an assertion
        }
    }

    /** The whole log, folded — the definition of what the partition should hold. */
    private Map<String, String> replayed(int partition) {
        Map<String, String> state = new TreeMap<>();
        for (InMemoryLog.Record record : log.committed(partition)) {
            if (record.isRangeDelete()) {
                state.keySet().removeIf(key -> covered(key, record.lower, record.upper));
                continue;
            }
            Mutation mutation = InMemoryLog.decode(record.value);
            if (mutation.isDelete()) {
                state.remove(record.key);
            } else {
                state.put(record.key, string(mutation.value()));
            }
        }
        return state;
    }

    private static boolean covered(String key, byte[] lower, byte[] upper) {
        byte[] encoded = bytes(key);
        return java.util.Arrays.compareUnsigned(encoded, lower) >= 0
                && java.util.Arrays.compareUnsigned(encoded, upper) < 0;
    }

    /** What the store holds, read back through a scan so the merge is covered too. */
    private Map<String, String> held(int partition) {
        Map<String, String> state = new TreeMap<>();
        try (StagedIterator scan = store.iterator(partition, null, null)) {
            while (scan.next()) {
                state.put(string(scan.key()), string(scan.value()));
            }
            scan.status();
        }
        return state;
    }

    private int totalRecords() {
        int total = 0;
        for (int partition = 0; partition < PARTITIONS; ++partition) {
            total += log.committed(partition).size();
        }
        return total;
    }
}
