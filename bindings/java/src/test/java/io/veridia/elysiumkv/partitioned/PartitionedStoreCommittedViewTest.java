package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.BatchedIterator;

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

import static io.veridia.elysiumkv.partitioned.InMemoryLog.bytes;
import static io.veridia.elysiumkv.partitioned.InMemoryLog.string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The committed view: what a reader outside the fold sees.
 *
 * <p>Its whole reason for existing is that it does <em>not</em> see the staged set, so every test here
 * stages something and asserts it is absent. A suite that only read committed state after a commit
 * would pass against methods that folded the staged set in.
 */
class PartitionedStoreCommittedViewTest {

    @TempDir
    Path root;

    private PartitionFixture fixture;
    private InMemoryLog log;
    private PartitionedStore<String> store;

    @BeforeEach
    void setUp() {
        fixture = new PartitionFixture(root);
        log = new InMemoryLog();
        store = PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(new Changelog<String>() {
                    @Override
                    public PendingPosition send(int partition, String key, Mutation mutation) {
                        return log.send(partition, key, mutation);
                    }

                    @Override
                    public PendingPosition sendDeleteRange(int partition, byte[] lo, byte[] hi) {
                        return log.sendDeleteRange(partition, lo, hi);
                    }
                })
                .restore(log.restoreIn(8))
                .build();
        store.assign(Collections.singletonList(0));
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
        fixture.close();
    }

    @Test
    @DisplayName("getCommitted ignores the staged set where get folds it in")
    void getCommittedIgnoresTheStagedSet() {
        commit(put("k", "committed"));

        store.put(0, put("k", "staged"));
        assertEquals("staged", string(store.get(0, "k")), "the fold sees its own write");
        assertEquals("committed", string(store.getCommitted(0, "k")), "a query does not");

        store.put(0, Collections.singletonMap("gone", Mutation.delete()));
        commit(put("gone", "present"));
        assertNotNull(store.getCommitted(0, "gone"));
    }

    @Test
    @DisplayName("a staged delete does not remove a row from the committed view")
    void aStagedDeleteIsInvisibleToTheCommittedView() {
        commit(put("k", "v"));
        store.put(0, Collections.singletonMap("k", Mutation.delete()));

        assertNull(store.get(0, "k"), "the fold sees its own delete");
        assertEquals("v", string(store.getCommitted(0, "k")), "a query still sees the row");
        assertEquals(Collections.singletonList("k=v"), batched(store.committedBatchedIterator(0, null, null)));
    }

    @Test
    @DisplayName("the batched scans deliver committed rows in both directions, and no staged ones")
    void theBatchedScansAreTheCommittedView() {
        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("p:1", Mutation.put(bytes("1")));
        committed.put("p:2", Mutation.put(bytes("2")));
        committed.put("q:1", Mutation.put(bytes("3")));
        commit(committed);

        Map<String, Mutation> staged = new LinkedHashMap<>();
        staged.put("p:3", Mutation.put(bytes("staged")));
        staged.put("q:2", Mutation.put(bytes("staged")));
        store.put(0, staged);

        assertEquals(Arrays.asList("p:1=1", "p:2=2", "q:1=3"),
                batched(store.committedBatchedIterator(0, null, null)));
        assertEquals(Arrays.asList("q:1=3", "p:2=2", "p:1=1"),
                batched(store.committedBatchedReverseIterator(0, null, null)));
        assertEquals(Arrays.asList("p:1=1", "p:2=2"),
                batched(store.committedBatchedPrefixIterator(0, bytes("p:"))));
        assertEquals(Arrays.asList("p:2=2", "p:1=1"),
                batched(store.committedBatchedReversePrefixIterator(0, bytes("p:"))));
    }

    @Test
    @DisplayName("the committed view still refuses a behind partition")
    void aBehindPartitionIsNotServedToAQueryEither() {
        commit(put("k", "v"));
        store.put(0, put("k", "next"));
        assertThrows(OutcomeUnknown.class, () -> store.commit(() -> {
            throw new OutcomeUnknown(new RuntimeException("injected"));
        }));
        assertEquals(Collections.singleton(0), store.behind());

        // A query is being served, so the invariant's second clause applies to it as much as to a fold.
        assertThrows(PartitionBehindException.class, () -> store.getCommitted(0, "k"));
        assertThrows(PartitionBehindException.class,
                () -> store.committedBatchedIterator(0, null, null));
        assertThrows(PartitionBehindException.class,
                () -> store.committedBatchedPrefixIterator(0, bytes("k")));
        assertThrows(PartitionBehindException.class,
                () -> store.committedBatchedReverseIterator(0, null, null));
        assertThrows(PartitionBehindException.class,
                () -> store.committedBatchedReversePrefixIterator(0, bytes("k")));
        assertThrows(PartitionNotAssignedException.class, () -> store.getCommitted(7, "k"));
    }

    // --- support -------------------------------------------------------------

    private void commit(Map<String, Mutation> mutations) {
        store.put(0, mutations);
        store.commit(log::commitTransaction);
    }

    private static Map<String, Mutation> put(String key, String value) {
        return Collections.singletonMap(key, Mutation.put(bytes(value)));
    }

    /** Drains a batched scan into {@code key=value} in delivery order, closing it. */
    private List<String> batched(BatchedIterator iterator) {
        List<String> delivered = new ArrayList<>();
        try (BatchedIterator scan = iterator) {
            while (scan.next()) {
                delivered.add(string(scan.keyBytes()) + "=" + string(scan.valueBytes()));
            }
            scan.status();
        }
        return delivered;
    }
}
