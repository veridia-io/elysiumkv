package io.veridia.elysiumkv.partitioned;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.veridia.elysiumkv.partitioned.InMemoryLog.bytes;
import static io.veridia.elysiumkv.partitioned.InMemoryLog.string;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What a transaction sees of its own staged set, through a read and through each of the four scans. */
class PartitionedStoreStagedViewTest {

    @TempDir
    Path root;

    private PartitionFixture fixture;
    private InMemoryLog log;
    private List<OptionalLong> resumePoints;
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

    // --- reads ---------------------------------------------------------------

    @Test
    @DisplayName("get sees what the transaction staged")
    void getFoldsTheStagedSetIn() {
        open();
        commit(put("k", "first"));

        store.put(0, put("k", "second"));
        assertEquals("second", string(store.get(0, "k")), "a fold must see its own earlier output");

        store.commit(log::commitTransaction);
        assertEquals("second", string(store.get(0, "k")));
    }

    @Test
    @DisplayName("a staged delete reads as absent, not as the value it replaced")
    void aStagedDeleteReadsAsAbsent() {
        open();
        commit(put("k", "v"));

        store.put(0, Collections.singletonMap("k", Mutation.delete()));
        assertNull(store.get(0, "k"));
    }

    @Test
    @DisplayName("an empty staged value is present, not absent")
    void anEmptyStagedValueIsPresentNotAbsent() {
        open();
        store.put(0, Collections.singletonMap("k", Mutation.put(new byte[0])));

        byte[] read = store.get(0, "k");
        assertNotNull(read, "an empty value is a value");
        assertEquals(0, read.length);
        assertEquals(Collections.singletonList("k="), scan(store.iterator(0, null, null)));

        store.commit(log::commitTransaction);
        assertNotNull(store.get(0, "k"));
    }

    @Test
    @DisplayName("the same key staged twice keeps the last mutation")
    void theSameKeyStagedTwiceKeepsTheLastMutation() {
        open();
        store.put(0, put("k", "first"));
        store.put(0, put("k", "second"));
        assertEquals("second", string(store.get(0, "k")));
        store.commit(log::commitTransaction);
        assertEquals("second", string(store.get(0, "k")));

        store.put(0, put("k", "third"));
        store.put(0, Collections.singletonMap("k", Mutation.delete()));
        assertNull(store.get(0, "k"));
        store.commit(log::commitTransaction);
        assertNull(store.get(0, "k"));
    }

    @Test
    @DisplayName("a read hands back a copy, so mutating it cannot rewrite the pending batch")
    void aReadHandsBackACopy() {
        open();
        store.put(0, put("k", "staged"));

        byte[] read = store.get(0, "k");
        read[0] = 'X';
        assertEquals("staged", string(store.get(0, "k")), "the staged set must be unchanged");

        try (StagedIterator scan = store.iterator(0, null, null)) {
            assertTrue(scan.next());
            scan.value()[0] = 'Y';
        }
        assertEquals("staged", string(store.get(0, "k")), "a scan's value is a copy too");

        // The changelog encoded "staged" when it was staged, so this is what the apply must write for
        // the store and the log to agree.
        store.commit(log::commitTransaction);
        assertEquals("staged", string(store.get(0, "k")));
    }

    // --- scans ---------------------------------------------------------------

    @Test
    @DisplayName("a scan merges staged puts into committed order and hides staged deletes")
    void aScanMergesTheStagedSet() {
        open();
        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("a", Mutation.put(bytes("1")));
        committed.put("c", Mutation.put(bytes("3")));
        committed.put("d", Mutation.put(bytes("4")));
        commit(committed);

        Map<String, Mutation> mutations = new LinkedHashMap<>();
        mutations.put("b", Mutation.put(bytes("2")));
        mutations.put("c", Mutation.put(bytes("30")));
        mutations.put("d", Mutation.delete());
        store.put(0, mutations);

        assertEquals(Arrays.asList("a=1", "b=2", "c=30"), scan(store.iterator(0, null, null)),
                "a staged key belongs in key order, a staged overwrite replaces rather than joins the "
                        + "committed value, and a staged delete removes it");
    }

    @Test
    @DisplayName("a reverse scan delivers the same set descending")
    void aReverseScanDeliversTheSameSetDescending() {
        open();
        commit(put("a", "1"));
        store.put(0, put("b", "2"));

        assertEquals(Arrays.asList("b=2", "a=1"), scan(store.reverseIterator(0, null, null)));
    }

    @Test
    @DisplayName("a reverse scan interleaves both sides and folds a range delete")
    void aReverseScanInterleavesBothSidesAndFoldsARangeDelete() {
        open();
        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("a", Mutation.put(bytes("1")));
        committed.put("c", Mutation.put(bytes("3")));
        committed.put("e", Mutation.put(bytes("5")));
        commit(committed);

        Map<String, Mutation> mutations = new LinkedHashMap<>();
        mutations.put("b", Mutation.put(bytes("2")));
        mutations.put("f", Mutation.put(bytes("6")));
        store.put(0, mutations);
        store.deleteRange(0, bytes("c"), bytes("d"));

        assertEquals(Arrays.asList("f=6", "e=5", "b=2", "a=1"),
                scan(store.reverseIterator(0, null, null)));
    }

    @Test
    @DisplayName("a range scan bounds the staged side as well as the committed one")
    void aRangeScanBoundsBothSides() {
        open();
        commit(put("a", "1"));

        Map<String, Mutation> mutations = new LinkedHashMap<>();
        mutations.put("b", Mutation.put(bytes("2")));
        mutations.put("z", Mutation.put(bytes("26")));
        store.put(0, mutations);

        assertEquals(Arrays.asList("a=1", "b=2"), scan(store.iterator(0, bytes("a"), bytes("c"))),
                "a staged key outside the bounds is not in the scan");
    }

    @Test
    @DisplayName("a prefix scan bounds the staged side to the prefix")
    void aPrefixScanBoundsTheStagedSide() {
        open();
        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("p:1", Mutation.put(bytes("x")));
        committed.put("q:1", Mutation.put(bytes("y")));
        commit(committed);

        Map<String, Mutation> mutations = new LinkedHashMap<>();
        mutations.put("p:2", Mutation.put(bytes("z")));
        mutations.put("q:2", Mutation.put(bytes("w")));
        store.put(0, mutations);

        assertEquals(Arrays.asList("p:1=x", "p:2=z"), scan(store.prefixIterator(0, bytes("p:"))));
        assertEquals(Arrays.asList("q:2=w", "q:1=y"), scan(store.reversePrefixIterator(0, bytes("q:"))));
    }

    @Test
    @DisplayName("a prefix of nothing but 0xFF scans both sides to the end of the keyspace")
    void anAllOnesPrefixScansToTheEndOfTheKeyspace() {
        // Latin-1 so a key can hold 0xFF at all; UTF-8 never emits it, which is why this corner was
        // out of reach while the staged side derived an upper bound from the prefix.
        store = PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(key -> key.getBytes(StandardCharsets.ISO_8859_1))
                .changelog(changelog())
                .restore(log.restoreIn(8))
                .build();
        store.assign(Collections.singletonList(0));

        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("\u00ff\u0001", Mutation.put(bytes("in")));
        committed.put("\u00fe\u0001", Mutation.put(bytes("out")));
        commit(committed);

        Map<String, Mutation> mutations = new LinkedHashMap<>();
        mutations.put("\u00ff\u0002", Mutation.put(bytes("in2")));
        mutations.put("\u00fe\u0002", Mutation.put(bytes("out2")));
        store.put(0, mutations);

        byte[] prefix = {(byte) 0xFF};
        assertEquals(Arrays.asList("in", "in2"), values(store.prefixIterator(0, prefix)),
                "the band runs to the end of the keyspace, and nothing below it is in the band");
        assertEquals(Arrays.asList("in2", "in"), values(store.reversePrefixIterator(0, prefix)));
    }

    @Test
    @DisplayName("an iterator is fixed when it is created")
    void anIteratorIsFixedWhenItIsCreated() {
        open();
        commit(put("a", "1"));

        try (StagedIterator scan = store.iterator(0, null, null)) {
            store.put(0, put("b", "2"));
            List<String> delivered = new ArrayList<>();
            while (scan.next()) {
                delivered.add(string(scan.key()));
            }
            assertEquals(Collections.singletonList("a"), delivered,
                    "staging after the scan started must not change what it delivers");
        }
    }

    // --- range deletes -------------------------------------------------------

    @Test
    @DisplayName("a staged range delete hides the band it covers and nothing else")
    void aStagedRangeDeleteHidesItsBand() {
        open();
        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("a", Mutation.put(bytes("1")));
        committed.put("b", Mutation.put(bytes("2")));
        committed.put("c", Mutation.put(bytes("3")));
        commit(committed);

        store.deleteRange(0, bytes("b"), bytes("c"));
        assertNull(store.get(0, "b"), "a key in the band is gone from the transaction's view");
        assertNotNull(store.get(0, "a"), "the lower bound is inclusive, so a is outside");
        assertNotNull(store.get(0, "c"), "the upper bound is exclusive, so c survives");
        assertEquals(Arrays.asList("a=1", "c=3"), scan(store.iterator(0, null, null)));
    }

    @Test
    @DisplayName("a bounded scan folds a range delete by its overlap")
    void aBoundedScanFoldsARangeDeleteByItsOverlap() {
        open();
        Map<String, Mutation> committed = new LinkedHashMap<>();
        for (String key : Arrays.asList("a", "b", "c", "d")) {
            committed.put(key, Mutation.put(bytes(key)));
        }
        commit(committed);

        // The direction that can actually fail: the snapshot keeps only the ranges overlapping the
        // scan, so one wrongly dropped there delivers keys the band covers.
        store.deleteRange(0, bytes("b"), bytes("d"));
        assertEquals(Collections.singletonList("a=a"), scan(store.iterator(0, bytes("a"), bytes("c"))),
                "a range overlapping the scan only partly still covers its part");

        store.discard();
        store.deleteRange(0, bytes("c"), bytes("e"));
        assertEquals(Arrays.asList("a=a", "b=b"), scan(store.iterator(0, bytes("a"), bytes("c"))),
                "a range starting at the scan's exclusive upper bound covers nothing in it");

        store.discard();
        store.deleteRange(0, bytes("a"), bytes("b"));
        assertEquals(Arrays.asList("b=b", "c=c"), scan(store.iterator(0, bytes("b"), bytes("d"))),
                "a range ending at the scan's inclusive lower bound covers nothing in it");
    }

    @Test
    @DisplayName("order decides between a range delete and a point mutation")
    void orderDecidesBetweenARangeDeleteAndAPoint() {
        open();
        commit(put("b", "before"));

        store.put(0, put("b1", "covered"));
        store.deleteRange(0, bytes("b"), bytes("c"));
        store.put(0, put("b2", "after"));

        assertNull(store.get(0, "b"), "a committed key in the band is covered");
        assertNull(store.get(0, "b1"), "a put staged before the range delete is covered by it");
        assertEquals("after", string(store.get(0, "b2")), "a put staged after it survives");

        store.commit(log::commitTransaction);
        assertNull(store.get(0, "b"));
        assertNull(store.get(0, "b1"));
        assertEquals("after", string(store.get(0, "b2")),
                "the batch must apply the range and the points in the order they were staged");
    }

    @Test
    @DisplayName("a scan sees point-versus-range order the way a read does")
    void aScanSeesPointVersusRangeOrderTheWayAReadDoes() {
        open();
        commit(put("b", "before"));
        store.put(0, put("b1", "covered"));
        store.deleteRange(0, bytes("b"), bytes("c"));
        store.put(0, put("b2", "after"));

        assertEquals(Collections.singletonList("b2=after"), scan(store.iterator(0, null, null)));
    }

    @Test
    @DisplayName("the later of two overlapping range deletes decides")
    void theLaterOfTwoOverlappingRangeDeletesDecides() {
        open();
        commit(put("b", "committed"));

        store.deleteRange(0, bytes("a"), bytes("z"));
        store.put(0, put("b", "between"));
        store.deleteRange(0, bytes("a"), bytes("c"));
        assertNull(store.get(0, "b"), "the range staged after the put covers it");
        assertEquals(Collections.emptyList(), scan(store.iterator(0, null, null)));

        store.discard();
        store.deleteRange(0, bytes("a"), bytes("c"));
        store.deleteRange(0, bytes("a"), bytes("z"));
        store.put(0, put("b", "last"));
        assertEquals("last", string(store.get(0, "b")), "the put staged after both survives");
    }

    @Test
    @DisplayName("an empty or inverted range stages nothing")
    void anEmptyRangeStagesNothing() {
        open();
        commit(put("b", "v"));

        store.deleteRange(0, bytes("b"), bytes("b"));
        store.deleteRange(0, bytes("c"), bytes("a"));
        store.commit(log::commitTransaction);
        assertEquals("v", string(store.get(0, "b")));
        assertEquals(1, log.committed(0).size(), "an empty range must not reach the log");
    }

    @Test
    @DisplayName("a committed range delete replays on a rebuild")
    void aCommittedRangeDeleteReplaysOnARebuild() {
        open();
        Map<String, Mutation> committed = new LinkedHashMap<>();
        committed.put("a", Mutation.put(bytes("1")));
        committed.put("b", Mutation.put(bytes("2")));
        commit(committed);

        store.deleteRange(0, bytes("b"), bytes("c"));
        store.commit(log::commitTransaction);
        commit(put("b2", "after"));

        resumePoints.clear();
        store.lost(Collections.singletonList(0));    // no flush: nothing on disk to resume from
        store.assign(Collections.singletonList(0));

        assertEquals(Collections.singletonList(OptionalLong.empty()), resumePoints,
                "the rebuild must have been a full replay, or this asserts nothing about the record");
        assertEquals("1", string(store.get(0, "a")));
        assertNull(store.get(0, "b"), "the replayed range delete must cover b again");
        assertEquals("after", string(store.get(0, "b2")),
                "a record after the range delete is not covered by it");
    }

    @Test
    @DisplayName("a range delete replayed below the materialised position is refused")
    void aStaleRangeDeleteIsRefused() {
        AtomicBoolean stale = new AtomicBoolean();
        store = PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(changelog())
                .restore((partition, materializedThrough, sink) -> {
                    if (stale.get()) {
                        sink.deleteRange(0, bytes("a"), bytes("z"));
                        return;
                    }
                    log.restoreIn(8).restore(partition, materializedThrough, sink);
                })
                .build();
        store.assign(Collections.singletonList(0));
        commit(put("k", "v"));
        commit(put("k2", "v"));           // the watermark is now 1, so replaying 0 goes backwards

        store.put(0, put("k", "next"));
        store.discardUnknown();
        stale.set(true);

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> store.repair(Collections.singletonList(0)));
        assertTrue(refused.getMessage().contains("went backwards"), refused.getMessage());
        assertEquals(Collections.singleton(0), store.behind(),
                "a repair that throws leaves the partition behind");
    }

    // --- the invariant's second clause ---------------------------------------

    @Test
    @DisplayName("every read and every stage rejects a behind partition")
    void aBehindPartitionIsNotServed() {
        open();
        commit(put("k", "v"));
        store.put(0, put("k", "next"));
        assertThrows(OutcomeUnknown.class, () -> store.commit(() -> {
            throw new OutcomeUnknown(new RuntimeException("injected"));
        }));
        assertEquals(Collections.singleton(0), store.behind());

        assertThrows(PartitionBehindException.class, () -> store.get(0, "k"));
        assertThrows(PartitionBehindException.class, () -> store.iterator(0, null, null));
        assertThrows(PartitionBehindException.class, () -> store.prefixIterator(0, bytes("k")));
        assertThrows(PartitionBehindException.class, () -> store.reverseIterator(0, null, null));
        assertThrows(PartitionBehindException.class,
                () -> store.reversePrefixIterator(0, bytes("k")));
        assertThrows(PartitionBehindException.class, () -> store.put(0, put("k", "v")));
        assertThrows(PartitionBehindException.class,
                () -> store.deleteRange(0, bytes("a"), bytes("z")));
    }

    // --- support -------------------------------------------------------------

    private void open() {
        store = PartitionedStore.<String>builder()
                .options(fixture::optionsFor)
                .keyBytes(PartitionFixture.KEY_BYTES)
                .changelog(changelog())
                .restore((partition, materializedThrough, sink) -> {
                    resumePoints.add(materializedThrough);
                    log.restoreIn(8).restore(partition, materializedThrough, sink);
                })
                .build();
        store.assign(Collections.singletonList(0));
    }

    /** A changelog that can carry a range record; a lambda cannot, having only one method. */
    private Changelog<String> changelog() {
        return new Changelog<String>() {
            @Override
            public PendingPosition send(int partition, String key, Mutation mutation) {
                return log.send(partition, key, mutation);
            }

            @Override
            public PendingPosition sendDeleteRange(int partition, byte[] lower, byte[] upper) {
                return log.sendDeleteRange(partition, lower, upper);
            }
        };
    }

    private void commit(Map<String, Mutation> mutations) {
        store.put(0, mutations);
        store.commit(log::commitTransaction);
    }

    private static Map<String, Mutation> put(String key, String value) {
        return Collections.singletonMap(key, Mutation.put(bytes(value)));
    }

    /** The values a scan delivers, in order — for keys that are not printable. */
    private List<String> values(StagedIterator iterator) {
        List<String> delivered = new ArrayList<>();
        try (StagedIterator scan = iterator) {
            while (scan.next()) {
                delivered.add(string(scan.value()));
            }
            scan.status();
        }
        return delivered;
    }

    /**
     * Drains a scan into {@code key=value} in delivery order, closing it — an outstanding iterator
     * fails the store's close. A list rather than a map, so that the same key delivered twice is a
     * failure rather than an overwrite.
     */
    private List<String> scan(StagedIterator iterator) {
        List<String> delivered = new ArrayList<>();
        try (StagedIterator scan = iterator) {
            while (scan.next()) {
                delivered.add(string(scan.key()) + "=" + string(scan.value()));
            }
            scan.status();
        }
        return delivered;
    }
}
