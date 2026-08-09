package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 1 of the adapter: a {@link KeyValueStore} backed by ElysiumKV with Streams' changelog and
 * restore semantics untouched.
 *
 * <p>These are store-contract cases rather than topology cases — what Streams asks of a bytes store,
 * asked directly. The parts that need a running broker (restore from a real changelog, rebalance
 * behaviour) belong to the integration matrix and are not runnable offline.
 */
class ElysiumKVKeyValueStoreTest {
    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private KeyValueStore<Bytes, byte[]> open(Path dir) {
        // The plain variant: these tests init the store as its own root, which is not a timestamped
        // chain, and the timestamped variant refuses that arrangement rather than run in it.
        KeyValueStore<Bytes, byte[]> store =
                ElysiumKVKeyValueBytesStoreSupplier.plain("state", ElysiumKVStoreConfig.local())
                        .get();
        MockProcessorContext<Object, Object> context = new MockProcessorContext<>(
                new java.util.Properties() {{
                    setProperty("application.id", "test");
                    setProperty("bootstrap.servers", "localhost:9092");
                }}, new org.apache.kafka.streams.processor.TaskId(0, 0), dir.toFile());
        store.init(context.getStateStoreContext(), store);
        return store;
    }

    @Test
    void putGetDeleteRoundTrip(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            assertNull(store.get(key("absent")), "absence is null, and only absence");

            store.put(key("a"), value("1"));
            assertEquals("1", string(store.get(key("a"))));

            // A null value is a delete, which is Streams' convention rather than the engine's.
            store.put(key("a"), null);
            assertNull(store.get(key("a")));

            store.put(key("b"), value("2"));
            assertEquals("2", string(store.delete(key("b"))), "delete returns what was there");
            assertNull(store.get(key("b")));
        } finally {
            store.close();
        }
    }

    @Test
    void putIfAbsentDoesNotOverwrite(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            assertNull(store.putIfAbsent(key("a"), value("first")));
            assertEquals("first", string(store.putIfAbsent(key("a"), value("second"))));
            assertEquals("first", string(store.get(key("a"))));
        } finally {
            store.close();
        }
    }

    /**
     * {@code reverseRange} and {@code reverseAll} have defaults that throw, so a caller asking for
     * descending order gets an exception rather than an answer unless the store implements them.
     * Asserted against the ascending scan reversed — a decreasing sequence is not enough, since a
     * scan that dropped an entry would still be decreasing.
     */
    @Test
    void reverseScansAreTheForwardScansReversed(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        for (int i = 0; i < 200; ++i) {
            store.put(key(String.format("k%03d", i)), value("v" + i));
        }

        List<String> ascending = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
            while (it.hasNext()) ascending.add(string(it.next().key.get()));
        }
        List<String> descending = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> it = store.reverseAll()) {
            while (it.hasNext()) descending.add(string(it.next().key.get()));
        }
        Collections.reverse(descending);
        assertEquals(ascending, descending);
        assertEquals(200, ascending.size());

        // And the same for a bounded scan, where Streams' range is inclusive at both ends.
        List<String> range = new ArrayList<>();
        try (KeyValueIterator<Bytes, byte[]> it = store.reverseRange(key("k010"), key("k012"))) {
            while (it.hasNext()) range.add(string(it.next().key.get()));
        }
        assertEquals(List.of("k012", "k011", "k010"), range);
        store.close();
    }

    @Test
    void putAllLandsAsOneBatch(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
            for (int i = 0; i < 100; ++i) {
                entries.add(KeyValue.pair(key(String.format("k%03d", i)), value("v" + i)));
            }
            store.putAll(entries);
            for (int i = 0; i < 100; ++i) {
                assertEquals("v" + i, string(store.get(key(String.format("k%03d", i)))));
            }
        } finally {
            store.close();
        }
    }

    /**
     * Streams' {@code range} is inclusive at both ends and the engine's is upper-exclusive, so the
     * adapter has to bridge them. Getting it wrong drops the last key of every range — silently, and
     * only for callers who happen to ask for one that ends on a real key.
     */
    @Test
    void rangeIsInclusiveAtBothEnds(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            for (String k : Arrays.asList("a", "b", "c", "d", "e")) {
                store.put(key(k), value(k));
            }

            List<String> seen = new ArrayList<>();
            try (KeyValueIterator<Bytes, byte[]> it = store.range(key("b"), key("d"))) {
                while (it.hasNext()) {
                    seen.add(string(it.next().value));
                }
            }
            assertEquals(Arrays.asList("b", "c", "d"), seen, "both ends inclusive");
        } finally {
            store.close();
        }
    }

    @Test
    void allReturnsEverythingInKeyOrder(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            for (int i = 9; i >= 0; --i) {
                store.put(key("k" + i), value("v" + i));
            }
            List<String> seen = new ArrayList<>();
            try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
                while (it.hasNext()) {
                    seen.add(string(it.next().key.get()));
                }
            }
            assertEquals(10, seen.size());
            List<String> sorted = new ArrayList<>(seen);
            java.util.Collections.sort(sorted);
            assertEquals(sorted, seen, "keys are ordered as unsigned bytes");
        } finally {
            store.close();
        }
    }

    /**
     * The iterator hands back copies. The engine's buffers point into pinned blocks and are only
     * valid until the next advance, while Streams' contract lets a caller keep what it was given —
     * so a window handed straight through would decay under the holder.
     */
    @Test
    void iteratorValuesSurviveAdvancing(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            for (int i = 0; i < 20; ++i) {
                store.put(key(String.format("k%02d", i)), value("v" + i));
            }
            List<KeyValue<Bytes, byte[]>> held = new ArrayList<>();
            try (KeyValueIterator<Bytes, byte[]> it = store.all()) {
                while (it.hasNext()) {
                    held.add(it.next());
                }
            }
            assertEquals(20, held.size());
            for (int i = 0; i < 20; ++i) {
                assertEquals("v" + i, string(held.get(i).value),
                             "a value collected earlier must still read correctly");
            }
        } finally {
            store.close();
        }
    }

    @Test
    void approximateNumEntriesIsAnUpperBound(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        try {
            for (int i = 0; i < 50; ++i) {
                store.put(key("k" + i), value("v"));
            }
            assertEquals(50L, store.approximateNumEntries());

            // Overwriting every key: the superseded records are still there until compaction.
            for (int round = 0; round < 3; ++round) {
                for (int i = 0; i < 50; ++i) {
                    store.put(key("k" + i), value("round" + round));
                }
                store.flush();
            }
            assertTrue(store.approximateNumEntries() >= 50L,
                       "an upper bound, never below the true live-key count");
        } finally {
            store.close();
        }
    }

    @Test
    void theStoreIsPersistentAndReportsItsState(@TempDir Path dir) {
        KeyValueStore<Bytes, byte[]> store = open(dir);
        assertTrue(store.persistent());
        assertTrue(store.isOpen());
        assertEquals("state", store.name());
        store.close();
        assertTrue(!store.isOpen());
    }

    /**
     * Hybrid is refused at construction rather than degraded. Without store-managed offsets, Streams'
     * checkpoint file assumes local state is durable, so after losing the transient tier it would
     * resume from an offset whose state lived only there — and the store cannot correct that from
     * inside.
     */
    @Test
    void hybridModeIsRefusedUntilStoreManagedOffsetsExist() {
        UnsupportedOperationException error = assertThrows(
                UnsupportedOperationException.class, () -> ElysiumKVStoreConfig.hybrid(d -> null));
        assertTrue(error.getMessage().contains("KIP-1035"),
                   "the refusal should say what is missing: " + error.getMessage());
    }
}
