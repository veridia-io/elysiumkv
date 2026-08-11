package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The window store as a store — what Streams asks of it, asked directly.
 *
 * <p>The interesting cases are the ones the layout makes possible and the ones it makes awkward:
 * retention dropping whole segments, and a scan whose time span crosses a segment boundary, since
 * the ordering inside a segment is key-major and the concatenation across segments is what has to
 * put the answer back in order.
 */
class ElysiumKVWindowStoreTest {
    private static final Duration RETENTION = Duration.ofMinutes(10);
    private static final Duration SEGMENT = Duration.ofMinutes(1);
    private static final Duration WINDOW = Duration.ofSeconds(10);

    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private WindowStore<Bytes, byte[]> open(Path dir, boolean retainDuplicates) {
        WindowStore<Bytes, byte[]> store = ElysiumKVWindowBytesStoreSupplier
                .plain("windows", ElysiumKVStoreConfig.local(), RETENTION, WINDOW, retainDuplicates)
                .get();
        MockProcessorContext<Object, Object> context = new MockProcessorContext<>(
                new Properties() {{
                    setProperty("application.id", "test");
                    setProperty("bootstrap.servers", "localhost:9092");
                }}, new TaskId(0, 0), dir.toFile());
        store.init(context.getStateStoreContext(), store);
        return store;
    }

    private WindowStore<Bytes, byte[]> open(Path dir) {
        return open(dir, false);
    }

    private static List<String> drain(WindowStoreIterator<byte[]> it) {
        List<String> out = new ArrayList<>();
        try (WindowStoreIterator<byte[]> scan = it) {
            while (scan.hasNext()) {
                KeyValue<Long, byte[]> entry = scan.next();
                out.add(entry.key + "=" + string(entry.value));
            }
        }
        return out;
    }

    private static List<String> drainWindowed(KeyValueIterator<Windowed<Bytes>, byte[]> it) {
        List<String> out = new ArrayList<>();
        try (KeyValueIterator<Windowed<Bytes>, byte[]> scan = it) {
            while (scan.hasNext()) {
                KeyValue<Windowed<Bytes>, byte[]> entry = scan.next();
                out.add(string(entry.key.key().get()) + "@" + entry.key.window().start() + "="
                        + string(entry.value));
            }
        }
        return out;
    }

    @Test
    void putAndFetchARange(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("v1"), 1_000L);
        store.put(key("a"), value("v2"), 2_000L);
        store.put(key("a"), value("v3"), 3_000L);
        store.put(key("b"), value("other"), 2_000L);

        assertEquals(List.of("1000=v1", "2000=v2", "3000=v3"),
                     drain(store.fetch(key("a"), 0L, 5_000L)));
        assertEquals(List.of("2000=v2"), drain(store.fetch(key("a"), 2_000L, 2_000L)),
                     "both bounds inclusive");
        assertArrayEquals(value("v2"), store.fetch(key("a"), 2_000L));
        assertNull(store.fetch(key("a"), 2_500L), "no window starts there");
        store.close();
    }

    /**
     * <b>The case the layout is built around.</b> Entries are grouped by segment first, so a scan
     * whose span crosses a boundary is several scans concatenated — and the concatenation is what
     * has to return them in time order.
     */
    @Test
    void aFetchCrossingSegmentBoundariesStaysInOrder(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        // One entry per 20s over five minutes: five segments at a one-minute interval.
        for (long at = 0; at < 300_000L; at += 20_000L) {
            store.put(key("a"), value("v" + at), at);
        }

        List<String> seen = drain(store.fetch(key("a"), 0L, 300_000L));

        assertEquals(15, seen.size());
        List<String> expected = new ArrayList<>();
        for (long at = 0; at < 300_000L; at += 20_000L) expected.add(at + "=v" + at);
        assertEquals(expected, seen, "concatenating segments must not disorder the result");
        store.close();
    }

    @Test
    void aDescendingFetchIsTheAscendingOneReversed(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        for (long at = 0; at < 300_000L; at += 20_000L) {
            store.put(key("a"), value("v" + at), at);
        }

        List<String> ascending = drain(store.fetch(key("a"), 0L, 300_000L));
        List<String> descending = drain(store.backwardFetch(key("a"), 0L, 300_000L));
        Collections.reverse(descending);

        assertEquals(ascending, descending);
        store.close();
    }

    /**
     * A key-range fetch sweeps whole keys inside each segment, so entries outside the time span
     * have to be filtered out rather than assumed absent.
     */
    @Test
    void aKeyRangeFetchFiltersOnTimeAsWellAsKey(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("early"), 1_000L);
        store.put(key("a"), value("wanted"), 30_000L);
        store.put(key("b"), value("wanted"), 30_000L);
        store.put(key("b"), value("late"), 55_000L);
        store.put(key("c"), value("outside-key"), 30_000L);

        List<String> seen = drainWindowed(store.fetch(key("a"), key("b"), 20_000L, 40_000L));

        assertEquals(List.of("a@30000=wanted", "b@30000=wanted"), seen,
                     "inside the key range and the time span, and nothing else");
        store.close();
    }

    @Test
    void fetchAllSpansKeysWithinTheTimeRange(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("1"), 1_000L);
        store.put(key("b"), value("2"), 2_000L);
        store.put(key("c"), value("3"), 90_000L);   // a later segment

        assertEquals(List.of("a@1000=1", "b@2000=2"),
                     drainWindowed(store.fetchAll(0L, 60_000L)));
        assertEquals(3, drainWindowed(store.all()).size());
        store.close();
    }

    /**
     * <b>Retention, which is the whole reason for the segment prefix.</b> Stream time advancing past
     * the retention period must make old windows unreadable — and it happens by one truncation, not
     * by a delete per key.
     */
    @Test
    void windowsFallOutOfRetentionAsStreamTimeAdvances(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("old"), 1_000L);
        assertEquals(List.of("1000=old"), drain(store.fetch(key("a"), 0L, 5_000L)));

        // Stream time jumps well past the retention period, so the first segment is dead.
        store.put(key("a"), value("new"), Duration.ofMinutes(30).toMillis());

        assertEquals(List.of(), drain(store.fetch(key("a"), 0L, 5_000L)),
                     "the old window is outside retention and must be gone");
        assertEquals(1, drain(store.fetch(key("a"), 0L, Duration.ofMinutes(31).toMillis())).size());
        store.close();
    }

    /** A record already outside the window is dropped rather than failing the task. */
    @Test
    void aRecordOlderThanRetentionIsDroppedNotRefused(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("now"), Duration.ofMinutes(30).toMillis());

        store.put(key("a"), value("far-too-late"), 1_000L);   // must not throw

        assertEquals(List.of(), drain(store.fetch(key("a"), 0L, 5_000L)));
        store.close();
    }

    /**
     * A stream-stream join stores several records under one key and timestamp and needs all of
     * them; an aggregation stores one and would treat a second as corruption.
     */
    @Test
    void retainDuplicatesDecidesWhetherASecondRecordReplacesTheFirst(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> replacing = open(dir.resolve("replacing"), false);
        replacing.put(key("a"), value("first"), 1_000L);
        replacing.put(key("a"), value("second"), 1_000L);
        assertEquals(List.of("1000=second"), drain(replacing.fetch(key("a"), 0L, 5_000L)));
        replacing.close();

        WindowStore<Bytes, byte[]> keeping = open(dir.resolve("keeping"), true);
        keeping.put(key("a"), value("first"), 1_000L);
        keeping.put(key("a"), value("second"), 1_000L);
        assertEquals(List.of("1000=first", "1000=second"),
                     drain(keeping.fetch(key("a"), 0L, 5_000L)),
                     "a join needs both sides of a duplicate, in arrival order");
        keeping.close();
    }

    @Test
    void aNullValueDeletesTheWindow(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("v"), 1_000L);
        store.put(key("a"), null, 1_000L);

        assertEquals(List.of(), drain(store.fetch(key("a"), 0L, 5_000L)));
        store.close();
    }

    @Test
    void anEmptyRangeYieldsNothingRatherThanFailing(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("v"), 1_000L);

        assertEquals(List.of(), drain(store.fetch(key("a"), 500_000L, 600_000L)));
        assertEquals(List.of(), drainWindowed(store.fetchAll(500_000L, 600_000L)));
        assertTrue(drainWindowed(store.fetch(key("x"), key("y"), 0L, 5_000L)).isEmpty());
        store.close();
    }

    /**
     * An open-ended upper bound must come back, not spin. A scan builds a range per segment it
     * spans, and {@code Long.MAX_VALUE} spans about ten to the fourteen of them — so this is a
     * termination test, not a correctness one. IQv2 asks exactly this when a query omits its bound.
     */
    @Test
    @org.junit.jupiter.api.Timeout(20)
    void anUnboundedTimeRangeTerminates(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("v"), 1_000L);

        assertEquals(List.of("1000=v"), drain(store.fetch(key("a"), 0L, Long.MAX_VALUE)));
        assertEquals(1, drainWindowed(store.fetchAll(0L, Long.MAX_VALUE)).size());
        assertEquals(1, drainWindowed(store.fetch(key("a"), key("z"), 0L, Long.MAX_VALUE)).size());
        store.close();
    }

    /**
     * A reopened store must report what is on disk, not what its in-memory clamps happen to say.
     *
     * <p>Both are derived from {@code observedStreamTime}, which starts empty — so before the store
     * seeded itself from disk, a reopen clamped every scan to segment zero and the store answered
     * nothing at all. Restore from a changelog masks it by replaying records; a store reopened on
     * existing local state has nothing to replay.
     */
    @Test
    void aReopenedStoreStillSeesWhatIsOnDisk(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("early"), 1_000L);
        store.put(key("a"), value("later"), Duration.ofMinutes(3).toMillis());
        store.flush();
        store.close();

        WindowStore<Bytes, byte[]> reopened = open(dir);
        assertEquals(2, drain(reopened.fetch(key("a"), 0L, Duration.ofMinutes(5).toMillis())).size(),
                     "a reopen must not clamp the store down to nothing");
        reopened.close();
    }

    @Test
    void backwardScansAgreeWithForwardOnes(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        for (long at = 0; at < 200_000L; at += 25_000L) {
            store.put(key("a"), value("a" + at), at);
            store.put(key("b"), value("b" + at), at);
        }

        List<String> forwardAll = drainWindowed(store.all());
        List<String> backwardAll = drainWindowed(store.backwardAll());
        Collections.reverse(backwardAll);
        assertEquals(forwardAll, backwardAll);

        List<String> forwardRange = drainWindowed(store.fetchAll(0L, 200_000L));
        List<String> backwardRange = drainWindowed(store.backwardFetchAll(0L, 200_000L));
        Collections.reverse(backwardRange);
        assertEquals(forwardRange, backwardRange);
        store.close();
    }

    /**
     * <b>Closing without an explicit flush must not lose the writes.</b> The engine has no
     * write-ahead log, so a {@code close()} that does not flush discards everything still in the
     * memtable — silently, which is the part that makes it dangerous. Streams flushes before it
     * closes, so no test that goes through a topology can see this; only closing the store directly
     * can, which is exactly what a caller outside Streams does.
     */
    @Test
    void closingWithoutAFlushKeepsTheWrites(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> store = open(dir);
        store.put(key("a"), value("v"), 1_000L);
        store.close();                       // deliberately no flush()

        WindowStore<Bytes, byte[]> reopened = open(dir);
        assertEquals(1, drain(reopened.fetch(key("a"), 0L, 5_000L)).size());
        reopened.close();
    }

}
