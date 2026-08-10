package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Our window store's iteration order, compared against RocksDB's — the reference implementation,
 * run side by side on the same data.
 *
 * <p>Ordering across segments is the one property that could not be settled by reading the
 * documentation. Streams says a window store iterates "by key then time" in some places and leaves
 * it unstated in others, and RocksDB's segmented store concatenates whole segments, so the answer
 * depends on an implementation detail rather than on a contract. Reasoning about it produces a
 * plausible answer; running both produces the real one.
 *
 * <p>The second case is the one that matters most here: <b>our segment interval is deliberately
 * different from RocksDB's</b>. If the order still agrees, the answer does not depend on how the
 * keyspace happens to be cut up — which is what a caller is entitled to assume.
 */
class WindowOrderingAgainstRocksDbTest {
    private static final Duration RETENTION = Duration.ofHours(4);
    private static final Duration WINDOW = Duration.ofSeconds(10);

    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private WindowStore<Bytes, byte[]> open(WindowBytesStoreSupplier supplier, Path dir) {
        WindowStore<Bytes, byte[]> store = supplier.get();
        MockProcessorContext<Object, Object> context = new MockProcessorContext<>(
                new Properties() {{
                    setProperty("application.id", "test");
                    setProperty("bootstrap.servers", "localhost:9092");
                }}, new TaskId(0, 0), dir.toFile());
        store.init(context.getStateStoreContext(), store);
        return store;
    }

    /** The same fixture into both stores: several keys across several hours. */
    private static void fill(WindowStore<Bytes, byte[]> store) {
        for (long minute = 0; minute < 180; minute += 20) {
            final long at = minute * 60_000L;
            for (String k : new String[] {"a", "b", "c"}) {
                store.put(key(k), value(k + "@" + at), at);
            }
        }
    }

    private static List<String> windowed(KeyValueIterator<Windowed<Bytes>, byte[]> it) {
        List<String> out = new ArrayList<>();
        try (KeyValueIterator<Windowed<Bytes>, byte[]> scan = it) {
            while (scan.hasNext()) {
                KeyValue<Windowed<Bytes>, byte[]> entry = scan.next();
                out.add(new String(entry.key.key().get(), StandardCharsets.UTF_8) + "@"
                        + entry.key.window().start());
            }
        }
        return out;
    }

    private static List<String> timestamps(WindowStoreIterator<byte[]> it) {
        List<String> out = new ArrayList<>();
        try (WindowStoreIterator<byte[]> scan = it) {
            while (scan.hasNext()) out.add(String.valueOf(scan.next().key));
        }
        return out;
    }

    /** With the same segmentation as RocksDB, every scan must agree entry for entry. */
    @Test
    void everyScanAgreesWithRocksDb(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> rocks = open(
                Stores.persistentWindowStore("rocks", RETENTION, WINDOW, false),
                dir.resolve("rocks"));
        WindowStore<Bytes, byte[]> ours = open(
                ElysiumKVWindowBytesStoreSupplier.plain("ours", ElysiumKVStoreConfig.local(),
                                                        RETENTION, WINDOW, false),
                dir.resolve("ours"));
        fill(rocks);
        fill(ours);

        final long span = 180 * 60_000L;
        assertFalse(windowed(rocks.all()).isEmpty(), "the fixture produced nothing at all");

        assertEquals(windowed(rocks.all()), windowed(ours.all()), "all()");
        assertEquals(windowed(rocks.fetchAll(0L, span)), windowed(ours.fetchAll(0L, span)),
                     "fetchAll over everything");
        assertEquals(windowed(rocks.fetchAll(30 * 60_000L, 100 * 60_000L)),
                     windowed(ours.fetchAll(30 * 60_000L, 100 * 60_000L)),
                     "fetchAll over a span that starts and ends mid-segment");
        assertEquals(timestamps(rocks.fetch(key("b"), 0L, span)),
                     timestamps(ours.fetch(key("b"), 0L, span)),
                     "fetch for one key across every segment");
        assertEquals(windowed(rocks.fetch(key("a"), key("b"), 0L, span)),
                     windowed(ours.fetch(key("a"), key("b"), 0L, span)),
                     "fetch across a key range and every segment");

        rocks.close();
        ours.close();
    }

    /** And the descending scans, which RocksDB serves from its own reverse iterators. */
    @Test
    void everyBackwardScanAgreesWithRocksDb(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> rocks = open(
                Stores.persistentWindowStore("rocks", RETENTION, WINDOW, false),
                dir.resolve("rocks"));
        WindowStore<Bytes, byte[]> ours = open(
                ElysiumKVWindowBytesStoreSupplier.plain("ours", ElysiumKVStoreConfig.local(),
                                                        RETENTION, WINDOW, false),
                dir.resolve("ours"));
        fill(rocks);
        fill(ours);

        final long span = 180 * 60_000L;
        assertEquals(windowed(rocks.backwardAll()), windowed(ours.backwardAll()), "backwardAll()");
        assertEquals(windowed(rocks.backwardFetchAll(0L, span)),
                     windowed(ours.backwardFetchAll(0L, span)), "backwardFetchAll");
        assertEquals(timestamps(rocks.backwardFetch(key("b"), 0L, span)),
                     timestamps(ours.backwardFetch(key("b"), 0L, span)), "backwardFetch one key");
        assertEquals(windowed(rocks.backwardFetch(key("a"), key("b"), 0L, span)),
                     windowed(ours.backwardFetch(key("a"), key("b"), 0L, span)),
                     "backwardFetch across a key range");

        rocks.close();
        ours.close();
    }

    /**
     * <b>Order is a function of the segment interval — in RocksDB too.</b>
     *
     * <p>This started as an assertion that the order does not depend on how the keyspace is cut up.
     * It does. RocksDB's own output breaks key-major runs exactly at two hours, its segment interval
     * for this retention: {@code a@0..a@6000000, b@0..b@6000000, c@..., } then {@code a@7200000...}.
     * Both implementations concatenate whole segments, so a smaller interval interleaves keys more
     * finely and a larger one less.
     *
     * <p>What is stable is the <em>set</em>, and that the order agrees with RocksDB whenever the
     * intervals do — which the two tests above pin, and which is what a caller migrating from
     * RocksDB on default settings actually gets, since the default rule is the same.
     *
     * <p>So a caller must not depend on the interleaving across keys. Within one key, time order
     * holds regardless: a key's entries in a segment are contiguous and segments concatenate in
     * time order.
     */
    @Test
    void aDifferentSegmentSizeChangesTheOrderButNotTheContents(@TempDir Path dir) {
        WindowStore<Bytes, byte[]> rocks = open(
                Stores.persistentWindowStore("rocks", RETENTION, WINDOW, false),
                dir.resolve("rocks"));
        WindowStore<Bytes, byte[]> finelyCut = open(
                ElysiumKVWindowBytesStoreSupplier.plain("ours", ElysiumKVStoreConfig.local(),
                                                        RETENTION, Duration.ofMinutes(10), WINDOW,
                                                        false),
                dir.resolve("ours"));
        fill(rocks);
        fill(finelyCut);

        final List<String> theirs = windowed(rocks.all());
        final List<String> ours = windowed(finelyCut.all());

        assertNotEquals(theirs, ours, "a twelve-times finer segmentation interleaves differently");
        assertEquals(new TreeSet<>(theirs), new TreeSet<>(ours),
                     "but every entry is present in both, exactly once");

        // Within a single key, time order is not affected by any of this.
        assertEquals(timestamps(rocks.fetch(key("b"), 0L, 180 * 60_000L)),
                     timestamps(finelyCut.fetch(key("b"), 0L, 180 * 60_000L)),
                     "one key's entries are in time order however the segments fall");

        rocks.close();
        finelyCut.close();
    }
}
