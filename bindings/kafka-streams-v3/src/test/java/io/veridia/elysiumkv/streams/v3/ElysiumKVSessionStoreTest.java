package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The session store as a store — what Streams asks of it, asked directly. */
class ElysiumKVSessionStoreTest {
    private static final Duration RETENTION = Duration.ofMinutes(10);

    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Windowed<Bytes> session(String k, long start, long end) {
        return new Windowed<>(key(k), new SessionWindow(start, end));
    }

    private SessionStore<Bytes, byte[]> open(Path dir) {
        SessionStore<Bytes, byte[]> store = ElysiumKVSessionBytesStoreSupplier
                .of("sessions", ElysiumKVStoreConfig.local(), RETENTION).get();
        MockProcessorContext<Object, Object> context = new MockProcessorContext<>(
                new Properties() {{
                    setProperty("application.id", "test");
                    setProperty("bootstrap.servers", "localhost:9092");
                }}, new TaskId(0, 0), dir.toFile());
        store.init(context.getStateStoreContext(), store);
        return store;
    }

    private static List<String> drain(KeyValueIterator<Windowed<Bytes>, byte[]> it) {
        List<String> out = new ArrayList<>();
        try (KeyValueIterator<Windowed<Bytes>, byte[]> scan = it) {
            while (scan.hasNext()) {
                KeyValue<Windowed<Bytes>, byte[]> entry = scan.next();
                out.add(new String(entry.key.key().get(), StandardCharsets.UTF_8) + "["
                        + entry.key.window().start() + "," + entry.key.window().end() + "]="
                        + new String(entry.value, StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    @Test
    void putAndFetchASession(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 1_000L, 5_000L), value("v"));

        assertArrayEquals(value("v"), store.fetchSession(key("a"), 1_000L, 5_000L));
        assertNull(store.fetchSession(key("a"), 1_000L, 6_000L), "a different end is a different session");
        assertEquals(List.of("a[1000,5000]=v"), drain(store.fetch(key("a"))));
        store.close();
    }

    /** Sessions are ordered by end, then start — which is what makes "still open after T" a range. */
    @Test
    void sessionsComeBackOrderedByEndThenStart(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 3_000L, 9_000L), value("third"));
        store.put(session("a", 0L, 2_000L), value("first"));
        store.put(session("a", 1_000L, 9_000L), value("second"));

        assertEquals(List.of("a[0,2000]=first", "a[1000,9000]=second", "a[3000,9000]=third"),
                     drain(store.fetch(key("a"))));
        store.close();
    }

    /**
     * <b>The two-sided predicate.</b> A session qualifies when it ended at or after
     * {@code earliestEnd} <em>and</em> began at or before {@code latestStart} — ordering gives the
     * first half, and the second has to be filtered.
     */
    @Test
    void findSessionsAppliesBothBounds(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 0L, 1_000L), value("ended-too-early"));
        store.put(session("a", 2_000L, 8_000L), value("overlaps"));
        store.put(session("a", 9_000L, 9_500L), value("began-too-late"));

        assertEquals(List.of("a[2000,8000]=overlaps"),
                     drain(store.findSessions(key("a"), 5_000L, 5_000L)),
                     "ends at or after 5000 and starts at or before 5000");
        store.close();
    }

    @Test
    void findSessionsSpansAKeyRange(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 0L, 5_000L), value("a"));
        store.put(session("b", 0L, 5_000L), value("b"));
        store.put(session("c", 0L, 5_000L), value("c"));

        assertEquals(List.of("a[0,5000]=a", "b[0,5000]=b"),
                     drain(store.findSessions(key("a"), key("b"), 0L, Long.MAX_VALUE)));
        assertEquals(3, drain(store.findSessions(0L, Long.MAX_VALUE)).size());
        store.close();
    }

    /** Removal has to target the exact triple, which is why the key carries both timestamps. */
    @Test
    void removeTakesExactlyTheSessionNamed(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 0L, 5_000L), value("keep"));
        store.put(session("a", 0L, 9_000L), value("drop"));

        store.remove(session("a", 0L, 9_000L));

        assertEquals(List.of("a[0,5000]=keep"), drain(store.fetch(key("a"))));
        store.close();
    }

    @Test
    void aDescendingScanIsTheAscendingOneReversed(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        for (long at = 0; at < 300_000L; at += 20_000L) {
            store.put(session("a", at, at + 1_000L), value("v" + at));
        }

        List<String> ascending = drain(store.fetch(key("a")));
        List<String> descending = drain(store.backwardFetch(key("a")));
        Collections.reverse(descending);

        assertEquals(ascending, descending);
        store.close();
    }

    /** Retention drops whole segments, by one truncation rather than a delete per session. */
    @Test
    void sessionsFallOutOfRetentionAsStreamTimeAdvances(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 0L, 1_000L), value("old"));
        assertEquals(1, drain(store.fetch(key("a"))).size());

        store.put(session("a", Duration.ofMinutes(30).toMillis(),
                          Duration.ofMinutes(30).toMillis() + 1_000L), value("new"));

        assertEquals(1, drain(store.fetch(key("a"))).size(),
                     "the old session is outside retention and must be gone");
        store.close();
    }

    /**
     * <b>Retention is durable, not just hidden.</b> The scan clamps to live segments from an
     * in-memory field, so a store that never called {@code truncateBelow} would still <em>look</em>
     * expired — until it reopened and the field reset. Reopening is what separates the two: the
     * engine's truncation floor is in the manifest and survives, the clamp does not.
     */
    @Test
    void expiredSessionsStayGoneAcrossAReopen(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 0L, 1_000L), value("old"));
        store.put(session("a", Duration.ofMinutes(30).toMillis(),
                          Duration.ofMinutes(30).toMillis() + 1_000L), value("new"));
        store.flush();
        store.close();

        SessionStore<Bytes, byte[]> reopened = open(dir);
        assertEquals(1, drain(reopened.fetch(key("a"))).size(),
                     "the expired session must not come back when the in-memory clamp resets");
        reopened.close();
    }

    /** An open-ended query must terminate rather than enumerate segments that cannot exist. */
    @Test
    @org.junit.jupiter.api.Timeout(20)
    void anOpenEndedQueryTerminates(@TempDir Path dir) {
        SessionStore<Bytes, byte[]> store = open(dir);
        store.put(session("a", 0L, 1_000L), value("v"));

        assertEquals(1, drain(store.findSessions(key("a"), 0L, Long.MAX_VALUE)).size());
        assertEquals(1, drain(store.findSessions(0L, Long.MAX_VALUE)).size());
        store.close();
    }
}
