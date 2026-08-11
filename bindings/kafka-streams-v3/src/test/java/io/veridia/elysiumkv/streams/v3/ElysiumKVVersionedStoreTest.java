package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The versioned store as a store. What distinguishes it from a key-value store is that a write does
 * not replace what was there — it adds a version — so every case here is about which version a read
 * is entitled to.
 */
class ElysiumKVVersionedStoreTest {
    private static final Duration RETENTION = Duration.ofMinutes(10);

    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A versioned bytes store answers in `timestamp ‖ value` form. */
    private static String valueOf(byte[] timestamped) {
        if (timestamped == null) return null;
        return new String(timestamped, 8, timestamped.length - 8, StandardCharsets.UTF_8);
    }

    private static long timestampOf(byte[] timestamped) {
        return ByteBuffer.wrap(timestamped, 0, 8).getLong();
    }

    private ElysiumKVVersionedStore open(Path dir) {
        ElysiumKVVersionedStore store = (ElysiumKVVersionedStore) ElysiumKVVersionedBytesStoreSupplier
                .of("versioned", ElysiumKVStoreConfig.local(), RETENTION).get();
        MockProcessorContext<Object, Object> context = new MockProcessorContext<>(
                new Properties() {{
                    setProperty("application.id", "test");
                    setProperty("bootstrap.servers", "localhost:9092");
                }}, new TaskId(0, 0), dir.toFile());
        store.init(context.getStateStoreContext(), store);
        return store;
    }

    @Test
    void aLaterWriteDoesNotDestroyTheEarlierOne(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("first"), 1_000L);
        store.put(key("k"), value("second"), 5_000L);

        assertEquals("second", valueOf(store.get(key("k"))), "the current value");
        assertEquals("second", valueOf(store.get(key("k"), 9_000L)), "after the second write");
        assertEquals("first", valueOf(store.get(key("k"), 3_000L)), "between the two");
        assertEquals("first", valueOf(store.get(key("k"), 1_000L)), "exactly at the first");
        assertNull(store.get(key("k"), 999L), "before the key existed");
        store.close();
    }

    @Test
    void theTimestampComesBackWithTheValue(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("v"), 4_321L);

        assertEquals(4_321L, timestampOf(store.get(key("k"))));
        assertEquals(4_321L, timestampOf(store.get(key("k"), 9_000L)));
        store.close();
    }

    /** An out-of-order write is a version, not a mistake, and it must not disturb the current one. */
    @Test
    void anOutOfOrderWriteLandsInHistoryAndLeavesTheCurrentValueAlone(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("newest"), 5_000L);
        store.put(key("k"), value("older"), 2_000L);

        assertEquals("newest", valueOf(store.get(key("k"))), "the current value is unchanged");
        assertEquals("older", valueOf(store.get(key("k"), 3_000L)));
        assertNull(store.get(key("k"), 1_999L));
        store.close();
    }

    /** put reports when the version it wrote stops applying, which is the next version's time. */
    @Test
    void putReportsWhenTheVersionStopsApplying(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);

        assertEquals(VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED,
                     store.put(key("k"), value("a"), 5_000L),
                     "the newest version applies until further notice");

        assertEquals(5_000L, store.put(key("k"), value("b"), 2_000L),
                     "an older version applies until the one that superseded it");

        assertEquals(2_000L, store.put(key("k"), value("c"), 1_000L),
                     "and until the next one after it, not the current value");
        store.close();
    }

    /** A delete is a version too: reads before it still see what was there. */
    @Test
    void aDeleteIsAVersionRatherThanAnErasure(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("live"), 1_000L);

        final byte[] removed = store.delete(key("k"), 5_000L);

        assertEquals("live", valueOf(removed), "delete hands back what it replaced");
        assertNull(store.get(key("k")), "the key has no current value");
        assertNull(store.get(key("k"), 6_000L), "nor after the delete");
        assertEquals("live", valueOf(store.get(key("k"), 3_000L)), "but before it, it was there");
        store.close();
    }

    /**
     * <b>A write older than the retention is refused, not stored.</b> There is nowhere to put it
     * that a read could reach, and the API has a return code that says so — a silent drop would be
     * indistinguishable from a write that landed.
     */
    @Test
    void aWriteOlderThanTheRetentionIsRefused(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("now"), Duration.ofMinutes(30).toMillis());

        assertEquals(VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT,
                     store.put(key("k"), value("ancient"), 1_000L));
        assertNull(store.get(key("k"), 2_000L));
        store.close();
    }

    /**
     * <b>The current value outlives the history retention.</b> A key written once and never touched
     * again is still readable however far stream time has advanced — which is why the two live in
     * separate keyspaces and only one of them is expired.
     */
    @Test
    void theCurrentValueSurvivesLongAfterItsHistoryIsGone(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("written-once"), 1_000L);
        store.put(key("k"), value("superseded"), 2_000L);

        // Stream time runs far past the retention on another key entirely.
        store.put(key("other"), value("v"), Duration.ofHours(2).toMillis());

        assertEquals("superseded", valueOf(store.get(key("k"))),
                     "the current value must not be expired, however old it is");
        assertNull(store.get(key("k"), 1_500L), "its history, however, is gone");
        store.close();
    }

    /** The plain key-value surface is the versioned one at the current time. */
    @Test
    void thePlainSurfaceAgreesWithTheVersionedOne(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("a"), value("1"), 1_000L);
        store.put(key("b"), value("2"), 2_000L);

        assertEquals("1", valueOf(store.get(key("a"))));
        int seen = 0;
        try (org.apache.kafka.streams.state.KeyValueIterator<Bytes, byte[]> it = store.all()) {
            while (it.hasNext()) {
                it.next();
                ++seen;
            }
        }
        assertEquals(2, seen, "both current values, and no history entries");
        store.close();
    }

    /** A deleted key is not part of the current keyspace, however much history it has. */
    @Test
    void aDeletedKeyDoesNotAppearInAScan(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("a"), value("1"), 1_000L);
        store.put(key("b"), value("2"), 1_000L);
        store.delete(key("a"), 2_000L);

        int seen = 0;
        try (org.apache.kafka.streams.state.KeyValueIterator<Bytes, byte[]> it = store.all()) {
            while (it.hasNext()) {
                assertNotEquals("a", new String(it.next().key.get(), StandardCharsets.UTF_8));
                ++seen;
            }
        }
        assertEquals(1, seen);
        store.close();
    }

    /**
     * <b>An {@code asOf} far in the future must answer, not hang.</b> Both history scans walk one
     * segment at a time, so an unclamped bound is not a slow query: {@link Long#MAX_VALUE} names a
     * segment around 10^13 and the loop would open an iterator for each. This is the same defect
     * the window store had, and it fails as a timeout rather than an assertion.
     */
    @Test
    void anAbsentKeyIsAbsentAtEveryTime(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        assertNull(store.get(key("missing")));
        assertNull(store.get(key("missing"), 0L));
        assertNull(store.get(key("missing"), Long.MAX_VALUE));
        store.close();
    }

    @Test
    void aFarFutureAsOfReturnsTheCurrentValue(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("v"), 1_000L);
        assertEquals("v", valueOf(store.get(key("k"), Long.MAX_VALUE)));
        store.close();
    }

    /**
     * <b>A key must not see the history of a key that extends it.</b> User keys are
     * variable-length, so a history scan bounded by {@code key ‖ ts} over-selects: a longer key's
     * extra bytes occupy the positions the shorter key's timestamp does, and if they are small
     * enough to look like a timestamp in range, its entries land inside the scan.
     *
     * <p>Which is why {@code SHORT} and {@code LONGER} below are byte keys rather than the obvious
     * {@code "a"} and {@code "ab"}: with ASCII, {@code 'b'} is 0x62 and a millisecond timestamp
     * leaves its high bytes zero, so {@code ab}'s entries sort past the end of {@code a}'s range
     * and the bug cannot be reached. The first draft of this test used exactly that pair, and
     * passed with the guard removed. Streams keys are arbitrary serialized bytes, so the low-byte
     * case is not exotic — a composite serde produces it readily.
     */
    private static final Bytes SHORT = Bytes.wrap(new byte[] {0x61});
    private static final Bytes LONGER =
            Bytes.wrap(new byte[] {0x61, 0, 0, 0, 0, 0, 0, 0, 0x05});

    @Test
    void aKeyDoesNotSeeTheHistoryOfAKeyThatExtendsIt(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        // Timestamps chosen so the longer key's entry sorts *above* the shorter key's inside the
        // shared range — a descending scan reaches it first, so an unguarded read returns it.
        store.put(SHORT, value("short-old"), 1L);
        store.put(SHORT, value("short-new"), 9_000L);
        store.put(LONGER, value("longer-old"), 2L);
        store.put(LONGER, value("longer-new"), 9_000L);

        assertEquals("short-old", valueOf(store.get(SHORT, 5_000L)), "its own history, not the other's");
        assertEquals("longer-old", valueOf(store.get(LONGER, 5_000L)));
        store.close();
    }

    /** The same over-selection on the ascending path, which decides how long a version stays valid. */
    @Test
    void validToIgnoresTheHistoryOfAKeyThatExtendsThisOne(@TempDir Path dir) {
        VersionedBytesStore store = open(dir);
        store.put(SHORT, value("current"), 9_000L);
        store.put(LONGER, value("noise"), 2L);
        store.put(LONGER, value("noise-new"), 9_000L);

        assertEquals(9_000L, store.put(SHORT, value("older"), 1L),
                     "valid until this key's own current value, not until the other key's version");
        store.close();
    }

    /**
     * <b>Real timestamps are epoch milliseconds, which is where a floor of zero stops being
     * harmless.</b> A history miss walks segments downwards, so a floor at 1970 means several
     * million iterator opens for a lookup that should touch two or three. The retention floor
     * bounds it; a regression here shows up as this test timing out rather than failing.
     */
    @Test
    void aMissAtEpochScaleTimestampsDoesNotWalkBackToNineteenSeventy(@TempDir Path dir) {
        final long now = 1_760_000_000_000L;
        ElysiumKVVersionedStore store = open(dir);
        store.put(key("k"), value("v"), now);
        store.flush();
        store.close();

        // Reopened, so the truncation point is unknown and only the retention bounds the scan.
        ElysiumKVVersionedStore reopened = open(dir);
        assertEquals("v", valueOf(reopened.get(key("k"))));
        assertNull(reopened.get(key("absent"), now - 1_000L), "a miss must not scan the epoch");

        // Asserted on the band rather than on elapsed time: a floor at zero spans some six million
        // segments here, but each is only an empty-range iterator, so it is slow rather than
        // hanging and a timeout would not reliably catch it.
        final long span = reopened.lastLiveSegment() - reopened.firstLiveSegment();
        assertTrue(span < 8, "the scan band should be a few segments, was " + (span + 1));
        reopened.close();
    }

    /**
     * <b>A reopened store remembers stream time.</b> Retention is measured from it and so are both
     * scan clamps, so a store that came back at zero would expire by a clock that restarted and
     * would look for history below segment zero — reporting an empty store that is not empty.
     */
    @Test
    void aReopenedStoreKeepsItsHistoryAndItsStreamTime(@TempDir Path dir) {
        // Stream time runs past the retention, so the reopened store has something to get wrong.
        VersionedBytesStore store = open(dir);
        store.put(key("k"), value("old"), 1_500_000L);
        store.put(key("k"), value("new"), 1_800_000L);
        store.flush();
        store.close();

        VersionedBytesStore reopened = open(dir);
        assertEquals("new", valueOf(reopened.get(key("k"))), "the current value survives");
        assertEquals("old", valueOf(reopened.get(key("k"), 1_600_000L)), "and so does its history");

        // Stream time came back with it, so a write far below the retention is still refused.
        assertEquals(VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT,
                     reopened.put(key("k"), value("ancient"), 1L));
        reopened.close();
    }
}
