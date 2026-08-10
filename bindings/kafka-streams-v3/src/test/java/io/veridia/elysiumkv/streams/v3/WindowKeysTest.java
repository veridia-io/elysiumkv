package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.WindowKeySchema;
import org.junit.jupiter.api.Test;

/**
 * The windowed key layout.
 *
 * <p>The suffix is checked against Kafka's own {@code WindowKeySchema} rather than against a
 * hand-written expectation. That class is internal, so the store reimplements the layout — and a
 * reimplementation that is merely self-consistent is worthless here: the changelog is written by
 * Streams and replayed into this store, so a suffix that disagrees with Kafka's by one byte
 * restores garbage rather than failing.
 */
class WindowKeysTest {
    private static final long SEGMENT_INTERVAL = 60_000L;

    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    /** The store key is Kafka's, with a segment prefix in front and nothing else changed. */
    @Test
    void theSuffixIsExactlyKafkasStoreKey() {
        for (long timestamp : new long[] {0L, 1L, 1_700_000_000_000L, Long.MAX_VALUE / 4}) {
            for (int seqnum : new int[] {0, 1, 42, Integer.MAX_VALUE}) {
                final Bytes ours = WindowKeys.storeKey(key("user-7"), timestamp, seqnum,
                                                       SEGMENT_INTERVAL);
                final byte[] kafkas =
                        WindowKeySchema.toStoreKeyBinary(key("user-7"), timestamp, seqnum).get();

                final byte[] withoutSegment = Arrays.copyOfRange(ours.get(),
                                                                 WindowKeys.SEGMENT_BYTES,
                                                                 ours.get().length);
                assertArrayEquals(kafkas, withoutSegment,
                                  "ts=" + timestamp + " seq=" + seqnum);
            }
        }
    }

    /**
     * Restore is handed changelog keys — Kafka's encoding — and has to place them where a later
     * fetch will look. Going through the changelog form must land on the same bytes as an ordinary
     * put, or a restored store answers differently from the one it replaced.
     */
    @Test
    void aRestoredChangelogKeyLandsWhereAPutWouldHavePutIt() {
        final long timestamp = 1_700_000_123_456L;
        final int seqnum = 9;

        final Bytes direct = WindowKeys.storeKey(key("user-7"), timestamp, seqnum, SEGMENT_INTERVAL);
        final byte[] changelogKey =
                WindowKeySchema.toStoreKeyBinary(key("user-7"), timestamp, seqnum).get();
        final Bytes restored = WindowKeys.fromChangelogKey(changelogKey, SEGMENT_INTERVAL);

        assertArrayEquals(direct.get(), restored.get());
    }

    @Test
    void aStoreKeyGivesBackWhatWentIntoIt() {
        final long timestamp = 1_700_000_123_456L;
        final Bytes stored = WindowKeys.storeKey(key("abc"), timestamp, 3, SEGMENT_INTERVAL);

        assertEquals(timestamp, WindowKeys.timestampOf(stored.get()));
        assertEquals(key("abc"), WindowKeys.userKeyOf(stored.get()));
    }

    /** An empty user key is a key. The suffix arithmetic must not assume otherwise. */
    @Test
    void anEmptyUserKeyRoundTrips() {
        final Bytes stored = WindowKeys.storeKey(Bytes.wrap(new byte[0]), 5L, 0, SEGMENT_INTERVAL);
        assertEquals(0, WindowKeys.userKeyOf(stored.get()).get().length);
        assertEquals(5L, WindowKeys.timestampOf(stored.get()));
    }

    /**
     * <b>The ordering the whole design rests on.</b> Sorted bytewise, entries must group by segment
     * first, then by user key, then by time — that is what makes expiry one contiguous run at the
     * bottom and a fetch one contiguous run inside each segment.
     */
    @Test
    void keysSortBySegmentThenKeyThenTime() {
        final List<Bytes> keys = new ArrayList<>();
        keys.add(WindowKeys.storeKey(key("b"), 30_000L, 0, SEGMENT_INTERVAL));   // segment 0
        keys.add(WindowKeys.storeKey(key("a"), 90_000L, 0, SEGMENT_INTERVAL));   // segment 1
        keys.add(WindowKeys.storeKey(key("a"), 10_000L, 0, SEGMENT_INTERVAL));   // segment 0
        keys.add(WindowKeys.storeKey(key("a"), 20_000L, 0, SEGMENT_INTERVAL));   // segment 0
        keys.add(WindowKeys.storeKey(key("a"), 20_000L, 1, SEGMENT_INTERVAL));   // dup, later seq

        final List<Bytes> sorted = new ArrayList<>(keys);
        sorted.sort(Bytes::compareTo);

        assertEquals(Arrays.asList(keys.get(2), keys.get(3), keys.get(4), keys.get(0), keys.get(1)),
                     sorted,
                     "segment 0 before segment 1; within a segment key before time; "
                             + "and a duplicate sorts after the entry it repeats");
    }

    /** A scan's bounds have to bracket exactly the entries it should see, at both edges. */
    @Test
    void theRangeBoundsBracketTheEntriesTheyShould() {
        final long segment = WindowKeys.segmentId(20_000L, SEGMENT_INTERVAL);
        final Bytes lower = WindowKeys.lowerRange(segment, key("a"), 10_000L);
        final Bytes upper = WindowKeys.upperRange(segment, key("a"), 30_000L);

        // Inside, including exactly on each bound.
        for (long at : new long[] {10_000L, 20_000L, 30_000L}) {
            final Bytes inside = WindowKeys.storeKey(key("a"), at, 0, SEGMENT_INTERVAL);
            assertTrue(lower.compareTo(inside) <= 0 && inside.compareTo(upper) <= 0,
                       "t=" + at + " should be inside the range");
        }
        // A duplicate at the upper bound still falls inside, which is why the bound carries the
        // maximal sequence number rather than zero.
        final Bytes duplicate = WindowKeys.storeKey(key("a"), 30_000L, 7, SEGMENT_INTERVAL);
        assertTrue(duplicate.compareTo(upper) <= 0);

        // Outside, on either side and for a neighbouring key.
        assertTrue(WindowKeys.storeKey(key("a"), 9_999L, 0, SEGMENT_INTERVAL).compareTo(lower) < 0);
        assertTrue(WindowKeys.storeKey(key("a"), 30_001L, 0, SEGMENT_INTERVAL).compareTo(upper) > 0);
        assertTrue(WindowKeys.storeKey(key("b"), 20_000L, 0, SEGMENT_INTERVAL).compareTo(upper) > 0);
    }

    /** A segment's own bounds have to enclose every key in it and nothing from the next one. */
    @Test
    void segmentBoundsEncloseTheirSegmentOnly() {
        final Bytes lower = WindowKeys.segmentLowerBound(1);
        final Bytes upper = WindowKeys.segmentUpperBound(1);   // exclusive

        final Bytes first = WindowKeys.storeKey(Bytes.wrap(new byte[0]), 60_000L, 0,
                                                SEGMENT_INTERVAL);
        final Bytes last = WindowKeys.storeKey(key("zzzz"), 119_999L, Integer.MAX_VALUE,
                                               SEGMENT_INTERVAL);
        final Bytes next = WindowKeys.storeKey(key("a"), 120_000L, 0, SEGMENT_INTERVAL);

        assertTrue(lower.compareTo(first) <= 0);
        assertTrue(last.compareTo(upper) < 0);
        assertTrue(next.compareTo(upper) >= 0, "the next segment starts at the exclusive bound");
    }

    @Test
    void theSegmentIdIsTheTimestampDividedByTheInterval() {
        assertEquals(0L, WindowKeys.segmentId(0L, SEGMENT_INTERVAL));
        assertEquals(0L, WindowKeys.segmentId(59_999L, SEGMENT_INTERVAL));
        assertEquals(1L, WindowKeys.segmentId(60_000L, SEGMENT_INTERVAL));
        assertEquals(28_333_335L, WindowKeys.segmentId(1_700_000_100_000L, SEGMENT_INTERVAL));
    }
}
