package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.SessionKeySchema;
import org.junit.jupiter.api.Test;

/**
 * The session key layout, checked against Kafka's own encoder rather than a hand-written
 * expectation — the changelog is written by Streams and replayed into this store, so a suffix that
 * disagrees restores garbage rather than failing.
 */
class SessionKeysTest {
    private static final long SEGMENT_INTERVAL = 60_000L;

    private static Bytes key(String s) {
        return Bytes.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void theSuffixIsExactlyKafkasSessionKey() {
        for (long start : new long[] {0L, 1_000L, 1_700_000_000_000L}) {
            for (long end : new long[] {start, start + 5_000L, start + 90_000L}) {
                final Bytes ours = SessionKeys.storeKey(key("user-7"), start, end, SEGMENT_INTERVAL);
                final byte[] kafkas = SessionKeySchema.toBinary(key("user-7"), start, end).get();

                assertArrayEquals(kafkas,
                                  Arrays.copyOfRange(ours.get(), SessionKeys.SEGMENT_BYTES,
                                                     ours.get().length),
                                  "start=" + start + " end=" + end);
            }
        }
    }

    @Test
    void aRestoredChangelogKeyLandsWhereAPutWouldHavePutIt() {
        final Bytes direct = SessionKeys.storeKey(key("k"), 1_000L, 9_000L, SEGMENT_INTERVAL);
        final byte[] changelog = SessionKeySchema.toBinary(key("k"), 1_000L, 9_000L).get();
        assertArrayEquals(direct.get(),
                          SessionKeys.fromChangelogKey(changelog, SEGMENT_INTERVAL).get());
    }

    @Test
    void aStoreKeyGivesBackWhatWentIntoIt() {
        final Bytes stored = SessionKeys.storeKey(key("abc"), 1_000L, 9_000L, SEGMENT_INTERVAL);
        assertEquals(9_000L, SessionKeys.endOf(stored.get()));
        assertEquals(1_000L, SessionKeys.startOf(stored.get()));
        assertEquals(key("abc"), SessionKeys.userKeyOf(stored.get()));
    }

    /** Sessions sort by end and then start, which is what makes "still open after T" a range. */
    @Test
    void sessionsSortByEndThenStart() {
        final Bytes early = SessionKeys.storeKey(key("a"), 0L, 1_000L, SEGMENT_INTERVAL);
        final Bytes laterEnd = SessionKeys.storeKey(key("a"), 0L, 2_000L, SEGMENT_INTERVAL);
        final Bytes sameEndLaterStart = SessionKeys.storeKey(key("a"), 500L, 2_000L,
                                                            SEGMENT_INTERVAL);
        assertEquals(-1, Integer.signum(early.compareTo(laterEnd)));
        assertEquals(-1, Integer.signum(laterEnd.compareTo(sameEndLaterStart)));
    }
}
