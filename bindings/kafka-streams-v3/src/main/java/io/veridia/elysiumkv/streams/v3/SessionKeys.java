package io.veridia.elysiumkv.streams.v3;

import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Bytes;

/**
 * The binary layout of a session key in this store.
 *
 * <pre>
 *   segmentId (8 B, big-endian) ‖ user key ‖ end (8 B, big-endian) ‖ start (8 B, big-endian)
 * </pre>
 *
 * <p>As with {@link WindowKeys}, the suffix is Kafka's — {@code key ‖ end ‖ start} — because the
 * changelog is written by Streams and replayed back into this store, and it is pinned byte-for-byte
 * against {@code SessionKeySchema} in the tests.
 *
 * <p>End before start, and the segment taken from the end: a session grows by having its end
 * pushed out, and queries ask which sessions were still open after T, so ordering by end makes that
 * a range rather than a scan. Retention then drops a session when the last thing that happened in
 * it falls out of the window, not when it began.
 */
final class SessionKeys {
    static final int TIMESTAMP_BYTES = 8;
    /** What the change logger appends to a user key: the end and then the start. */
    static final int SUFFIX_BYTES = TIMESTAMP_BYTES * 2;
    static final int SEGMENT_BYTES = 8;

    private SessionKeys() {}

    static long segmentId(long endTime, long segmentIntervalMs) {
        return endTime / segmentIntervalMs;
    }

    static byte[] segmentPrefix(long segmentId) {
        return ByteBuffer.allocate(SEGMENT_BYTES).putLong(segmentId).array();
    }

    static Bytes storeKey(Bytes key, long start, long end, long segmentIntervalMs) {
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + key.get().length + SUFFIX_BYTES)
                                  .putLong(segmentId(end, segmentIntervalMs))
                                  .put(key.get())
                                  .putLong(end)
                                  .putLong(start)
                                  .array());
    }

    /** A changelog key — {@code key ‖ end ‖ start} — with only a segment prefix added. */
    static Bytes fromChangelogKey(byte[] changelogKey, long segmentIntervalMs) {
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + changelogKey.length)
                                  .putLong(segmentId(endOf(changelogKey), segmentIntervalMs))
                                  .put(changelogKey)
                                  .array());
    }

    /** Reads the end from a changelog key or a store key — both carry it 16 bytes from the end. */
    static long endOf(byte[] key) {
        return ByteBuffer.wrap(key, key.length - SUFFIX_BYTES, TIMESTAMP_BYTES).getLong();
    }

    static long startOf(byte[] key) {
        return ByteBuffer.wrap(key, key.length - TIMESTAMP_BYTES, TIMESTAMP_BYTES).getLong();
    }

    static Bytes userKeyOf(byte[] storeKey) {
        final int length = storeKey.length - SEGMENT_BYTES - SUFFIX_BYTES;
        final byte[] key = new byte[length];
        System.arraycopy(storeKey, SEGMENT_BYTES, key, 0, length);
        return Bytes.wrap(key);
    }

    /** Every session for a key within a segment, ordered by end then start. */
    static Bytes lowerRange(long segmentId, Bytes key) {
        return bound(segmentId, key, 0L, 0L);
    }

    static Bytes upperRange(long segmentId, Bytes key) {
        return bound(segmentId, key, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    private static Bytes bound(long segmentId, Bytes key, long end, long start) {
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + key.get().length + SUFFIX_BYTES)
                                  .putLong(segmentId)
                                  .put(key.get())
                                  .putLong(end)
                                  .putLong(start)
                                  .array());
    }

    static Bytes segmentLowerBound(long segmentId) {
        return Bytes.wrap(segmentPrefix(segmentId));
    }

    static Bytes segmentUpperBound(long segmentId) {
        return Bytes.wrap(segmentPrefix(segmentId + 1));
    }
}
