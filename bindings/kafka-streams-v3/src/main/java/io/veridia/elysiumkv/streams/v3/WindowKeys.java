package io.veridia.elysiumkv.streams.v3;

import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Bytes;

/**
 * The binary layout of a windowed key in this store.
 *
 * <pre>
 *   segmentId (8 B, big-endian) ‖ user key ‖ timestamp (8 B, big-endian) ‖ seqnum (4 B, big-endian)
 * </pre>
 *
 * <p><b>The suffix is not ours to choose.</b> {@code key ‖ timestamp ‖ seqnum} is what Streams'
 * change logger writes to the changelog, so restore hands those exact bytes back. Matching it means
 * a restored record needs a segment prefix and nothing else — no decode, no re-encode. It is pinned
 * byte-for-byte against Kafka's own {@code WindowKeySchema} in the tests; the class itself is
 * internal, so this reimplements the layout rather than depending on it.
 *
 * <p><b>The prefix is ours, and it is the whole design.</b> Kafka's stores put the user key first
 * and partition time across separate physical stores — one RocksDB per segment — because retention
 * then means dropping a store. That costs an instance, and its threads, per segment per partition.
 * Leading with the segment id instead makes every expired entry one contiguous run at the bottom of
 * the keyspace, so retention is a single {@code truncateBelow} against one store. What a fetch pays
 * for that is a scan per segment its time range spans, which is two or three for a window much
 * shorter than the segment interval — the ordinary case.
 *
 * <p>Timestamps are written signed, as Kafka writes them, so the ordering these keys imply is the
 * ordering of the numbers only for non-negative times. Streams does not produce negative window
 * starts, and matching Kafka's encoding matters more than defending against one that cannot occur.
 */
final class WindowKeys {
    static final int TIMESTAMP_BYTES = 8;
    static final int SEQNUM_BYTES = 4;
    /** What the change logger appends to a user key: timestamp and sequence number. */
    static final int SUFFIX_BYTES = TIMESTAMP_BYTES + SEQNUM_BYTES;
    static final int SEGMENT_BYTES = 8;

    private WindowKeys() {}

    static long segmentId(long timestamp, long segmentIntervalMs) {
        return timestamp / segmentIntervalMs;
    }

    /** The lowest key any entry of this segment can have — also the argument to a truncation. */
    static byte[] segmentPrefix(long segmentId) {
        return ByteBuffer.allocate(SEGMENT_BYTES).putLong(segmentId).array();
    }

    /** A complete store key, as {@code put} writes it. */
    static Bytes storeKey(Bytes key, long timestamp, int seqnum, long segmentIntervalMs) {
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + key.get().length + SUFFIX_BYTES)
                                  .putLong(segmentId(timestamp, segmentIntervalMs))
                                  .put(key.get())
                                  .putLong(timestamp)
                                  .putInt(seqnum)
                                  .array());
    }

    /**
     * A changelog key — {@code key ‖ timestamp ‖ seqnum} — turned into a store key.
     *
     * <p>The prefix is all that is added, which is the point of matching the suffix layout: restore
     * copies bytes rather than parsing and rebuilding them.
     */
    static Bytes fromChangelogKey(byte[] changelogKey, long segmentIntervalMs) {
        final long timestamp = timestampOf(changelogKey);
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + changelogKey.length)
                                  .putLong(segmentId(timestamp, segmentIntervalMs))
                                  .put(changelogKey)
                                  .array());
    }

    /** The timestamp of a changelog key, or of a store key — both carry it in the last 12 bytes. */
    static long timestampOf(byte[] key) {
        return ByteBuffer.wrap(key, key.length - SUFFIX_BYTES, TIMESTAMP_BYTES).getLong();
    }

    /** The user key of a store key: everything between the segment prefix and the suffix. */
    static Bytes userKeyOf(byte[] storeKey) {
        final int length = storeKey.length - SEGMENT_BYTES - SUFFIX_BYTES;
        final byte[] key = new byte[length];
        System.arraycopy(storeKey, SEGMENT_BYTES, key, 0, length);
        return Bytes.wrap(key);
    }

    /**
     * The first key a scan for {@code key} in this segment could match.
     *
     * <p>Both bounds are built at the exact width of a real store key, so the comparison against
     * stored keys is a plain byte comparison with nothing to reason about at the boundary.
     */
    static Bytes lowerRange(long segmentId, Bytes key, long timeFrom) {
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + key.get().length + SUFFIX_BYTES)
                                  .putLong(segmentId)
                                  .put(key.get())
                                  .putLong(Math.max(timeFrom, 0L))
                                  .putInt(0)
                                  .array());
    }

    /** The last key such a scan could match — inclusive, hence the maximal sequence number. */
    static Bytes upperRange(long segmentId, Bytes key, long timeTo) {
        return Bytes.wrap(ByteBuffer.allocate(SEGMENT_BYTES + key.get().length + SUFFIX_BYTES)
                                  .putLong(segmentId)
                                  .put(key.get())
                                  .putLong(timeTo)
                                  .putInt(Integer.MAX_VALUE)
                                  .array());
    }

    /** Every entry of a segment, for the scans that do not name a key. */
    static Bytes segmentLowerBound(long segmentId) {
        return Bytes.wrap(segmentPrefix(segmentId));
    }

    static Bytes segmentUpperBound(long segmentId) {
        return Bytes.wrap(segmentPrefix(segmentId + 1));
    }
}
