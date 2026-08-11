package io.veridia.elysiumkv.streams.v3;

import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Bytes;

/**
 * The binary layout of a versioned key, which is two keyspaces in one store.
 *
 * <pre>
 *   history: 0x00 ‖ segmentId (8 B BE) ‖ user key ‖ timestamp (8 B BE)
 *   latest:  0x01 ‖ user key
 * </pre>
 *
 * <p><b>Why the split, and why history sorts first.</b> A versioned store keeps old versions only
 * for its history retention, but a key's <em>current</em> value must survive however long ago it was
 * written — a key written once and never touched again is still readable a year later. So the two
 * have different lifetimes and cannot share an expiry rule.
 *
 * <p>History is segmented by timestamp exactly as {@link WindowKeys} is, so expiring it is one
 * {@link io.veridia.elysiumkv.ElysiumKV#truncateBelow} rather than a delete per version. That is
 * also why history carries the low prefix byte: {@code truncateBelow} moves a floor up from the
 * bottom of the keyspace, so anything it must never touch has to sort <em>above</em> everything it
 * will. Putting latest values first would mean expiring the oldest history segment could only be
 * expressed as a floor above every latest value.
 *
 * <p>Values are stored in the timestamped format Streams uses — {@code timestamp ‖ value} — with a
 * leading byte distinguishing a real value from a deletion, since a versioned store records a
 * delete as a version rather than as an absence.
 */
final class VersionedKeys {
    static final byte HISTORY_PREFIX = 0x00;
    static final byte LATEST_PREFIX = 0x01;
    static final byte STREAM_TIME_PREFIX = 0x02;

    static final int TIMESTAMP_BYTES = 8;
    static final int SEGMENT_BYTES = 8;

    /** Marks a stored version as a real value or as a deletion. */
    static final byte VALUE = 0x00;
    static final byte TOMBSTONE = 0x01;

    private VersionedKeys() {}

    static long segmentId(long timestamp, long segmentIntervalMs) {
        return timestamp / segmentIntervalMs;
    }

    /**
     * Where the observed stream time is kept, so a reopened store recovers it exactly.
     *
     * <p>Retention is measured from stream time, so a store that came back not knowing it would
     * expire by a clock that restarted — and its scan clamps, which are derived from the same
     * field, would place the live band at segment zero. Deriving it from the data instead would
     * mean scanning every current value at open, since the newest write is not at any particular
     * key. It sorts above both subspaces so no truncation floor can reach it.
     */
    static byte[] streamTimeKey() {
        return new byte[] {STREAM_TIME_PREFIX};
    }

    static byte[] encodeStreamTime(long streamTime) {
        return ByteBuffer.allocate(TIMESTAMP_BYTES).putLong(streamTime).array();
    }

    static long decodeStreamTime(byte[] stored) {
        return ByteBuffer.wrap(stored, 0, TIMESTAMP_BYTES).getLong();
    }

    /** The key a current value lives at. */
    static Bytes latestKey(Bytes key) {
        return Bytes.wrap(ByteBuffer.allocate(1 + key.get().length)
                                  .put(LATEST_PREFIX)
                                  .put(key.get())
                                  .array());
    }

    /** The key a superseded version lives at. */
    static Bytes historyKey(Bytes key, long timestamp, long segmentIntervalMs) {
        return Bytes.wrap(ByteBuffer.allocate(1 + SEGMENT_BYTES + key.get().length + TIMESTAMP_BYTES)
                                  .put(HISTORY_PREFIX)
                                  .putLong(segmentId(timestamp, segmentIntervalMs))
                                  .put(key.get())
                                  .putLong(timestamp)
                                  .array());
    }

    /** Everything for one key in one history segment, oldest first. */
    static Bytes historyLowerBound(Bytes key, long segmentId, long fromTimestamp) {
        return historyBound(key, segmentId, Math.max(fromTimestamp, 0L));
    }

    static Bytes historyUpperBound(Bytes key, long segmentId, long toTimestamp) {
        return historyBound(key, segmentId, toTimestamp);
    }

    private static Bytes historyBound(Bytes key, long segmentId, long timestamp) {
        return Bytes.wrap(ByteBuffer.allocate(1 + SEGMENT_BYTES + key.get().length + TIMESTAMP_BYTES)
                                  .put(HISTORY_PREFIX)
                                  .putLong(segmentId)
                                  .put(key.get())
                                  .putLong(timestamp)
                                  .array());
    }

    /** The floor that expires every history segment below `segmentId`. */
    static byte[] historySegmentFloor(long segmentId) {
        return ByteBuffer.allocate(1 + SEGMENT_BYTES)
                .put(HISTORY_PREFIX)
                .putLong(segmentId)
                .array();
    }

    static long timestampOfHistoryKey(byte[] storeKey) {
        return ByteBuffer.wrap(storeKey, storeKey.length - TIMESTAMP_BYTES, TIMESTAMP_BYTES)
                .getLong();
    }

    /**
     * Whether a history entry belongs to this key rather than to one that merely starts with it.
     *
     * <p>User keys are variable-length, so a bounded scan over {@code key ‖ ts} cannot be trusted
     * to contain only that key: a longer key's extra bytes occupy the positions where the shorter
     * key's timestamp begins, and when they are small enough to read as a timestamp inside the
     * bounds, its entries fall in the range. Ordinary text keys happen to escape this — {@code 'b'}
     * is 0x62 and a millisecond timestamp leaves its high bytes zero, so {@code ab} sorts past the
     * end of {@code a}'s range — but Streams keys are arbitrary serialized bytes and a composite
     * serde produces low bytes readily. The bounds stay as they are, being still the smallest range
     * containing the answer; the entries they over-select are rejected here.
     */
    static boolean isHistoryEntryFor(byte[] storeKey, Bytes key) {
        final byte[] user = key.get();
        if (storeKey.length != 1 + SEGMENT_BYTES + user.length + TIMESTAMP_BYTES) return false;
        if (storeKey[0] != HISTORY_PREFIX) return false;
        return java.util.Arrays.equals(storeKey, 1 + SEGMENT_BYTES, 1 + SEGMENT_BYTES + user.length,
                                       user, 0, user.length);
    }

    // --- values ---------------------------------------------------------------

    /** {@code flag ‖ timestamp ‖ value}, the last two being what Streams expects back. */
    static byte[] encodeValue(byte[] value, long timestamp) {
        final boolean tombstone = value == null;
        final ByteBuffer buffer =
                ByteBuffer.allocate(1 + TIMESTAMP_BYTES + (tombstone ? 0 : value.length));
        buffer.put(tombstone ? TOMBSTONE : VALUE).putLong(timestamp);
        if (!tombstone) buffer.put(value);
        return buffer.array();
    }

    static boolean isTombstone(byte[] stored) {
        return stored.length > 0 && stored[0] == TOMBSTONE;
    }

    static long timestampOfValue(byte[] stored) {
        return ByteBuffer.wrap(stored, 1, TIMESTAMP_BYTES).getLong();
    }

    /**
     * The bytes Streams expects from a versioned bytes store: {@code timestamp ‖ value}, or null
     * for a deletion. The leading flag is ours and does not leave this class.
     */
    static byte[] toTimestampedFormat(byte[] stored) {
        if (stored == null || isTombstone(stored)) return null;
        final byte[] out = new byte[stored.length - 1];
        System.arraycopy(stored, 1, out, 0, out.length);
        return out;
    }
}
