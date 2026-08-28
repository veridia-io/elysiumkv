package io.veridia.elysiumkv.partitioned.kafka;

import java.util.Arrays;

/**
 * The shape of a changelog record, for both kinds — a worked example, not shipped API.
 *
 * <p>A range delete has no key of its own, and it cannot borrow one: compaction retains the newest
 * record per key, so a range record sharing a key with an entity would be superseded by the next
 * write to that entity, and a rebuild would then replay the puts the range was meant to cover. The
 * band would come back, silently. So every record key carries a tag and the two kinds live in
 * separate key spaces.
 *
 * <p>A range record's key is the bounds themselves rather than a hash or a counter, and both of those
 * alternatives are wrong in ways worth stating. A counter is never superseded, so every range delete
 * ever issued is retained and replayed on every cold rebuild. A hash — or the lower bound alone —
 * lets two different bands collide, and the later record then supersedes a deletion it does not
 * cover. Keying by the exact pair means only a repeat of the <em>same</em> band supersedes, which is
 * safe: the surviving record sits at the later offset and covers exactly what the dropped one did.
 *
 * <p>The tag costs a byte on every point record, so adopting this on a topic that already holds
 * untagged records means a re-key or a fresh topic. A caller whose key schema has a provably unused
 * namespace can put range records there instead and skip the tag.
 */
final class ChangelogRecords {

    private static final byte POINT = 0x00;
    private static final byte RANGE = 0x01;

    /** Non-null, because a Kafka tombstone is what {@link MutationCodec} exists to avoid. */
    static final byte[] RANGE_VALUE = {RANGE};

    private ChangelogRecords() {
    }

    static byte[] pointKey(byte[] key) {
        byte[] tagged = new byte[key.length + 1];
        tagged[0] = POINT;
        System.arraycopy(key, 0, tagged, 1, key.length);
        return tagged;
    }

    /** {@code RANGE | length(lower) | lower | upper}, which is injective over the pair. */
    static byte[] rangeKey(byte[] lower, byte[] upper) {
        byte[] tagged = new byte[1 + 4 + lower.length + upper.length];
        tagged[0] = RANGE;
        tagged[1] = (byte) (lower.length >>> 24);
        tagged[2] = (byte) (lower.length >>> 16);
        tagged[3] = (byte) (lower.length >>> 8);
        tagged[4] = (byte) lower.length;
        System.arraycopy(lower, 0, tagged, 5, lower.length);
        System.arraycopy(upper, 0, tagged, 5 + lower.length, upper.length);
        return tagged;
    }

    static boolean isRange(byte[] recordKey) {
        require(recordKey);
        return recordKey[0] == RANGE;
    }

    /** The entity key a point record carries, with the tag stripped. */
    static byte[] entityKey(byte[] recordKey) {
        require(recordKey);
        if (recordKey[0] != POINT) {
            throw new IllegalStateException("not a point record");
        }
        return Arrays.copyOfRange(recordKey, 1, recordKey.length);
    }

    static byte[] lowerBound(byte[] recordKey) {
        return Arrays.copyOfRange(recordKey, 5, 5 + boundLength(recordKey));
    }

    static byte[] upperBound(byte[] recordKey) {
        return Arrays.copyOfRange(recordKey, 5 + boundLength(recordKey), recordKey.length);
    }

    private static int boundLength(byte[] recordKey) {
        if (!isRange(recordKey) || recordKey.length < 5) {
            throw new IllegalStateException("not a range record");
        }
        int length = ((recordKey[1] & 0xFF) << 24) | ((recordKey[2] & 0xFF) << 16)
                | ((recordKey[3] & 0xFF) << 8) | (recordKey[4] & 0xFF);
        if (length < 0 || 5 + length > recordKey.length) {
            throw new IllegalStateException("a range record's lower bound runs past its key");
        }
        return length;
    }

    private static void require(byte[] recordKey) {
        if (recordKey == null || recordKey.length == 0) {
            throw new IllegalStateException(
                    "a changelog record with no key: this topic is not one an incremental restore "
                            + "can read, because neither kind of record can be identified");
        }
    }
}
