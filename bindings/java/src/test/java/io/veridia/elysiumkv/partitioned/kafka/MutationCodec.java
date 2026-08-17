package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.partitioned.Mutation;

/**
 * How a {@link Mutation} travels on a changelog topic — a worked example, not shipped API.
 *
 * <p>The one rule: {@link #encode} must never return {@code null}. A compacted topic retains delete
 * markers only for {@code delete.retention.ms}, so a partition away for longer resumes from its
 * watermark and never learns a key was deleted — the local store keeps the old value and no later
 * record contradicts it. A delete encoded as an ordinary value survives compaction as that key's
 * latest value, which is what makes an incremental restore sound.
 *
 * <p>{@link #decode} is given whatever the topic holds, including {@code null} for a real tombstone.
 * Failing on one is the correct response: it means the topic is not one this design can restore
 * incrementally, and learning that at replay time beats learning it as a resurrected key later.
 */
interface MutationCodec {
    /** Never {@code null}, whatever the mutation. */
    byte[] encode(Mutation mutation);

    /** @param value the record's value, {@code null} for a tombstone — which must fail */
    Mutation decode(byte[] value);
}
