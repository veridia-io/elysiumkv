package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.partitioned.Mutation;

/**
 * A one-byte tag in front of the value: {@code 0x00} for a delete, {@code 0x01} for a put.
 *
 * <p>Deliberately the dullest encoding that satisfies the contract. A delete becomes a one-byte
 * record rather than a tombstone, which is the entire point — compaction keeps it as the key's
 * latest value instead of eventually erasing the fact of the delete.
 *
 * <p>It copies on both sides. A caller whose values already carry a discriminator should implement
 * {@link MutationCodec} over that instead and skip the copy.
 */
final class PrefixedMutationCodec implements MutationCodec {
    private static final byte DELETED = 0x00;
    private static final byte PRESENT = 0x01;
    private static final byte[] TOMBSTONE_MARKER = {DELETED};

    @Override
    public byte[] encode(Mutation mutation) {
        if (mutation.isDelete()) {
            return TOMBSTONE_MARKER.clone();
        }
        byte[] value = mutation.value();
        byte[] encoded = new byte[value.length + 1];
        encoded[0] = PRESENT;
        System.arraycopy(value, 0, encoded, 1, value.length);
        return encoded;
    }

    @Override
    public Mutation decode(byte[] value) {
        if (value == null) {
            throw new IllegalStateException(
                    "a Kafka tombstone on the changelog: compaction may drop it before a lagging "
                            + "partition sees it, so an incremental restore cannot be trusted on this "
                            + "topic. Encode deletes as values (MutationCodec).");
        }
        if (value.length == 0) {
            throw new IllegalStateException("an empty changelog value carries no put/delete tag");
        }
        if (value[0] == DELETED) {
            return Mutation.delete();
        }
        byte[] decoded = new byte[value.length - 1];
        System.arraycopy(value, 1, decoded, 0, decoded.length);
        return Mutation.put(decoded);
    }
}
