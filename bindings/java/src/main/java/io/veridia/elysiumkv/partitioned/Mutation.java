package io.veridia.elysiumkv.partitioned;

import java.util.Objects;

/**
 * A put or a delete. Deletion is a case of this type rather than a {@code null} value, which is the
 * whole of the reason it exists.
 *
 * <p>A changelog on a compacted topic retains delete markers only for {@code delete.retention.ms}, so
 * a partition that is away for longer than that can resume from its watermark and never learn that a
 * key was deleted — the local store keeps the old value and no later record contradicts it. Full
 * replay does not have the problem, so it appears only on the incremental path this component exists
 * for. The fix is that a delete travels the log as an ordinary <em>value</em>, which compaction keeps
 * as the latest value for that key.
 *
 * <p>That splits along the serialisation boundary. This component speaks {@code Mutation} and applies
 * a delete as a store delete; the caller encodes {@link #delete()} into whatever non-null marker its
 * topic uses and decodes it back on restore. Nothing here ever hands {@code null} to a changelog
 * callback, and {@link PartitionedStore#stage} rejects a {@code null} value rather than guessing
 * which of the two was meant.
 */
public final class Mutation {
    private static final Mutation DELETE = new Mutation(null);

    private final byte[] value;

    private Mutation(byte[] value) {
        this.value = value;
    }

    /** A put. The value may be empty but not {@code null} — use {@link #delete()} for absence. */
    public static Mutation put(byte[] value) {
        return new Mutation(Objects.requireNonNull(value, "value; use Mutation.delete() to delete"));
    }

    /** A delete, which reaches the log as a value the caller encodes. */
    public static Mutation delete() {
        return DELETE;
    }

    public boolean isDelete() {
        return value == null;
    }

    /** The value of a put. Throws on a delete, which carries none. */
    public byte[] value() {
        if (value == null) {
            throw new IllegalStateException("a delete carries no value; check isDelete() first");
        }
        return value;
    }

    @Override
    public String toString() {
        return value == null ? "Mutation.delete()" : "Mutation.put(" + value.length + " bytes)";
    }
}
