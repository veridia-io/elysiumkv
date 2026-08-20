package io.veridia.elysiumkv;

/**
 * A transient store lost its contents, so what survived is wrong rather than merely
 * incomplete: a key whose newer value lived on that store now reads as its older one. Reads are
 * refused until the gap is replayed and {@link ElysiumKV#markRecoveryComplete()} is called.
 *
 * <p>Writes are not refused, because the replay that clears this is made of them. An
 * embedder whose replay also reads — the read-modify-write shape a changelog consumer usually has —
 * sets {@link ElysiumKVOptions#allowReadsBeforeRecovery(boolean)} and takes responsibility for
 * reading values that may be behind.
 *
 * <p>Not a {@link RetryableException}: repeating the same call produces the same answer, and not a
 * {@link CorruptException} either — nothing is damaged, and the remedy is a replay rather than a
 * restore.
 */
public class RecoveryRequiredException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    RecoveryRequiredException(Status status, String message) {
        super(status, message);
    }
}
