package io.veridia.elysiumkv;

/** Bytes that failed verification. Retrying reads the same bad bytes. */
public final class CorruptException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    CorruptException(Status status, String message) {
        super(status, message);
    }
}
