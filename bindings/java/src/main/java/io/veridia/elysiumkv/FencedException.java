package io.veridia.elysiumkv;

/** See {@link ElysiumKVException} for why this distinction is load-bearing. */
public final class FencedException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    FencedException(Status status, String message) {
        super(status, message);
    }
}
