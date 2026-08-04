package io.veridia.elysiumkv;

/**
 * The engine could not determine an answer — an unreachable store, or a write
 * held back by the stall valve. The call may be worth repeating. This is the one
 * class of failure that must never be read as absence (ARCHITECTURE.md "Immutable named objects", ARCHITECTURE.md "The ABI boundary").
 */
public final class RetryableException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    RetryableException(Status status, String message) {
        super(status, message);
    }
}
