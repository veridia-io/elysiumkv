package io.veridia.elysiumkv;

/**
 * The instance cannot continue — including a C++ exception that escaped the ABI,
 * which the glue catches rather than letting it unwind through JNI.
 */
public final class UnusableException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    UnusableException(Status status, String message) {
        super(status, message);
    }
}
