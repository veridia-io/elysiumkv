package io.veridia.elysiumkv;

/**
 * Every failure the engine reports, carrying the {@link Status} and the
 * thread-local detail from {@code elysiumkv_last_error()}.
 *
 * <p>The subclasses exist for one reason, and it is the rule ARCHITECTURE.md "The ABI boundary" puts at the top
 * of the ABI: <b>a retryable failure must never be confused with a definite
 * answer.</b> {@link RetryableException} means the engine could not determine
 * something and the call may be worth repeating; {@link CorruptException} and
 * {@link UnusableException} mean stop. Absence is not in this hierarchy at all —
 * a missing key returns {@code null}, and only a missing key does. A binding
 * that mapped an IO failure to "not found" would silently turn an unreachable
 * store into deleted data.
 *
 * <p>Constructed by {@link #of}, which the glue calls; keeping the mapping here
 * rather than in C++ means one cached method id instead of six cached classes.
 */
public class ElysiumKVException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final Status status;

    ElysiumKVException(Status status, String message) {
        super(message == null || message.isEmpty() ? status.toString() : message);
        this.status = status;
    }

    public Status status() {
        return status;
    }

    /** Called from {@code elysiumkv_jni.cpp}; the signature is registered there. */
    static ElysiumKVException of(int statusCode, String message) {
        Status status = Status.fromCode(statusCode);
        switch (status) {
            case IO:
            case STALLED:
                return new RetryableException(status, message);
            case CORRUPT:
                return new CorruptException(status, message);
            case UNUSABLE:
                return new UnusableException(status, message);
            case FENCED:
                return new FencedException(status, message);
            case CONFIG:
                return new ConfigException(status, message);
            default:
                return new ElysiumKVException(status, message);
        }
    }
}
