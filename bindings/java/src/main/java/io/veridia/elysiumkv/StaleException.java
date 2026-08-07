package io.veridia.elysiumkv;

/**
 * A read-only handle is older than the writer's retention window, so an object its version
 * references has been legitimately collected.
 *
 * <p><b>Not a corruption error, and the distinction matters operationally:</b> the data is not
 * damaged and there is nothing to restore. The remedy is {@link ReadOnlyStore#refresh()} or a
 * reopen. The engine tells the two apart by re-reading the manifest — if the writer has moved past
 * the version holding the missing file, it collected it legitimately.
 *
 * <p>Not a {@link RetryableException}: repeating the same call produces the same answer. It is a
 * definite result with a specific remedy.
 */
public class StaleException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    StaleException(Status status, String message) {
        super(status, message);
    }
}
