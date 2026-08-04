package io.veridia.elysiumkv;

/** See {@link ElysiumKVException} for why this distinction is load-bearing. */
public final class ConfigException extends ElysiumKVException {
    private static final long serialVersionUID = 1L;

    ConfigException(Status status, String message) {
        super(status, message);
    }
}
