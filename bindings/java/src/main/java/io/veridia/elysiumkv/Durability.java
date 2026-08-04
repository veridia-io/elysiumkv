package io.veridia.elysiumkv;

/**
 * Mirrors {@code elysiumkv_durability}. A {@code TRANSIENT} tier may lose its
 * store; ARCHITECTURE.md "A tier is not a level" requires the last tier to be {@code DURABLE}, and
 * {@link ElysiumKV#open} refuses a configuration containing any transient tier at
 * all — use {@link ElysiumKV#openWithResult} when that is intended.
 */
public enum Durability {
    DURABLE,
    TRANSIENT;

    int code() {
        return ordinal();
    }
}
