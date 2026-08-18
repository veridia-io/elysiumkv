package io.veridia.elysiumkv;

/** Mirrors {@code elysiumkv_status}. Order is the ABI's; do not reorder. */
public enum Status {
    OK,
    NOT_FOUND,
    CORRUPT,
    UNUSABLE,
    FENCED,
    CONFIG,
    IO,
    STALLED,
    /**
     * The data is intact but this build cannot read it — a manifest written by a newer format
     * version. Distinct from {@link #CORRUPT}: the remedy is a different binary, not a restore.
     */
    UNSUPPORTED,
    /**
     * A read-only handle is behind the writer's retention window. Recoverable with
     * {@link ReadOnlyStore#refresh()}; never a sign of damaged data.
     */
    STALE,
    RECOVERY_REQUIRED;

    private static final Status[] VALUES = values();

    static Status fromCode(int code) {
        // An unknown code means the loaded library is newer than this binding.
        // Reporting it as UNUSABLE is the safe direction: it is the status a
        // caller must not retry and must not read as absence.
        return code >= 0 && code < VALUES.length ? VALUES[code] : UNUSABLE;
    }
}
