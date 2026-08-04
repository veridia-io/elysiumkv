package io.veridia.elysiumkv;

/** Mirrors {@code elysiumkv_compression}. A level property, never a tier one (ARCHITECTURE.md "A tier is not a level"). */
public enum Compression {
    NONE,
    LZ4,
    ZSTD;

    int code() {
        return ordinal();
    }
}
