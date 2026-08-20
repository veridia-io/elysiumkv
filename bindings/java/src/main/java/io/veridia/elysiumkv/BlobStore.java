package io.veridia.elysiumkv;

/**
 * The object-storage seam (ARCHITECTURE.md "Immutable named objects"): objects written once under a
 * name and never modified. Implementations are named for their medium — {@link DiskBlobStore},
 * {@link S3BlobStore} — so the seam's own name stays free.
 *
 * <p>Not extensible from Java: the constructor is package-private because a store implemented in
 * Java would attach native threads to the JVM and translate exceptions back across the boundary,
 * re-entering a single-writer engine. The C ABI's vtable seam is there for a language that needs
 * it.
 *
 * <p>Owned by the caller and must outlive the database that uses it.
 */
public abstract class BlobStore implements AutoCloseable {
    private long handle;

    BlobStore(long handle) {
        this.handle = handle;
    }

    /**
     * Releases the native store. Idempotent, and final: every implementation is
     * destroyed the same way, because the handle is a {@code BlobStore} on the
     * other side of the ABI whichever constructor produced it.
     */
    @Override
    public final void close() {
        if (handle == 0) return;
        long h = handle;
        handle = 0;
        Native.blobStoreDestroy(h);
    }

    final long handle() {
        if (handle == 0) throw new IllegalStateException("blob store is closed");
        return handle;
    }
}
