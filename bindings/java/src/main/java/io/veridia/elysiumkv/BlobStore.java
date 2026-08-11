package io.veridia.elysiumkv;

/**
 * The object-storage seam (ARCHITECTURE.md "Immutable named objects"): objects written once under a name and never
 * modified. This is the abstraction, so it carries the abstraction's name;
 * {@link DiskBlobStore} and {@link S3BlobStore} carry theirs.
 *
 * <p>The distinction matters because ARCHITECTURE.md "Immutable named objects" exists so that other implementations
 * can be substituted, and a concrete class holding the seam's name leaves the
 * next one nowhere to stand — which is exactly what happened here once already.
 *
 * <p><b>Not extensible from Java.</b> The constructor is package-private, and
 * deliberately: ARCHITECTURE.md "The ABI boundary" keeps upcalls out of this binding, because a store
 * implemented in Java would mean attaching native threads to the JVM and
 * translating exceptions back across the boundary, re-entering a single-writer
 * engine. The C ABI's vtable seam is still there for a language that needs it.
 *
 * <p>Owned by the caller and <b>must outlive the database</b> that uses it.
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
