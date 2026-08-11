package io.veridia.elysiumkv;

/**
 * The one pluggable metadata seam (ARCHITECTURE.md "Ownership is one compare-and-set"): the manifest generations plus the
 * pointer naming which one is live, and the compare-and-swap that moves it. The
 * seam's name belongs to the seam; {@link DiskManifestCatalog}, {@link
 * S3ManifestCatalog} and {@link DynamoManifestCatalog} are named for what they
 * are.
 *
 * <p>Bytes are opaque to an implementation: record encoding, replay, gap
 * detection and the GC ordering rule all stay in the engine. An implementation
 * stores bytes at an address and swaps a pointer — it can get storage and CAS
 * wrong, and nothing else.
 *
 * <p>Not extensible from Java, for the reason given on {@link BlobStore}. Owned
 * by the caller and must outlive the database.
 */
public abstract class ManifestCatalog implements AutoCloseable {
    private long handle;

    ManifestCatalog(long handle) {
        this.handle = handle;
    }

    @Override
    public final void close() {
        if (handle == 0) return;
        long h = handle;
        handle = 0;
        Native.manifestCatalogDestroy(h);
    }

    final long handle() {
        if (handle == 0) throw new IllegalStateException("manifest catalog is closed");
        return handle;
    }
}
