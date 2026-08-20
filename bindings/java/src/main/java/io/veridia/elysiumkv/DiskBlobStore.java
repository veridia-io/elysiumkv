package io.veridia.elysiumkv;

/**
 * The built-in on-disk implementation of the {@link BlobStore} seam
 * (ARCHITECTURE.md "Immutable named objects") — objects written once under a name and never modified.
 *
 * <p>The name carries the medium, as in {@link S3BlobStore} and {@code DiskCacheBlobStore}; the
 * seam itself is {@link BlobStore}.
 *
 * <p>Owned by the caller and must outlive the database that uses it.
 */
public final class DiskBlobStore extends BlobStore {
    public DiskBlobStore(String rootDirectory, String storeId) {
        super(create(rootDirectory, storeId));
    }

    private static long create(String rootDirectory, String storeId) {
        Native.ensureLoaded();
        return Native.diskBlobStoreCreate(rootDirectory, storeId);
    }
}
