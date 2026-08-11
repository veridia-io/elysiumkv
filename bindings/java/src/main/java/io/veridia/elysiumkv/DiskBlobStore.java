package io.veridia.elysiumkv;

/**
 * The built-in on-disk implementation of the {@link BlobStore} seam
 * (ARCHITECTURE.md "Immutable named objects") — objects written once under a name and never modified.
 *
 * <p><b>The name says which implementation this is, deliberately.</b> The seam is
 * {@code BlobStore}, and the whole point of ARCHITECTURE.md "Immutable named objects" is that other implementations
 * exist: {@link S3BlobStore} today, a disk cache decorator later, a
 * caller-supplied one through the C ABI's vtable. A class called {@code
 * BlobStore} would claim to be the abstraction while being one concrete choice.
 * {@code Disk} is the medium, as in {@link S3BlobStore} and {@code DiskCacheBlobStore}.
 *
 * <p>Owned by the caller and <b>must outlive the database</b> that uses it.
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
