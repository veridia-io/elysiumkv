package io.veridia.elysiumkv;

/**
 * The built-in local-directory implementation of the {@link BlobStore} seam
 * (ARCHITECTURE.md "Immutable named objects") — objects written once under a name and never modified.
 *
 * <p><b>The name says which implementation this is, deliberately.</b> The seam is
 * {@code BlobStore}, and the whole point of ARCHITECTURE.md "Immutable named objects" is that other implementations
 * exist: {@link S3BlobStore} today, a disk cache decorator later, a
 * caller-supplied one through the C ABI's vtable. A class called {@code
 * BlobStore} would claim to be the abstraction while being one concrete choice.
 *
 * <p>Owned by the caller and <b>must outlive the database</b> that uses it.
 */
public final class LocalFileBlobStore extends BlobStore {
    public LocalFileBlobStore(String rootDirectory, String storeId) {
        super(create(rootDirectory, storeId));
    }

    private static long create(String rootDirectory, String storeId) {
        Native.ensureLoaded();
        return Native.localBlobStoreCreate(rootDirectory, storeId);
    }
}
