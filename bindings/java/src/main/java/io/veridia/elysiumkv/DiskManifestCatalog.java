package io.veridia.elysiumkv;

/**
 * The on-disk implementation of the {@link ManifestCatalog} seam (ARCHITECTURE.md "Ownership is one compare-and-set").
 *
 * <p>Named for the implementation rather than the seam: {@link S3ManifestCatalog}
 * and {@link DynamoManifestCatalog} exist too, and calling this one {@code
 * ManifestCatalog} would take the abstraction's name for a single concrete
 * choice.
 *
 * <p>Owned by the caller and must outlive the database.
 */
public final class DiskManifestCatalog extends ManifestCatalog {
    public DiskManifestCatalog(String directory) {
        super(create(directory));
    }

    private static long create(String directory) {
        Native.ensureLoaded();
        return Native.diskManifestCatalogCreate(directory);
    }
}
