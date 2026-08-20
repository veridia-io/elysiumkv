package io.veridia.elysiumkv;

/**
 * The S3 implementation of the {@link ManifestCatalog} seam (ARCHITECTURE.md "Ownership is one compare-and-set"). Generation
 * objects live under a prefix; the pointer is a small object whose ETag is the
 * fencing token, which is what makes the compare-and-swap a single
 * conditional PUT with no read-modify-write.
 *
 * <p>Commit latency lands on every flush and every compaction: roughly 50 ms here against
 * single-digit milliseconds for {@link DynamoManifestCatalog}.
 *
 * <p>Optional native component — see {@link ElysiumKV#hasAwsSupport()}.
 *
 * <p>The prefix should differ from any blob store's on the same bucket, or
 * manifest objects and SSTs share a namespace.
 */
public final class S3ManifestCatalog extends ManifestCatalog {
    private S3ManifestCatalog(long handle) {
        super(handle);
    }

    public static Builder builder(String bucket) {
        return new Builder(bucket);
    }

    public static final class Builder {
        private final String bucket;
        private String prefix;
        private String region;
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private long pointTimeoutMs;
        private long bulkTimeoutMs;

        private Builder(String bucket) {
            this.bucket = bucket;
        }

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /** See {@link S3BlobStore.Builder#credentials}. */
        public Builder credentials(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            return this;
        }

        public Builder timeouts(long pointTimeoutMs, long bulkTimeoutMs) {
            this.pointTimeoutMs = pointTimeoutMs;
            this.bulkTimeoutMs = bulkTimeoutMs;
            return this;
        }

        public S3ManifestCatalog open() {
            Native.ensureLoaded();
            return new S3ManifestCatalog(
                Native.s3ManifestCatalogCreate(bucket, prefix, region, endpoint, accessKey,
                                               secretKey, pointTimeoutMs, bulkTimeoutMs));
        }
    }
}
