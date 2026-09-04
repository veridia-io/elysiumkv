package io.veridia.elysiumkv;

/**
 * The S3 implementation of the {@link BlobStore} seam (ARCHITECTURE.md "The ABI boundary") — what makes a cold
 * tier actually cold rather than another directory on the same machine.
 *
 * <p>Optional native component: the library is built without the AWS SDK by default, and
 * construction then fails with a {@link ConfigException} naming the missing build option.
 * {@link ElysiumKV#hasAwsSupport()} answers the question in advance.
 *
 * <pre>{@code
 * try (S3BlobStore cold = S3BlobStore.builder("my-bucket").prefix("cold").open()) {
 *     options.addTier(cold, Durability.DURABLE, 0, 0, 0);
 * }
 * }</pre>
 *
 * <p>Owned by the caller and must outlive the database.
 */
public final class S3BlobStore extends BlobStore {
    private S3BlobStore(long handle) {
        super(handle);
    }

    /** @param bucket required; everything else has a working default. */
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
        private String storeId;
        private long pointTimeoutMs;
        private long bulkTimeoutMs;

        private Builder(String bucket) {
            this.bucket = bucket;
        }

        /** Separates stores sharing a bucket. Absent means the bucket root. */
        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * Points at LocalStack or another S3-compatible endpoint instead of the
         * real service. Absent means real S3.
         */
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Explicit credentials. Leave unset in a deployed process: the SDK's own
         * chain — environment, profile, instance role — is what should supply
         * them, and hard-coded keys in a config object outlive the config.
         */
        public Builder credentials(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            return this;
        }

        /**
         * Two timeouts, because compaction reads whole files while a point lookup
         * reads a footer and one budget cannot serve both. Zero keeps the native
         * default.
         */
        public Builder timeouts(long pointTimeoutMs, long bulkTimeoutMs) {
            this.pointTimeoutMs = pointTimeoutMs;
            this.bulkTimeoutMs = bulkTimeoutMs;
            return this;
        }

        /**
         * Overrides the derived {@code s3://bucket/prefix} identity, which the
         * manifest records. Leave unset unless a store is being renamed — the
         * recorded id is how the engine knows which store a file belongs to.
         */
        public Builder storeId(String storeId) {
            this.storeId = storeId;
            return this;
        }

        public S3BlobStore open() {
            Native.ensureLoaded();
            return new S3BlobStore(Native.s3BlobStoreCreate(bucket, prefix, region, endpoint,
                                                            accessKey, secretKey, pointTimeoutMs,
                                                            bulkTimeoutMs, storeId));
        }
    }
}
