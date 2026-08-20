package io.veridia.elysiumkv;

/**
 * The DynamoDB implementation of the {@link ManifestCatalog} seam (ARCHITECTURE.md "Ownership is one compare-and-set"), and
 * the better fit for this workload than object storage: small items,
 * conditional writes giving compare-and-swap without an ETag round trip, and
 * single-digit-millisecond commits against roughly 50 ms for S3. Commit latency
 * sits on every flush and every compaction, so that difference is not academic.
 *
 * <p>Optional native component — see {@link ElysiumKV#hasAwsSupport()}.
 *
 * <p>{@code storeId} is the partition key value: one store's manifest state, kept
 * apart from any other sharing the table. One table can serve many databases.
 */
public final class DynamoManifestCatalog extends ManifestCatalog {
    private DynamoManifestCatalog(long handle) {
        super(handle);
    }

    /**
     * @param table   the DynamoDB table, which must have a string partition key
     *                {@code PK} and a string sort key {@code SK}
     * @param storeId this database's partition within it
     */
    public static Builder builder(String table, String storeId) {
        return new Builder(table, storeId);
    }

    public static final class Builder {
        private final String table;
        private final String storeId;
        private String region;
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private long timeoutMs;
        private boolean createTableIfMissing;

        private Builder(String table, String storeId) {
            this.table = table;
            this.storeId = storeId;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /** Points at LocalStack or DynamoDB Local instead of the real service. */
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

        /**
         * One timeout, not two: every call here is a small item read or write, so
         * this is a commit-latency budget rather than a bulk one. Zero keeps the
         * native default.
         */
        public Builder timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Off by default, and should stay off outside tests: a production
         * table belongs to whatever provisions infrastructure, and creating one
         * silently would hide a misconfigured table name behind a working store.
         *
         * <p>This is also the only argument here that performs I/O, so turning it
         * on means {@link #open} can fail with a {@link RetryableException} rather
         * than only a {@link ConfigException}.
         */
        public Builder createTableIfMissing(boolean createTableIfMissing) {
            this.createTableIfMissing = createTableIfMissing;
            return this;
        }

        public DynamoManifestCatalog open() {
            Native.ensureLoaded();
            return new DynamoManifestCatalog(
                Native.dynamoManifestCatalogCreate(table, storeId, region, endpoint, accessKey,
                                                   secretKey, timeoutMs, createTableIfMissing));
        }
    }
}
