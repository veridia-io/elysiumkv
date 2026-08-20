package io.veridia.elysiumkv;

/**
 * Data keys minted and unwrapped by AWS KMS, so the key that wraps them never enters this process.
 *
 * <p>Optional native component. Like {@link S3BlobStore}, this needs a library built with the
 * AWS SDK; without one, registering it fails with a {@link ConfigException} naming the build option.
 * {@link ElysiumKV#hasAwsSupport()} answers in advance.
 *
 * <p>Every use is a network round trip — one per object written, and one per object whenever
 * its reader is not resident, since the reader holds the unwrapped key for as long as the cache
 * keeps it. A reader cache sized well below the working set turns evictions into KMS traffic.
 *
 * <p>{@code keyId} is what a rotation changes. The wrapped form records which key produced
 * it, so files written under an earlier key keep opening without it being named here.
 *
 * <pre>{@code
 * options.encryptWith("kms-v1", AwsKmsEncryptionKeyManager.builder(keyArn).build(), 0);
 * }</pre>
 */
public final class AwsKmsEncryptionKeyManager extends BuiltinEncryptionKeyManager {
    private final String keyId;
    private final String region;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final long timeoutMs;

    private AwsKmsEncryptionKeyManager(Builder builder) {
        this.keyId = builder.keyId;
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.timeoutMs = builder.timeoutMs;
    }

    /** @param keyId a key id, alias or ARN — whatever {@code GenerateDataKey} accepts. */
    public static Builder builder(String keyId) {
        return new Builder(keyId);
    }

    public static final class Builder {
        private final String keyId;
        private String region;
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private long timeoutMs;

        private Builder(String keyId) {
            if (keyId == null || keyId.isEmpty()) {
                throw new IllegalArgumentException("a KMS key id is required");
            }
            this.keyId = keyId;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * Points at LocalStack or another KMS-compatible endpoint. Absent means the real service.
         */
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Explicit credentials. Leave unset in a deployed process: the SDK's own chain —
         * environment, profile, instance role — is what should supply them.
         */
        public Builder credentials(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            return this;
        }

        /** Zero keeps the native default. Applies to both the mint and the unwrap. */
        public Builder timeoutMs(long timeoutMs) {
            if (timeoutMs < 0) {
                throw new IllegalArgumentException("timeoutMs must not be negative: " + timeoutMs);
            }
            this.timeoutMs = timeoutMs;
            return this;
        }

        public AwsKmsEncryptionKeyManager build() {
            return new AwsKmsEncryptionKeyManager(this);
        }
    }

    @Override
    void register(long optionsHandle, String id, long chunkBytes) {
        // No ensureLoaded(): the options handle this registers into could not exist otherwise.
        Native.optionsAddAes256GcmEncryptionWithKms(optionsHandle, id, keyId, region, endpoint,
                                                    accessKey, secretKey, timeoutMs, chunkBytes);
    }
}
