#ifndef ELYSIUMKV_AWS_KMS_ENCRYPTION_KEY_MANAGER_HPP
#define ELYSIUMKV_AWS_KMS_ENCRYPTION_KEY_MANAGER_HPP

#include "elysiumkv/encryption.hpp"

#include <chrono>
#include <memory>
#include <string>

namespace elysiumkv {

struct KmsOptions {
    /// The KMS key that wraps every data key: an id, an alias or an ARN — whatever
    /// `GenerateDataKey` accepts. This is the thing that is rotated, and rotating it means
    /// naming a new one and letting compaction rewrite files under it; the old one stays
    /// reachable for as long as any file's envelope was wrapped by it.
    std::string key_id;
    std::string region = "us-east-1";

    /// Non-empty points at LocalStack rather than real KMS, as the S3 and DynamoDB options do.
    std::string endpoint;
    std::string access_key;
    std::string secret_key;

    /// A data key is minted once per SST and unwrapped once per reader, both off the hot path but
    /// both on the path a flush or a cold read waits for.
    std::chrono::milliseconds timeout{3'000};
};

/// Data keys minted and unwrapped by AWS KMS, so the key that protects them never enters this
/// process.
///
/// `GenerateDataKey` returns the plaintext key and its wrapped form in one call, which is exactly
/// the shape `new_data_key` needs — the wrapped form is what lands in the manifest beside the file.
///
/// Every call is a network round trip. One per object written, and one per object whenever its
/// reader is not resident — the reader holds the unwrapped key for as long as the cache keeps it,
/// which is what makes the per-block case impossible. A reader cache sized well below the working
/// set turns evictions into KMS traffic.
class AwsKmsEncryptionKeyManager final : public EncryptionKeyManager {
public:
    /// Fails only on unusable configuration; it does not talk to KMS. A key id that does not exist
    /// surfaces on first use, for the same reason `S3BlobStore::open` does not probe its bucket.
    static Result<std::shared_ptr<AwsKmsEncryptionKeyManager>> open(KmsOptions options);

    Result<DataKey> new_data_key() override;
    Result<SecretKey> open_data_key(Slice envelope) override;

    ~AwsKmsEncryptionKeyManager() override;

    AwsKmsEncryptionKeyManager(const AwsKmsEncryptionKeyManager&) = delete;
    AwsKmsEncryptionKeyManager& operator=(const AwsKmsEncryptionKeyManager&) = delete;

    /// Public only as an incomplete type, so the AWS headers stay out of this one.
    struct Impl;

private:
    explicit AwsKmsEncryptionKeyManager(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_AWS_KMS_ENCRYPTION_KEY_MANAGER_HPP
