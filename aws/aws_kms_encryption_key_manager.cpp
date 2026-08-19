#include "elysiumkv/aws_kms_encryption_key_manager.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/kms/KMSClient.h>
#include <aws/kms/model/DecryptRequest.h>
#include <aws/kms/model/GenerateDataKeyRequest.h>

#include <mutex>
#include <utility>

namespace elysiumkv {
namespace {

/// One SDK lifetime for the process however many clients exist, refcounted here rather than asked
/// of the embedder: getting it wrong surfaces as a crash during exit, long after the mistake.
class SdkGuard {
public:
    SdkGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (refs_++ == 0) {
            options_ = new Aws::SDKOptions();
            Aws::InitAPI(*options_);
        }
    }
    ~SdkGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (--refs_ == 0) {
            Aws::ShutdownAPI(*options_);
            delete options_;
            options_ = nullptr;
        }
    }
    SdkGuard(const SdkGuard&) = delete;
    SdkGuard& operator=(const SdkGuard&) = delete;

private:
    static std::mutex mutex_;
    static int refs_;
    static Aws::SDKOptions* options_;
};

std::mutex SdkGuard::mutex_;
int SdkGuard::refs_ = 0;
Aws::SDKOptions* SdkGuard::options_ = nullptr;

constexpr size_t kKeyBytes = 32;   // AES_256, which is what the provider requires

}  // namespace

struct AwsKmsEncryptionKeyManager::Impl {
    SdkGuard sdk;
    KmsOptions options;
    std::shared_ptr<Aws::KMS::KMSClient> client;
};

Result<std::shared_ptr<AwsKmsEncryptionKeyManager>> AwsKmsEncryptionKeyManager::open(
    KmsOptions options) {
    if (options.key_id.empty()) return std::unexpected(Status::Config);

    auto impl = std::make_unique<Impl>();
    impl->options = std::move(options);

    Aws::Client::ClientConfiguration config;
    config.region = impl->options.region;
    config.requestTimeoutMs = static_cast<long>(impl->options.timeout.count());
    config.connectTimeoutMs = static_cast<long>(impl->options.timeout.count());
    if (!impl->options.endpoint.empty()) {
        config.endpointOverride = impl->options.endpoint;
        config.scheme = Aws::Http::Scheme::HTTP;
    }

    if (!impl->options.access_key.empty()) {
        impl->client = std::make_shared<Aws::KMS::KMSClient>(
            Aws::Auth::AWSCredentials(impl->options.access_key, impl->options.secret_key), config);
    } else {
        // The SDK's default chain: environment, profile, instance metadata. The same arrangement
        // the S3 and DynamoDB clients use, so one set of credentials serves all three.
        impl->client = std::make_shared<Aws::KMS::KMSClient>(config);
    }

    return std::shared_ptr<AwsKmsEncryptionKeyManager>(
        new AwsKmsEncryptionKeyManager(std::move(impl)));
}

AwsKmsEncryptionKeyManager::AwsKmsEncryptionKeyManager(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

AwsKmsEncryptionKeyManager::~AwsKmsEncryptionKeyManager() = default;

Result<DataKey> AwsKmsEncryptionKeyManager::new_data_key() {
    Aws::KMS::Model::GenerateDataKeyRequest request;
    request.SetKeyId(impl_->options.key_id);
    // **`AES_256`, matching what the provider requires**, rather than a byte count: asking KMS for
    // the named spec means a mismatch is a rejected request rather than a key the cipher refuses
    // later for a reason that reads like corruption.
    request.SetKeySpec(Aws::KMS::Model::DataKeySpec::AES_256);

    auto outcome = impl_->client->GenerateDataKey(request);
    if (!outcome.IsSuccess()) {
        // Could not determine: a throttle, an expired credential, a network fault. Retryable, and
        // the write that needed the key will ask again.
        return std::unexpected(Status::Io);
    }

    const auto& plaintext = outcome.GetResult().GetPlaintext();
    const auto& wrapped = outcome.GetResult().GetCiphertextBlob();
    if (plaintext.GetLength() != kKeyBytes) return std::unexpected(Status::Config);

    DataKey made;
    made.key = SecretKey(plaintext.GetUnderlyingData(), plaintext.GetLength());
    made.envelope.assign(reinterpret_cast<const char*>(wrapped.GetUnderlyingData()),
                         wrapped.GetLength());
    // The SDK owns the plaintext buffer and will free it; nothing here can zero it, which is the
    // reason a KMS integration is a boundary rather than a guarantee about this process's memory.
    return made;
}

Result<SecretKey> AwsKmsEncryptionKeyManager::open_data_key(Slice envelope) {
    Aws::KMS::Model::DecryptRequest request;
    // **The key id is not sent.** A ciphertext blob from KMS names the key that produced it, so
    // Decrypt resolves it on its own — which is what lets a store keep reading files wrapped by a
    // key that is no longer the one being written under.
    request.SetCiphertextBlob(Aws::Utils::ByteBuffer(envelope.data(), envelope.size()));

    auto outcome = impl_->client->Decrypt(request);
    if (!outcome.IsSuccess()) {
        // Distinguishing "this key is gone or forbidden" from "KMS is briefly unreachable" needs
        // the error type, and the two want different remedies: one is a configuration to fix, the
        // other is a retry.
        const auto& error = outcome.GetError();
        const auto type = error.GetErrorType();
        const bool retryable = error.ShouldRetry() || type == Aws::KMS::KMSErrors::THROTTLING ||
                               type == Aws::KMS::KMSErrors::NETWORK_CONNECTION ||
                               type == Aws::KMS::KMSErrors::DEPENDENCY_TIMEOUT ||
                               type == Aws::KMS::KMSErrors::K_M_S_INTERNAL;
        return std::unexpected(retryable ? Status::Io : Status::Config);
    }

    const auto& plaintext = outcome.GetResult().GetPlaintext();
    if (plaintext.GetLength() != kKeyBytes) return std::unexpected(Status::Corrupt);
    return SecretKey(plaintext.GetUnderlyingData(), plaintext.GetLength());
}

}  // namespace elysiumkv
