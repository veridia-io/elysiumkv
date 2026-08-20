#include "elysiumkv/aes256_gcm_encryption_provider.hpp"
#include "elysiumkv/aws_kms_encryption_key_manager.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/kms/KMSClient.h>
#include <aws/kms/model/CreateKeyRequest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using namespace elysiumkv;
static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("  %-56s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++failures;
}

// LocalStack starts with no keys, so the run makes its own. Kept alive by the SDK guard inside the
// managers this creates keys for.
static std::string create_key(const char* endpoint) {
    Aws::SDKOptions sdk;
    Aws::InitAPI(sdk);
    std::string id;
    {
        Aws::Client::ClientConfiguration config;
        config.region = "us-east-1";
        config.endpointOverride = endpoint;
        config.scheme = Aws::Http::Scheme::HTTP;
        Aws::KMS::KMSClient client(Aws::Auth::AWSCredentials("test", "test"), config);
        auto outcome = client.CreateKey(Aws::KMS::Model::CreateKeyRequest());
        if (outcome.IsSuccess()) id = outcome.GetResult().GetKeyMetadata().GetKeyId();
    }
    Aws::ShutdownAPI(sdk);
    return id;
}

int main() {
    const char* endpoint = std::getenv("ELYSIUMKV_KMS_ENDPOINT");
    if (endpoint == nullptr) endpoint = std::getenv("ELYSIUMKV_S3_ENDPOINT");
    if (endpoint == nullptr) { std::printf("skipped\n"); return 77; }

    const std::string key_id = create_key(endpoint);
    if (key_id.empty()) { std::printf("CreateKey failed\n"); return 1; }
    const std::string other_key_id = create_key(endpoint);

    KmsOptions o;
    o.key_id = key_id;
    o.endpoint = endpoint;
    o.access_key = "test";
    o.secret_key = "test";

    check(!AwsKmsEncryptionKeyManager::open(KmsOptions{}).has_value(),
          "an empty key id is refused at open");

    auto opened = AwsKmsEncryptionKeyManager::open(o);
    if (!opened) { std::printf("open failed\n"); return 1; }
    auto& keys = **opened;

    auto first = keys.new_data_key();
    check(first.has_value() && first->key.size() == 32,
          "GenerateDataKey yields the 32 bytes AES-256 needs");
    check(first.has_value() && !first->envelope.empty(), "and a non-empty wrapped form");

    auto second = keys.new_data_key();
    check(second.has_value() && second->envelope != first->envelope,
          "every object gets its own data key");
    check(second.has_value() && first.has_value() &&
              std::memcmp(first->key.data(), second->key.data(), 32) != 0,
          "and the plaintext differs too, not only the wrapping");

    auto reopened = keys.open_data_key(Slice::from(first->envelope));
    check(reopened.has_value() && reopened->size() == 32 &&
              std::memcmp(reopened->data(), first->key.data(), 32) == 0,
          "Decrypt returns exactly the key GenerateDataKey handed out");

    // The envelope names its own key, so a manager configured for a different one still opens
    // it. That is what lets a store keep reading files written before a key rotation.
    {
        KmsOptions rotated = o;
        rotated.key_id = other_key_id;
        auto after = AwsKmsEncryptionKeyManager::open(rotated);
        check(after.has_value(), "a manager on a second key opens");
        auto across = (*after)->open_data_key(Slice::from(first->envelope));
        check(across.has_value() && std::memcmp(across->data(), first->key.data(), 32) == 0,
              "it unwraps the first key's envelope — the id is not sent on Decrypt");
    }

    // Damage is a configuration answer, not a retry: asking KMS again gets the same refusal.
    {
        std::string damaged = first->envelope;
        damaged[damaged.size() / 2] ^= 0x40;
        auto bad = keys.open_data_key(Slice::from(damaged));
        check(!bad.has_value() && bad.error() == Status::Config,
              "a damaged envelope is Config, not a retryable Io");
        auto empty = keys.open_data_key(Slice());
        check(!empty.has_value(), "an empty envelope is refused");
    }

    // End to end: the provider the engine actually uses, over KMS-held keys. Two round trips at a
    // chunk boundary, because that is where the nonce derivation would show up wrong.
    {
        auto provider = Aes256GcmEncryptionProvider::open(*opened, 64);
        check(provider.has_value(), "the AES-256-GCM provider takes a KMS manager");

        auto made = (*provider)->create(7);
        check(made.has_value() && made->cipher != nullptr,
              "create mints an object key through KMS");
        check(made.has_value() && made->cipher->object_id() == 7, "the object id is recorded");

        const std::string plain(100, 'x');
        std::string sealed;
        check(made.has_value() &&
                  made->cipher->seal(0, Slice::from(plain), Slice(), sealed) == Status::Ok,
              "seal");

        auto reopened_cipher = (*provider)->open(7, Slice::from(made->metadata));
        check(reopened_cipher.has_value(), "open reconstructs the cipher from the metadata alone");
        std::string back;
        check(reopened_cipher.has_value() &&
                  (*reopened_cipher)->open(0, Slice::from(sealed), Slice(), back) == Status::Ok &&
                  back == plain,
              "and the round trip is byte-identical after a KMS unwrap");
    }

    std::printf("%s\n", failures ? "FAILURES" : "all probes passed");
    return failures;
}
