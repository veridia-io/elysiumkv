#ifndef ELYSIUMKV_TESTS_SUPPORT_TEST_ENCRYPTION_HPP
#define ELYSIUMKV_TESTS_SUPPORT_TEST_ENCRYPTION_HPP

#include "elysiumkv/encryption.hpp"
#include "elysiumkv/aes256_gcm_encryption_provider.hpp"
#include "elysiumkv/no_encryption_provider.hpp"

#include <atomic>
#include <memory>
#include <string>

namespace elysiumkv::test {

/// A key manager with no custody: the envelope *is* the key.
///
/// **Adequate because what these suites test is the engine, not a KMS.** A real one would add a
/// network and a mock of it, and would tell us nothing more about whether the boundary holds.
/// `seed` distinguishes two managers, so a test can register providers that genuinely cannot read
/// each other's files.
class TestKeyManager final : public EncryptionKeyManager {
public:
    explicit TestKeyManager(int seed = 1) : seed_(seed) {}

    Result<DataKey> new_data_key() override {
        const int n = next_.fetch_add(1);
        std::string material(32, '\0');
        for (size_t i = 0; i < material.size(); ++i) {
            material[i] = static_cast<char>(seed_ * 1009 + n * 31 + static_cast<int>(i));
        }
        DataKey key;
        key.key = SecretKey(reinterpret_cast<const uint8_t*>(material.data()), material.size());
        key.envelope = material;
        return key;
    }

    Result<SecretKey> open_data_key(Slice envelope) override {
        if (envelope.size() != 32) return std::unexpected(Status::Corrupt);
        return SecretKey(envelope.data(), envelope.size());
    }

private:
    int seed_;
    std::atomic<int> next_{1};
};

inline std::shared_ptr<EncryptionProvider> make_test_provider(int seed = 1,
                                                              size_t chunk_bytes = 4096) {
    auto made = Aes256GcmEncryptionProvider::open(std::make_shared<TestKeyManager>(seed),
                                                  chunk_bytes);
    return made.value_or(nullptr);
}

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_TEST_ENCRYPTION_HPP
