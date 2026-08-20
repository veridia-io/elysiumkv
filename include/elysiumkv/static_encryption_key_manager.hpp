#ifndef ELYSIUMKV_STATIC_ENCRYPTION_KEY_MANAGER_HPP
#define ELYSIUMKV_STATIC_ENCRYPTION_KEY_MANAGER_HPP

#include "elysiumkv/encryption.hpp"

#include <cstddef>
#include <memory>

namespace elysiumkv {

/// A key-encryption key held in this process, wrapping a fresh data key per object.
///
/// Not "one key for everything", which is what the name might suggest and would be unsafe. The
/// provider derives its nonce from a per-object salt and the chunk index; with one key across every
/// object, two objects drawing the same salt would reuse a nonce under that key, and a repeated
/// nonce breaks GCM completely rather than gradually. So this generates a random data key per
/// object and wraps it under the master key, exactly as a KMS would — the difference is only where
/// the master key lives.
///
/// For tests, single-tenant deployments, and anywhere a KMS is not warranted. The master key is
/// in this process's memory, so it protects storage and nothing else: an attacker who reads the
/// process reads the key. That is the trade, and it is the whole trade — see `AwsKmsEncryptionKeyManager`
/// for the arrangement where the master key never leaves a boundary this process cannot cross.
class StaticEncryptionKeyManager final : public EncryptionKeyManager {
public:
    /// The master key must be exactly 32 bytes: this wraps with AES-256, and a short key silently
    /// weakened is worse than a refused configuration.
    static Result<std::shared_ptr<StaticEncryptionKeyManager>> open(Slice master_key);

    /// A master key from the process environment, hex-encoded. Convenience for a deployment that
    /// injects secrets that way; the same 32-byte rule applies after decoding.
    static Result<std::shared_ptr<StaticEncryptionKeyManager>> from_hex(std::string_view hex);

    Result<DataKey> new_data_key() override;
    Result<SecretKey> open_data_key(Slice envelope) override;

    ~StaticEncryptionKeyManager() override;

    StaticEncryptionKeyManager(const StaticEncryptionKeyManager&) = delete;
    StaticEncryptionKeyManager& operator=(const StaticEncryptionKeyManager&) = delete;

    /// Public only as an incomplete type, so the OpenSSL headers stay in one translation unit.
    struct Impl;

private:
    explicit StaticEncryptionKeyManager(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_STATIC_ENCRYPTION_KEY_MANAGER_HPP
