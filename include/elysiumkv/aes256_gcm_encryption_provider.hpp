#ifndef ELYSIUMKV_AES256_GCM_ENCRYPTION_PROVIDER_HPP
#define ELYSIUMKV_AES256_GCM_ENCRYPTION_PROVIDER_HPP

#include "elysiumkv/encryption.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace elysiumkv {

/// The construction persisted per object. **Recorded, never inferred from configuration**: what a
/// file was written with is what reads it, whatever the configuration later says.
enum class CipherSuite : uint16_t {
    /// Reserved and never written. AES-256-CTR was specified and dropped: length-preserving buys
    /// identity offsets and pays for them with a construction whose failure mode is silent. A value
    /// that once meant something must not come to mean something else, so a file recording this is
    /// refused rather than opened as the suite below.
    Aes256CtrReserved = 1,
    /// Chunked AEAD. Confidentiality, and integrity per chunk bound to `(object, chunk index)`.
    Aes256Gcm = 2,
};

/// AES-256-GCM over envelope encryption: a fresh data key per object, wrapped by the embedder's
/// `EncryptionKeyManager`, with the wrapped form recorded in the object's metadata.
///
/// **One key per object is what makes the nonce safe.** Nonce reuse under one key breaks GCM
/// completely. A fresh key per object confines the nonce space to that object, so the nonce can be
/// a deterministic function of the chunk index with nothing to persist and nothing to coordinate —
/// and because objects are write-once and file numbers are never reused, an object is encrypted
/// exactly once and no path could reuse one.
class Aes256GcmEncryptionProvider final : public EncryptionProvider {
public:
    static constexpr size_t kDefaultChunkBytes = 4096;

    /// Fails only on unusable configuration, in the shape `S3BlobStore::open` uses for the same
    /// reason: a component whose construction validates its inputs should say so where it is built,
    /// not at the first write.
    ///
    /// `chunk_bytes` of zero takes the default.
    static Result<std::shared_ptr<Aes256GcmEncryptionProvider>> open(
        std::shared_ptr<EncryptionKeyManager> keys, size_t chunk_bytes = kDefaultChunkBytes);

    Result<NewObject> create(uint64_t object_id) override;
    Result<std::shared_ptr<ObjectCipher>> open(uint64_t object_id, Slice metadata) override;

    ~Aes256GcmEncryptionProvider() override;

    Aes256GcmEncryptionProvider(const Aes256GcmEncryptionProvider&) = delete;
    Aes256GcmEncryptionProvider& operator=(const Aes256GcmEncryptionProvider&) = delete;

    /// Public only as an incomplete type: `open` has to name it. Keeps the OpenSSL headers out of
    /// this one, exactly as `S3BlobStore::Impl` keeps the AWS ones out of its.
    struct Impl;

private:
    explicit Aes256GcmEncryptionProvider(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_AES256_GCM_ENCRYPTION_PROVIDER_HPP
