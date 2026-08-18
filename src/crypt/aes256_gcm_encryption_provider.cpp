#include "elysiumkv/aes256_gcm_encryption_provider.hpp"

#include "sst/format.hpp"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cstring>
#include <utility>

namespace elysiumkv {
namespace {

constexpr size_t kKeyBytes = 32;    // AES-256
constexpr size_t kNonceBytes = 12;  // GCM's standard nonce, and the only length worth using
constexpr size_t kTagBytes = 16;

/// The per-object metadata this provider records. Versioned separately from the suite, so the
/// layout and the construction can move independently.
constexpr uint32_t kMetadataVersion = 1;

/// RAII for the OpenSSL context, which is a heap allocation with a free function.
class CipherContext {
public:
    CipherContext() : ctx_(EVP_CIPHER_CTX_new()) {}
    ~CipherContext() {
        if (ctx_ != nullptr) EVP_CIPHER_CTX_free(ctx_);
    }
    CipherContext(const CipherContext&) = delete;
    CipherContext& operator=(const CipherContext&) = delete;

    EVP_CIPHER_CTX* get() const { return ctx_; }
    explicit operator bool() const { return ctx_ != nullptr; }

private:
    EVP_CIPHER_CTX* ctx_;
};

/// **The nonce is the chunk index, and that is safe only because the key is per object.** With a
/// key used for one object, every chunk index appears at most once under it — objects are
/// write-once and file numbers are never reused, so there is no second encryption to collide with.
/// The high four bytes carry a per-object random salt so that two objects never share a nonce
/// *sequence* even if a key manager ever returned the same key twice.
void derive_nonce(uint32_t salt, uint64_t chunk, uint8_t out[kNonceBytes]) {
    for (int i = 0; i < 4; ++i) out[i] = static_cast<uint8_t>(salt >> (8 * i));
    for (int i = 0; i < 8; ++i) out[4 + i] = static_cast<uint8_t>(chunk >> (8 * i));
}

/// Length-prefixed bytes. Local rather than shared with the manifest encoder: this metadata is the
/// provider's private format and must be free to move without touching the manifest's.
void put_bytes(std::string& out, const std::string& value) {
    put_varint64(out, value.size());
    out.append(value);
}

bool get_bytes(const uint8_t*& p, const uint8_t* limit, std::string& out) {
    uint64_t size = 0;
    if (!get_varint64(p, limit, size)) return false;
    if (static_cast<uint64_t>(limit - p) < size) return false;
    out.assign(reinterpret_cast<const char*>(p), static_cast<size_t>(size));
    p += size;
    return true;
}

class GcmCipher final : public ObjectCipher {
public:
    GcmCipher(SecretKey key, uint32_t salt, size_t chunk_bytes, uint64_t object_id)
        : key_(std::move(key)), salt_(salt), chunk_bytes_(chunk_bytes), object_id_(object_id) {}

    size_t chunk_bytes() const override { return chunk_bytes_; }
    size_t overhead_bytes() const override { return kTagBytes; }
    uint64_t object_id() const override { return object_id_; }

    Status seal(uint64_t chunk, Slice plaintext, Slice aad, std::string& out) override {
        CipherContext ctx;
        if (!ctx) return Status::Io;

        uint8_t nonce[kNonceBytes];
        derive_nonce(salt_, chunk, nonce);

        if (EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
            EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, kNonceBytes, nullptr) != 1 ||
            EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, key_.data(), nonce) != 1) {
            return Status::Io;
        }

        int len = 0;
        if (!aad.empty() &&
            EVP_EncryptUpdate(ctx.get(), nullptr, &len, aad.data(),
                              static_cast<int>(aad.size())) != 1) {
            return Status::Io;
        }

        const size_t at = out.size();
        out.resize(at + plaintext.size() + kTagBytes);
        auto* cursor = reinterpret_cast<uint8_t*>(out.data()) + at;

        if (EVP_EncryptUpdate(ctx.get(), cursor, &len, plaintext.data(),
                              static_cast<int>(plaintext.size())) != 1) {
            return Status::Io;
        }
        int total = len;
        if (EVP_EncryptFinal_ex(ctx.get(), cursor + total, &len) != 1) return Status::Io;
        total += len;
        if (static_cast<size_t>(total) != plaintext.size()) return Status::Io;

        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, kTagBytes, cursor + total) != 1) {
            return Status::Io;
        }
        return Status::Ok;
    }

    Status open(uint64_t chunk, Slice ciphertext, Slice aad, std::string& out) override {
        // A chunk shorter than its own tag cannot be one of ours.
        if (ciphertext.size() < kTagBytes) return Status::Corrupt;
        const size_t body = ciphertext.size() - kTagBytes;

        CipherContext ctx;
        if (!ctx) return Status::Io;

        uint8_t nonce[kNonceBytes];
        derive_nonce(salt_, chunk, nonce);

        if (EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
            EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, kNonceBytes, nullptr) != 1 ||
            EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, key_.data(), nonce) != 1) {
            return Status::Io;
        }

        int len = 0;
        if (!aad.empty() &&
            EVP_DecryptUpdate(ctx.get(), nullptr, &len, aad.data(),
                              static_cast<int>(aad.size())) != 1) {
            return Status::Io;
        }

        const size_t at = out.size();
        out.resize(at + body);
        auto* cursor = reinterpret_cast<uint8_t*>(out.data()) + at;

        if (EVP_DecryptUpdate(ctx.get(), cursor, &len, ciphertext.data(),
                              static_cast<int>(body)) != 1) {
            out.resize(at);
            return Status::Corrupt;
        }

        // The tag is set before `Final`, which is where verification happens.
        auto* tag = const_cast<uint8_t*>(ciphertext.data() + body);
        if (EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, kTagBytes, tag) != 1) {
            out.resize(at);
            return Status::Io;
        }

        // **The whole point of the suite.** A failure here means the bytes are not what was
        // written: a wrong key, a damaged chunk, or a chunk moved from somewhere else. The
        // plaintext produced above is discarded rather than returned, because unauthenticated
        // plaintext is exactly what an AEAD exists to refuse.
        int final_len = 0;
        if (EVP_DecryptFinal_ex(ctx.get(), cursor + len, &final_len) != 1) {
            out.resize(at);
            return Status::Corrupt;
        }
        return Status::Ok;
    }

private:
    SecretKey key_;
    uint32_t salt_;
    size_t chunk_bytes_;
    uint64_t object_id_;
};

}  // namespace

/// The provider's state and the metadata codec. Behind a pimpl so the OpenSSL headers stay in this
/// translation unit, exactly as `S3BlobStore::Impl` keeps the AWS ones in its.
struct Aes256GcmEncryptionProvider::Impl {
    Result<NewObject> create(uint64_t object_id) {
        auto data_key = keys->new_data_key();
        if (!data_key) return std::unexpected(data_key.error());
        if (data_key->key.size() != kKeyBytes) return std::unexpected(Status::Config);

        uint32_t salt = 0;
        if (RAND_bytes(reinterpret_cast<uint8_t*>(&salt), sizeof(salt)) != 1) {
            return std::unexpected(Status::Io);
        }

        NewObject made;
        made.metadata = encode_metadata(salt, chunk_bytes, object_id, data_key->envelope);
        made.cipher = std::make_shared<GcmCipher>(std::move(data_key->key), salt,
                                                  chunk_bytes, object_id);
        return made;
    }

    Result<std::shared_ptr<ObjectCipher>> open(Slice metadata) {
        uint32_t salt = 0;
        uint64_t recorded_chunk_bytes = 0;
        uint64_t object_id = 0;
        std::string envelope;
        if (!decode_metadata(metadata, salt, recorded_chunk_bytes, object_id, envelope)) {
            return std::unexpected(Status::Corrupt);
        }

        auto key = keys->open_data_key(Slice::from(envelope));
        if (!key) return std::unexpected(key.error());
        if (key->size() != kKeyBytes) return std::unexpected(Status::Corrupt);

        // **The recorded chunk size wins over the configured one.** Reading a file with a different
        // chunk size than it was written with produces garbage at every boundary, so configuration
        // must not be able to reinterpret an existing object.
        return std::shared_ptr<ObjectCipher>(std::make_shared<GcmCipher>(
            std::move(*key), salt, static_cast<size_t>(recorded_chunk_bytes), object_id));
    }

    static std::string encode_metadata(uint32_t salt, size_t chunk_bytes, uint64_t object_id,
                                       const std::string& envelope) {
        std::string out;
        put_varint64(out, kMetadataVersion);
        put_varint64(out, static_cast<uint64_t>(CipherSuite::Aes256Gcm));
        put_varint64(out, salt);
        put_varint64(out, chunk_bytes);
        // The identity the chunks authenticate against, fixed here so a migrated copy — which gets
        // a new file number — still opens.
        put_varint64(out, object_id);
        put_bytes(out, envelope);
        return out;
    }

    static bool decode_metadata(Slice metadata, uint32_t& salt, uint64_t& chunk_bytes,
                                uint64_t& object_id, std::string& envelope) {
        const uint8_t* p = metadata.data();
        const uint8_t* limit = p + metadata.size();

        uint64_t version = 0;
        uint64_t suite = 0;
        uint64_t salt64 = 0;
        if (!get_varint64(p, limit, version) || version != kMetadataVersion) return false;
        if (!get_varint64(p, limit, suite)) return false;
        // Reserved, never written, and refused rather than reinterpreted.
        if (suite != static_cast<uint64_t>(CipherSuite::Aes256Gcm)) return false;
        if (!get_varint64(p, limit, salt64) || salt64 > 0xFFFFFFFFull) return false;
        if (!get_varint64(p, limit, chunk_bytes) || chunk_bytes == 0) return false;
        if (!get_varint64(p, limit, object_id)) return false;
        if (!get_bytes(p, limit, envelope)) return false;
        salt = static_cast<uint32_t>(salt64);
        return p == limit;   // trailing bytes mean this is not what we wrote
    }

    std::shared_ptr<EncryptionKeyManager> keys;
    size_t chunk_bytes = Aes256GcmEncryptionProvider::kDefaultChunkBytes;
};

Result<std::shared_ptr<Aes256GcmEncryptionProvider>> Aes256GcmEncryptionProvider::open(
    std::shared_ptr<EncryptionKeyManager> keys, size_t chunk_bytes) {
    if (keys == nullptr) return std::unexpected(Status::Config);

    auto impl = std::make_unique<Impl>();
    impl->keys = std::move(keys);
    if (chunk_bytes != 0) impl->chunk_bytes = chunk_bytes;
    return std::shared_ptr<Aes256GcmEncryptionProvider>(
        new Aes256GcmEncryptionProvider(std::move(impl)));
}

Aes256GcmEncryptionProvider::Aes256GcmEncryptionProvider(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Aes256GcmEncryptionProvider::~Aes256GcmEncryptionProvider() = default;

Result<NewObject> Aes256GcmEncryptionProvider::create(uint64_t object_id) {
    return impl_->create(object_id);
}

Result<std::shared_ptr<ObjectCipher>> Aes256GcmEncryptionProvider::open(uint64_t, Slice metadata) {
    return impl_->open(metadata);
}

}  // namespace elysiumkv
