#include "elysiumkv/static_encryption_key_manager.hpp"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <array>
#include <string>
#include <utility>

namespace elysiumkv {
namespace {

constexpr size_t kKeyBytes = 32;    // AES-256, for both the master key and the data keys it wraps
constexpr size_t kNonceBytes = 12;
constexpr size_t kTagBytes = 16;

/// nonce ‖ wrapped key ‖ tag. Fixed width, so decoding is a length check rather than a parser.
constexpr size_t kEnvelopeBytes = kNonceBytes + kKeyBytes + kTagBytes;

class Context {
public:
    Context() : ctx_(EVP_CIPHER_CTX_new()) {}
    ~Context() {
        if (ctx_ != nullptr) EVP_CIPHER_CTX_free(ctx_);
    }
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;

    EVP_CIPHER_CTX* get() const { return ctx_; }
    explicit operator bool() const { return ctx_ != nullptr; }

private:
    EVP_CIPHER_CTX* ctx_;
};

int from_hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

}  // namespace

struct StaticEncryptionKeyManager::Impl {
    SecretKey master;

    /// **A fresh nonce per wrap, from the system generator.** The master key is used for every
    /// object, so this is the one place a repeat would matter — and unlike the per-object data
    /// keys, nothing structural stops it. 96 random bits per wrap is the standard margin.
    Result<DataKey> wrap() {
        std::array<uint8_t, kKeyBytes> plain{};
        std::array<uint8_t, kNonceBytes> nonce{};
        if (RAND_bytes(plain.data(), static_cast<int>(plain.size())) != 1 ||
            RAND_bytes(nonce.data(), static_cast<int>(nonce.size())) != 1) {
            secure_zero(plain.data(), plain.size());
            return std::unexpected(Status::Io);
        }

        Context ctx;
        if (!ctx) {
            secure_zero(plain.data(), plain.size());
            return std::unexpected(Status::Io);
        }

        std::string envelope(kEnvelopeBytes, '\0');
        auto* out = reinterpret_cast<uint8_t*>(envelope.data());
        std::copy(nonce.begin(), nonce.end(), out);

        int len = 0;
        const bool ok =
            EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, master.data(),
                               nonce.data()) == 1 &&
            EVP_EncryptUpdate(ctx.get(), out + kNonceBytes, &len, plain.data(),
                              static_cast<int>(plain.size())) == 1 &&
            EVP_EncryptFinal_ex(ctx.get(), out + kNonceBytes + len, &len) == 1 &&
            EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, kTagBytes,
                                out + kNonceBytes + kKeyBytes) == 1;
        if (!ok) {
            secure_zero(plain.data(), plain.size());
            return std::unexpected(Status::Io);
        }

        DataKey made;
        made.key = SecretKey(plain.data(), plain.size());
        made.envelope = std::move(envelope);
        secure_zero(plain.data(), plain.size());
        return made;
    }

    Result<SecretKey> unwrap(Slice envelope) {
        // A length this is not cannot be one of ours, and the tag check below would report it as
        // damage rather than as the wrong manager.
        if (envelope.size() != kEnvelopeBytes) return std::unexpected(Status::Corrupt);

        Context ctx;
        if (!ctx) return std::unexpected(Status::Io);

        std::array<uint8_t, kKeyBytes> plain{};
        auto* tag = const_cast<uint8_t*>(envelope.data() + kNonceBytes + kKeyBytes);
        int len = 0;
        const bool ok =
            EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, master.data(),
                               envelope.data()) == 1 &&
            EVP_DecryptUpdate(ctx.get(), plain.data(), &len, envelope.data() + kNonceBytes,
                              static_cast<int>(kKeyBytes)) == 1 &&
            EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, kTagBytes, tag) == 1 &&
            EVP_DecryptFinal_ex(ctx.get(), plain.data() + len, &len) == 1;
        if (!ok) {
            secure_zero(plain.data(), plain.size());
            // **The tag failing means a different master key, not damaged bytes.** Both are
            // possible and this cannot tell them apart, so it reports the one whose remedy is
            // cheaper to try: configure the key that wrote this.
            return std::unexpected(Status::Config);
        }

        SecretKey key(plain.data(), plain.size());
        secure_zero(plain.data(), plain.size());
        return key;
    }
};

Result<std::shared_ptr<StaticEncryptionKeyManager>> StaticEncryptionKeyManager::open(
    Slice master_key) {
    if (master_key.size() != kKeyBytes) return std::unexpected(Status::Config);

    auto impl = std::make_unique<Impl>();
    impl->master = SecretKey(master_key.data(), master_key.size());
    return std::shared_ptr<StaticEncryptionKeyManager>(
        new StaticEncryptionKeyManager(std::move(impl)));
}

Result<std::shared_ptr<StaticEncryptionKeyManager>> StaticEncryptionKeyManager::from_hex(
    std::string_view hex) {
    if (hex.size() != kKeyBytes * 2) return std::unexpected(Status::Config);

    std::array<uint8_t, kKeyBytes> key{};
    for (size_t i = 0; i < kKeyBytes; ++i) {
        const int high = from_hex_digit(hex[2 * i]);
        const int low = from_hex_digit(hex[2 * i + 1]);
        if (high < 0 || low < 0) {
            secure_zero(key.data(), key.size());
            return std::unexpected(Status::Config);
        }
        key[i] = static_cast<uint8_t>(high * 16 + low);
    }
    auto made = open(Slice(key.data(), key.size()));
    secure_zero(key.data(), key.size());
    return made;
}

StaticEncryptionKeyManager::StaticEncryptionKeyManager(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

StaticEncryptionKeyManager::~StaticEncryptionKeyManager() = default;

Result<DataKey> StaticEncryptionKeyManager::new_data_key() { return impl_->wrap(); }

Result<SecretKey> StaticEncryptionKeyManager::open_data_key(Slice envelope) {
    return impl_->unwrap(envelope);
}

}  // namespace elysiumkv
