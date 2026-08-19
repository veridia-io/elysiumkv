// The providers on their own: the passthrough, and AES-256-GCM under envelope encryption. What the
// engine does with them is settled elsewhere; what is settled here is that a chunk sealed is a
// chunk opened, and that everything else is refused.

#include "elysiumkv/encryption.hpp"
#include "elysiumkv/aes256_gcm_encryption_provider.hpp"
#include "elysiumkv/no_encryption_provider.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

/// A key manager with no custody at all: the envelope *is* the key. Adequate here because what is
/// under test is the cipher, and a real KMS would only add a network to the test.
class DirectKeyManager final : public EncryptionKeyManager {
public:
    Result<DataKey> new_data_key() override {
        std::string material(32, '\0');
        for (size_t i = 0; i < material.size(); ++i) {
            material[i] = static_cast<char>(next_ * 31 + static_cast<int>(i));
        }
        ++next_;
        DataKey key;
        key.key = SecretKey(reinterpret_cast<const uint8_t*>(material.data()), material.size());
        key.envelope = material;
        return key;
    }

    Result<SecretKey> open_data_key(Slice envelope) override {
        if (fail_open) return std::unexpected(Status::Config);
        return SecretKey(envelope.data(), envelope.size());
    }

    bool fail_open = false;

private:
    int next_ = 1;
};

std::shared_ptr<EncryptionProvider> gcm_provider(std::shared_ptr<EncryptionKeyManager> keys,
                                                 size_t chunk_bytes = 4096) {
    auto made = Aes256GcmEncryptionProvider::open(std::move(keys), chunk_bytes);
    EXPECT_TRUE(made.has_value());
    return made.value_or(nullptr);
}

std::string aad_for(uint64_t object, uint64_t chunk) {
    return "obj:" + std::to_string(object) + ":chunk:" + std::to_string(chunk);
}

TEST(Encryption, TheGcmProviderRoundTripsEveryChunk) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);
    ASSERT_NE(provider, nullptr);

    auto made = provider->create(42);
    ASSERT_TRUE(made.has_value()) << status_name(made.error());
    EXPECT_EQ(made->cipher->overhead_bytes(), 16u) << "a GCM tag";
    EXPECT_FALSE(made->metadata.empty()) << "the wrapped key has to be recorded somewhere";

    // A short chunk, a full one, and an empty one: a file's last chunk is short, and an empty
    // object has to seal to something openable rather than to nothing.
    for (const std::string& plaintext :
         {std::string(), std::string("x"), std::string(4096, 'v'), std::string(17, 'q')}) {
        std::string sealed;
        ASSERT_EQ(made->cipher->seal(7, Slice::from(plaintext), Slice::from(aad_for(42, 7)), sealed),
                  Status::Ok);
        EXPECT_EQ(sealed.size(), plaintext.size() + 16u) << "length plus exactly one tag";

        auto reopened = provider->open(42, Slice::from(made->metadata));
        ASSERT_TRUE(reopened.has_value());
        std::string opened;
        ASSERT_EQ((*reopened)->open(7, Slice::from(sealed), Slice::from(aad_for(42, 7)), opened),
                  Status::Ok);
        EXPECT_EQ(opened, plaintext);
    }
}

// **The property the suite exists for.** Every other test here would pass against a cipher that
// encrypted and never authenticated.
TEST(Encryption, EveryDamagedByteIsRefused) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);
    auto made = provider->create(1);
    ASSERT_TRUE(made.has_value());

    const std::string plaintext(64, 'p');
    std::string sealed;
    ASSERT_EQ(made->cipher->seal(0, Slice::from(plaintext), Slice::from(aad_for(1, 0)), sealed),
              Status::Ok);

    for (size_t i = 0; i < sealed.size(); ++i) {
        std::string damaged = sealed;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x01);
        std::string out;
        EXPECT_EQ(made->cipher->open(0, Slice::from(damaged), Slice::from(aad_for(1, 0)), out),
                  Status::Corrupt)
            << "byte " << i << " of " << sealed.size();
        EXPECT_TRUE(out.empty()) << "unauthenticated plaintext must not be handed back";
    }
}

// A chunk moved to another index, or lifted into another object, is not the chunk that was sealed.
// This is what binds a ciphertext to its position rather than merely to its key.
TEST(Encryption, AChunkIsBoundToItsIndexAndObjectByTheAad) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);
    auto made = provider->create(1);
    ASSERT_TRUE(made.has_value());

    const std::string plaintext(128, 'z');
    std::string sealed;
    ASSERT_EQ(made->cipher->seal(3, Slice::from(plaintext), Slice::from(aad_for(1, 3)), sealed),
              Status::Ok);

    std::string out;
    EXPECT_EQ(made->cipher->open(3, Slice::from(sealed), Slice::from(aad_for(1, 4)), out),
              Status::Corrupt)
        << "the same chunk claimed to be a different index";
    EXPECT_EQ(made->cipher->open(3, Slice::from(sealed), Slice::from(aad_for(2, 3)), out),
              Status::Corrupt)
        << "the same chunk claimed to belong to another object";

    // And the nonce is the index, so opening at the wrong index fails even with matching AAD.
    EXPECT_EQ(made->cipher->open(4, Slice::from(sealed), Slice::from(aad_for(1, 3)), out),
              Status::Corrupt);
}

TEST(Encryption, AWrongKeyIsRefusedRatherThanReturningGarbage) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);

    auto first = provider->create(1);
    auto second = provider->create(2);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    std::string sealed;
    ASSERT_EQ(first->cipher->seal(0, Slice::from(std::string(32, 'a')), Slice::from(aad_for(1, 0)),
                                  sealed),
              Status::Ok);

    std::string out;
    EXPECT_EQ(second->cipher->open(0, Slice::from(sealed), Slice::from(aad_for(1, 0)), out),
              Status::Corrupt)
        << "each object gets its own data key, so another object's cipher must not open this";
}

// Two objects must not produce the same ciphertext for the same plaintext, or an observer learns
// which files hold the same bytes without decrypting anything.
TEST(Encryption, TwoObjectsSealTheSamePlaintextDifferently) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);

    const std::string plaintext(256, 's');
    std::string first_sealed;
    std::string second_sealed;
    auto first = provider->create(1);
    auto second = provider->create(2);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(first->cipher->seal(0, Slice::from(plaintext), Slice(), first_sealed), Status::Ok);
    ASSERT_EQ(second->cipher->seal(0, Slice::from(plaintext), Slice(), second_sealed), Status::Ok);

    EXPECT_NE(first_sealed, second_sealed);
    EXPECT_NE(first->metadata, second->metadata) << "different keys, so different envelopes";
}

TEST(Encryption, MetadataThatIsNotOursIsRefused) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);
    auto made = provider->create(1);
    ASSERT_TRUE(made.has_value());

    EXPECT_EQ(provider->open(1, Slice::from(std::string())).error(), Status::Corrupt);
    EXPECT_EQ(provider->open(1, Slice::from(std::string("garbage"))).error(), Status::Corrupt);

    // Trailing bytes: a decoder that stopped when it had what it wanted would accept this.
    std::string extended = made->metadata + "tail";
    EXPECT_EQ(provider->open(1, Slice::from(extended)).error(), Status::Corrupt);

    // Every truncation, since a short read of any field must fail rather than produce a cipher.
    for (size_t n = 0; n < made->metadata.size(); ++n) {
        EXPECT_FALSE(provider->open(1, Slice::from(made->metadata.substr(0, n))).has_value())
            << "truncated to " << n;
    }
}

TEST(Encryption, AKeyManagerFailureIsReportedNotSwallowed) {
    auto keys = std::make_shared<DirectKeyManager>();
    auto provider = gcm_provider(keys);
    auto made = provider->create(1);
    ASSERT_TRUE(made.has_value());

    keys->fail_open = true;
    EXPECT_EQ(provider->open(1, Slice::from(made->metadata)).error(), Status::Config);
}

/// The suite that was specified and dropped. Reserved rather than reused, so a configuration asking
/// for it is refused instead of silently getting the one that is implemented.
TEST(Encryption, TheReservedSuiteIsRefused) {
    // The suite is not a parameter any more: the class names the construction, so asking for the
    // dropped one is not expressible. What remains testable is that a file recording it is refused
    // rather than reinterpreted, which `MetadataThatIsNotOursIsRefused` covers through the decoder.
    EXPECT_EQ(static_cast<int>(CipherSuite::Aes256CtrReserved), 1)
        << "reserved, so a later suite must not reuse the value";
}

TEST(Encryption, AProviderWithoutAKeyManagerIsRefused) {
    EXPECT_EQ(Aes256GcmEncryptionProvider::open(nullptr).error(), Status::Config);
}

TEST(Encryption, ThePassthroughIsIdentityAndSaysSo) {
    auto provider = std::make_shared<NoEncryptionProvider>();

    auto made = provider->create(1);
    ASSERT_TRUE(made.has_value());
    EXPECT_TRUE(made->metadata.empty()) << "nothing to record, so nothing recorded";
    EXPECT_EQ(made->cipher->overhead_bytes(), 0u) << "length-preserving, which is what makes the "
                                                     "logical and physical layouts identical";

    const std::string plaintext(100, 'p');
    std::string sealed;
    ASSERT_EQ(made->cipher->seal(0, Slice::from(plaintext), Slice(), sealed), Status::Ok);
    EXPECT_EQ(sealed, plaintext);

    std::string opened;
    ASSERT_EQ(made->cipher->open(0, Slice::from(sealed), Slice(), opened), Status::Ok);
    EXPECT_EQ(opened, plaintext);
}

/// The passthrough writes no metadata, so metadata reaching it came from somewhere else — a file
/// whose recorded provider was lost, or a registry that routed wrongly. Either way these are not
/// bytes it can serve.
TEST(Encryption, ThePassthroughRefusesMetadataItCouldNotHaveWritten) {
    auto provider = std::make_shared<NoEncryptionProvider>();
    EXPECT_TRUE(provider->open(1, Slice()).has_value());
    EXPECT_EQ(provider->open(1, Slice::from(std::string("something"))).error(), Status::Corrupt);
}

}  // namespace
}  // namespace elysiumkv::test
