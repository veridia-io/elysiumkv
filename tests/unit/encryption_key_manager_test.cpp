// The key managers on their own. `StaticEncryptionKeyManager` is the one that can be tested without
// a network; `AwsKmsEncryptionKeyManager` is exercised by the KMS smoke against LocalStack.

#include "elysiumkv/static_encryption_key_manager.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <set>
#include <string>

namespace elysiumkv::test {
namespace {

const std::string kMasterHex =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

Slice bytes_of(const std::string& s) {
    return Slice(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

TEST(StaticEncryptionKeyManager, OnlyA256BitMasterKeyIsAccepted) {
    EXPECT_EQ(StaticEncryptionKeyManager::open(Slice()).error(), Status::Config);
    const std::string short_key(31, 'k');
    EXPECT_EQ(StaticEncryptionKeyManager::open(bytes_of(short_key)).error(), Status::Config);
    const std::string long_key(33, 'k');
    EXPECT_EQ(StaticEncryptionKeyManager::open(bytes_of(long_key)).error(), Status::Config);
    const std::string exact(32, 'k');
    EXPECT_TRUE(StaticEncryptionKeyManager::open(bytes_of(exact)).has_value());
}

TEST(StaticEncryptionKeyManager, HexIsParsedAndValidated) {
    EXPECT_TRUE(StaticEncryptionKeyManager::from_hex(kMasterHex).has_value());
    EXPECT_TRUE(StaticEncryptionKeyManager::from_hex(
                    "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F")
                    .has_value())
        << "upper case is the same key";
    EXPECT_EQ(StaticEncryptionKeyManager::from_hex(kMasterHex.substr(2)).error(), Status::Config)
        << "a short string is not a 32-byte key";
    std::string bad = kMasterHex;
    bad[5] = 'z';
    EXPECT_EQ(StaticEncryptionKeyManager::from_hex(bad).error(), Status::Config);
}

TEST(StaticEncryptionKeyManager, EveryDataKeyIsFreshAndRoundTrips) {
    auto keys = StaticEncryptionKeyManager::from_hex(kMasterHex);
    ASSERT_TRUE(keys.has_value());

    std::set<std::string> envelopes;
    for (int i = 0; i < 16; ++i) {
        auto made = (*keys)->new_data_key();
        ASSERT_TRUE(made.has_value());
        EXPECT_EQ(made->key.size(), 32u);
        EXPECT_EQ(made->envelope.size(), 60u) << "nonce(12) + wrapped(32) + tag(16)";
        EXPECT_TRUE(envelopes.insert(made->envelope).second)
            << "a repeated envelope means a repeated nonce, which breaks the wrapping";

        auto reopened = (*keys)->open_data_key(bytes_of(made->envelope));
        ASSERT_TRUE(reopened.has_value());
        EXPECT_EQ(reopened->size(), 32u);
        EXPECT_EQ(std::memcmp(reopened->data(), made->key.data(), 32), 0);
    }
}

/// The engine's own nonces are derived from the chunk index, so two objects sharing a data key
/// would share nonces. Nothing downstream can detect that, which is why it is pinned here.
TEST(StaticEncryptionKeyManager, TwoObjectsNeverShareKeyMaterial) {
    auto keys = StaticEncryptionKeyManager::from_hex(kMasterHex);
    ASSERT_TRUE(keys.has_value());
    auto first = (*keys)->new_data_key();
    auto second = (*keys)->new_data_key();
    ASSERT_TRUE(first.has_value() && second.has_value());
    EXPECT_NE(std::memcmp(first->key.data(), second->key.data(), 32), 0);
}

TEST(StaticEncryptionKeyManager, ADamagedEnvelopeIsRefused) {
    auto keys = StaticEncryptionKeyManager::from_hex(kMasterHex);
    ASSERT_TRUE(keys.has_value());
    auto made = (*keys)->new_data_key();
    ASSERT_TRUE(made.has_value());

    // Every byte, including the nonce and the tag — GCM authenticates all of it.
    for (size_t i = 0; i < made->envelope.size(); ++i) {
        std::string damaged = made->envelope;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x01);
        EXPECT_FALSE((*keys)->open_data_key(bytes_of(damaged)).has_value())
            << "byte " << i << " survived being flipped";
    }
}

TEST(StaticEncryptionKeyManager, AWrongLengthEnvelopeIsCorruptNotConfig) {
    auto keys = StaticEncryptionKeyManager::from_hex(kMasterHex);
    ASSERT_TRUE(keys.has_value());
    EXPECT_EQ((*keys)->open_data_key(Slice()).error(), Status::Corrupt);
    const std::string wrong(59, 'x');
    EXPECT_EQ((*keys)->open_data_key(bytes_of(wrong)).error(), Status::Corrupt)
        << "a length ours never has is a routing or damage problem, not a wrong master key";
}

/// The remedy differs, so the status has to. A file wrapped under a master key this process was
/// not given is a configuration to fix; reporting it as damage would send an operator looking for a
/// disk fault that is not there.
TEST(StaticEncryptionKeyManager, AnotherMasterKeyIsConfigNotCorrupt) {
    auto mine = StaticEncryptionKeyManager::from_hex(kMasterHex);
    auto theirs = StaticEncryptionKeyManager::from_hex(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    ASSERT_TRUE(mine.has_value() && theirs.has_value());

    auto made = (*theirs)->new_data_key();
    ASSERT_TRUE(made.has_value());
    EXPECT_EQ((*mine)->open_data_key(bytes_of(made->envelope)).error(), Status::Config);
}

}  // namespace
}  // namespace elysiumkv::test
