// The framing every manifest payload gets: compress, seal, head. What the cipher does is settled by
// the encryption suites; what is settled here is that the framing round-trips, that a payload cannot
// be moved to another address, and — the one that matters most — that the three ways it can fail
// are three different statuses, because replay reacts to each differently.

#include "version/manifest_payload.hpp"

#include "support/test_encryption.hpp"
#include "elysiumkv/static_encryption_key_manager.hpp"

#include <gtest/gtest.h>

#include <string>

namespace elysiumkv::test {
namespace {

ProviderRegistry encrypted(int seed = 1) {
    ProviderRegistry registry = passthrough_registry();
    registry.providers["gcm"] = make_test_provider(seed);
    registry.primary = "gcm";
    return registry;
}

/// Compressible, so the zstd path is the one exercised unless a test says otherwise.
std::string manifest_like(size_t entries = 200) {
    std::string out;
    for (size_t i = 0; i < entries; ++i) {
        out += "store-a/file-" + std::to_string(i) + "/key:aaaaaaaaaaaaaaaa\n";
    }
    return out;
}

TEST(ManifestPayload, RoundTripsThroughTheProvider) {
    const ProviderRegistry registry = encrypted();
    const std::string plain = manifest_like();

    auto framed = ManifestPayload::seal(registry, 7, "snap#000000000007", Slice::from(plain));
    ASSERT_TRUE(framed.has_value());
    EXPECT_EQ(framed->find(plain.substr(0, 32)), std::string::npos)
            << "the payload is in the clear";

    std::string why;
    auto opened =
            ManifestPayload::open(registry, 7, "snap#000000000007", Slice::from(*framed), why);
    ASSERT_TRUE(opened.has_value());
    EXPECT_EQ(*opened, plain);
    EXPECT_TRUE(why.empty());
}

/// The passthrough is a provider like any other here too, so an unencrypted store gets the same
/// framing rather than a second format.
TEST(ManifestPayload, ThePassthroughUsesTheSameFraming) {
    const ProviderRegistry registry = passthrough_registry();
    const std::string plain = manifest_like();

    auto framed = ManifestPayload::seal(registry, 1, "snap#000000000001", Slice::from(plain));
    ASSERT_TRUE(framed.has_value());
    EXPECT_GE(framed->size(), ManifestPayload::kHeaderBytes);

    std::string why;
    auto opened =
            ManifestPayload::open(registry, 1, "snap#000000000001", Slice::from(*framed), why);
    ASSERT_TRUE(opened.has_value());
    EXPECT_EQ(*opened, plain);
}

TEST(ManifestPayload, CompressionIsAppliedAndIsOptional) {
    const ProviderRegistry registry = passthrough_registry();

    const std::string compressible = manifest_like(400);
    auto packed = ManifestPayload::seal(registry, 1, "a", Slice::from(compressible));
    ASSERT_TRUE(packed.has_value());
    EXPECT_LT(packed->size(), compressible.size())
        << "a manifest-shaped payload has to shrink, or encrypting it costs what compression saved";

    // splitmix64 output does not compress, so this takes the store-as-is branch — which must
    // still round-trip rather than being a path nothing exercises.
    std::string random(4096, '\0');
    uint64_t state = 1;
    for (char& c : random) {
        state += 0x9E3779B97F4A7C15ull;
        uint64_t z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        c = static_cast<char>(z ^ (z >> 31));
    }
    auto raw = ManifestPayload::seal(registry, 1, "a", Slice::from(random));
    ASSERT_TRUE(raw.has_value());
    std::string why;
    auto opened = ManifestPayload::open(registry, 1, "a", Slice::from(*raw), why);
    ASSERT_TRUE(opened.has_value());
    EXPECT_EQ(*opened, random);
}

TEST(ManifestPayload, AnEmptyPayloadRoundTrips) {
    const ProviderRegistry registry = encrypted();
    auto framed = ManifestPayload::seal(registry, 1, "a", Slice());
    ASSERT_TRUE(framed.has_value());
    std::string why;
    auto opened = ManifestPayload::open(registry, 1, "a", Slice::from(*framed), why);
    ASSERT_TRUE(opened.has_value());
    EXPECT_TRUE(opened->empty());
}

/// **The property the AAD exists for.** A manifest payload means nothing without its address — an
/// edit replayed at another sequence applies the wrong change — and there is no file number inside
/// the bytes to catch it the way there is for an SST.
TEST(ManifestPayload, APayloadCannotBeMovedToAnotherAddress) {
    const ProviderRegistry registry = encrypted();
    const std::string plain = manifest_like();

    auto framed = ManifestPayload::seal(registry, 3, "edit#000000000003#000000000001",
                                        Slice::from(plain));
    ASSERT_TRUE(framed.has_value());

    std::string why;
    auto moved = ManifestPayload::open(registry, 3, "edit#000000000003#000000000002",
                                       Slice::from(*framed), why);
    EXPECT_FALSE(moved.has_value()) << "an edit opened at another sequence number";
    auto other_generation = ManifestPayload::open(registry, 4, "edit#000000000004#000000000001",
                                                  Slice::from(*framed), why);
    EXPECT_FALSE(other_generation.has_value()) << "an edit opened under another generation";
}

TEST(ManifestPayload, EveryDamagedByteIsRefused) {
    const ProviderRegistry registry = encrypted();
    auto framed = ManifestPayload::seal(registry, 1, "a", Slice::from(manifest_like(8)));
    ASSERT_TRUE(framed.has_value());

    for (size_t i = 0; i < framed->size(); ++i) {
        std::string damaged = *framed;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x01);
        std::string why;
        EXPECT_FALSE(
            ManifestPayload::open(registry, 1, "a", Slice::from(damaged), why).has_value())
            << "byte " << i << " survived being flipped";
    }
}

/// **Truncation is detectable rather than ambiguous**, which is what lets replay treat a short
/// payload as the torn write it is without that decision also swallowing a wrong key.
TEST(ManifestPayload, ATruncatedPayloadIsCorrupt) {
    const ProviderRegistry registry = encrypted();
    auto framed = ManifestPayload::seal(registry, 1, "a", Slice::from(manifest_like()));
    ASSERT_TRUE(framed.has_value());

    for (size_t keep : {size_t{0}, size_t{1}, ManifestPayload::kHeaderBytes - 1,
                        ManifestPayload::kHeaderBytes, framed->size() / 2, framed->size() - 1}) {
        const std::string cut = framed->substr(0, keep);
        std::string why;
        auto opened = ManifestPayload::open(registry, 1, "a", Slice::from(cut), why);
        ASSERT_FALSE(opened.has_value()) << "kept " << keep;
        EXPECT_EQ(opened.error(), Status::Corrupt) << "kept " << keep;
    }
    // Padding is not a shorter object, and must not be accepted as one either.
    std::string padded = *framed + std::string(16, 'x');
    std::string why;
    EXPECT_EQ(ManifestPayload::open(registry, 1, "a", Slice::from(padded), why).error(),
              Status::Corrupt);
}

/// **`Config`, not `Corrupt`, and the difference is load-bearing.** Replay treats `Corrupt` as an
/// unacknowledged edit and stops; if an unregistered provider looked the same, a store opened with
/// the wrong configuration would come up quietly on a truncated history.
TEST(ManifestPayload, AnUnregisteredProviderIsConfigAndSaysWhich) {
    const ProviderRegistry writer = encrypted();
    auto framed = ManifestPayload::seal(writer, 1, "a", Slice::from(manifest_like()));
    ASSERT_TRUE(framed.has_value());

    const ProviderRegistry reader = passthrough_registry();
    std::string why;
    auto opened = ManifestPayload::open(reader, 1, "a", Slice::from(*framed), why);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error(), Status::Config);
    EXPECT_NE(why.find("gcm"), std::string::npos) << why;
}

/// The same provider id holding a different key must not open the payload. A real key manager is
/// needed to state this — the test double's envelope *is* its key, so any instance of it unwraps
/// any envelope, which is exactly the property that would make this test vacuous.
TEST(ManifestPayload, AWrongKeyUnderTheSameIdIsRefused) {
    auto under = [](const char* master_hex) {
        auto keys = StaticEncryptionKeyManager::from_hex(master_hex);
        ProviderRegistry registry = passthrough_registry();
        registry.providers["gcm"] = *Aes256GcmEncryptionProvider::open(*keys);
        registry.primary = "gcm";
        return registry;
    };
    const ProviderRegistry mine =
        under("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    const ProviderRegistry theirs =
        under("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

    auto framed = ManifestPayload::seal(mine, 1, "a", Slice::from(manifest_like()));
    ASSERT_TRUE(framed.has_value());

    std::string why;
    auto opened = ManifestPayload::open(mine, 1, "a", Slice::from(*framed), why);
    ASSERT_TRUE(opened.has_value()) << "the right key must still work, or this proves nothing";

    EXPECT_FALSE(ManifestPayload::open(theirs, 1, "a", Slice::from(*framed), why).has_value());
}

/// Framing that is not ours at all — an older format, or a write that never landed — reads as
/// `Corrupt` so replay treats it as the unacknowledged edit it is.
TEST(ManifestPayload, ForeignBytesAreCorrupt) {
    const ProviderRegistry registry = encrypted();
    const std::string junk(64, 'j');
    std::string why;
    auto opened = ManifestPayload::open(registry, 1, "a", Slice::from(junk), why);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error(), Status::Corrupt);
}

}  // namespace
}  // namespace elysiumkv::test
