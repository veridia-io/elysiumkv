#include "sst/compression.hpp"
#include "sst/crc32c.hpp"
#include "sst/format.hpp"

#include <gtest/gtest.h>

#include <string>

namespace elysiumkv {
namespace {

constexpr size_t kMaxUncompressed = 1u << 20;

std::string compressible(size_t size) {
    std::string s;
    while (s.size() < size) s += "the quick brown fox jumps over the lazy dog ";
    s.resize(size);
    return s;
}

std::string incompressible(size_t size) {
    std::string s(size, '\0');
    uint64_t state = 0x2545F4914F6CDD1Dull;
    for (size_t i = 0; i < size; ++i) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        s[i] = static_cast<char>(state);
    }
    return s;
}

uint8_t stored_codec(const std::string& framed) {
    return static_cast<uint8_t>(framed[framed.size() - 5]);
}

class CompressionTest : public ::testing::TestWithParam<Compression> {};

TEST_P(CompressionTest, RoundTrips) {
    const std::string content = compressible(8192);
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(content), GetParam(), framed), Status::Ok);
    EXPECT_GE(framed.size(), kBlockTrailerLength);

    auto decoded = unframe_block(Slice::from(framed), kMaxUncompressed);
    ASSERT_TRUE(decoded.has_value()) << status_name(decoded.error());
    EXPECT_EQ(std::string(decoded->begin(), decoded->end()), content);
}

TEST_P(CompressionTest, EmptyContentRoundTrips) {
    std::string framed;
    ASSERT_EQ(frame_block(Slice(), GetParam(), framed), Status::Ok);
    auto decoded = unframe_block(Slice::from(framed), kMaxUncompressed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->empty());
}

// The CRC covers the *stored* bytes, so corruption is caught before anything
// reaches the decompressor (ARCHITECTURE.md "Inside an SST").
TEST_P(CompressionTest, AnyCorruptedByteIsDetected) {
    const std::string content = compressible(4096);
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(content), GetParam(), framed), Status::Ok);

    for (size_t i = 0; i < framed.size(); i += 13) {
        std::string damaged = framed;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x40);
        auto decoded = unframe_block(Slice::from(damaged), kMaxUncompressed);
        ASSERT_FALSE(decoded.has_value()) << "byte " << i;
        EXPECT_EQ(decoded.error(), Status::Corrupt) << "byte " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(Codecs, CompressionTest,
                         ::testing::Values(Compression::None, Compression::Lz4,
                                           Compression::Zstd),
                         // Not `info`: gtest's own macro expansion declares a
                         // parameter by that name, and -Wshadow is an error.
                         [](const auto& codec) {
                             switch (codec.param) {
                                 case Compression::None: return "None";
                                 case Compression::Lz4: return "Lz4";
                                 case Compression::Zstd: return "Zstd";
                             }
                             return "Unknown";
                         });

TEST(Compression, CompressibleDataActuallyShrinks) {
    const std::string content = compressible(64 * 1024);
    for (Compression codec : {Compression::Lz4, Compression::Zstd}) {
        std::string framed;
        ASSERT_EQ(frame_block(Slice::from(content), codec, framed), Status::Ok);
        EXPECT_LT(framed.size(), content.size() / 2);
        EXPECT_EQ(stored_codec(framed), static_cast<uint8_t>(codec));
    }
}

// A block the codec fails to shrink is stored raw. The per-block type byte means
// the reader neither knows nor cares which happened.
TEST(Compression, IncompressibleBlocksFallBackToNone) {
    const std::string content = incompressible(4096);
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(content), Compression::Zstd, framed), Status::Ok);
    EXPECT_EQ(stored_codec(framed), static_cast<uint8_t>(Compression::None));

    auto decoded = unframe_block(Slice::from(framed), kMaxUncompressed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(std::string(decoded->begin(), decoded->end()), content);
}

// ARCHITECTURE.md "Inside an SST" — a corrupted length field must not request an arbitrary allocation. The
// CRC normally catches it first, so this test recomputes a *valid* CRC over the
// damaged header — which is exactly the case the bound exists for.
TEST(Compression, AbsurdUncompressedLengthIsRejectedBeforeAllocating) {
    const std::string content = compressible(4096);
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(content), Compression::Zstd, framed), Status::Ok);

    std::string damaged = framed;
    const size_t len_offset = damaged.size() - kBlockTrailerLength;
    damaged[len_offset + 0] = static_cast<char>(0xFF);
    damaged[len_offset + 1] = static_cast<char>(0xFF);
    damaged[len_offset + 2] = static_cast<char>(0xFF);
    damaged[len_offset + 3] = static_cast<char>(0x7F);  // ~2 GiB

    const uint32_t crc = crc32c(std::string_view(damaged.data(), damaged.size() - 4));
    std::string fixed(damaged, 0, damaged.size() - 4);
    put_fixed32(fixed, crc);

    auto decoded = unframe_block(Slice::from(fixed), kMaxUncompressed);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error(), Status::Corrupt);
}

TEST(Compression, UnknownCodecByteIsCorrupt) {
    const std::string content = compressible(128);
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(content), Compression::None, framed), Status::Ok);

    std::string damaged(framed, 0, framed.size() - 4);
    damaged[damaged.size() - 1] = static_cast<char>(0x7F);
    put_fixed32(damaged, crc32c(std::string_view(damaged.data(), damaged.size())));

    auto decoded = unframe_block(Slice::from(damaged), kMaxUncompressed);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error(), Status::Corrupt);
}

TEST(Compression, TruncatedFrameIsCorrupt) {
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(std::string("hello")), Compression::None, framed),
              Status::Ok);
    for (size_t len = 0; len < kBlockTrailerLength; ++len) {
        auto decoded = unframe_block(Slice(reinterpret_cast<const uint8_t*>(framed.data()), len),
                                     kMaxUncompressed);
        ASSERT_FALSE(decoded.has_value()) << len;
        EXPECT_EQ(decoded.error(), Status::Corrupt);
    }
}

}  // namespace
}  // namespace elysiumkv
