#include "sst/crc32c.hpp"
#include "sst/xxhash.hpp"

#include <gtest/gtest.h>

#include <set>
#include <string>

namespace elysiumkv {
namespace {

TEST(Crc32c, StandardCheckValue) {
    // The CRC-32C check value: the CRC of "123456789".
    EXPECT_EQ(crc32c("123456789"), 0xE3069283u);
    EXPECT_EQ(crc32c(""), 0u);
}

// A build that silently fell back to the table implementation is slow rather
// than wrong, which is exactly the sort of thing that goes unnoticed.
TEST(Crc32c, ReportsWhichImplementationIsInUse) {
    const std::string impl = crc32c_implementation();
    EXPECT_TRUE(impl == "hardware" || impl == "table") << impl;
#if defined(__aarch64__) || defined(__x86_64__)
    EXPECT_EQ(impl, "hardware");
#endif
}

TEST(Crc32c, DetectsSingleBitFlips) {
    std::string data(1000, 'x');
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 7);
    const uint32_t original = crc32c(data);

    for (size_t i = 0; i < data.size(); i += 37) {
        std::string flipped = data;
        flipped[i] = static_cast<char>(flipped[i] ^ 0x01);
        EXPECT_NE(crc32c(flipped), original) << i;
    }
}

TEST(Crc32c, SeedChainsAcrossSegments) {
    const std::string whole = "the quick brown fox jumps over the lazy dog";
    const uint32_t once = crc32c(whole);

    // Chaining requires undoing the final complement of the first segment.
    const std::string head = whole.substr(0, 10);
    const std::string tail = whole.substr(10);
    const uint32_t chained = crc32c(tail, crc32c(head));
    EXPECT_EQ(chained, once);
}

TEST(XxHash64, KnownVector) {
    EXPECT_EQ(xxhash64(nullptr, 0, 0), 0xEF46DB3751D8E999ull);
}

TEST(XxHash64, IsDeterministicAndSeedSensitive) {
    const std::string key = "user:0000000042";
    const auto* p = reinterpret_cast<const uint8_t*>(key.data());
    EXPECT_EQ(xxhash64(p, key.size()), xxhash64(p, key.size()));
    EXPECT_NE(xxhash64(p, key.size(), 0), xxhash64(p, key.size(), 1));
}

// Every input length must exercise a different tail path (32-byte loop, 8, 4, 1).
TEST(XxHash64, DistinctAcrossLengthsAndValues) {
    std::set<uint64_t> hashes;
    std::string key;
    for (int i = 0; i < 300; ++i) {
        key.push_back(static_cast<char>('a' + (i % 26)));
        hashes.insert(xxhash64(reinterpret_cast<const uint8_t*>(key.data()), key.size()));
    }
    EXPECT_EQ(hashes.size(), 300u);
}

}  // namespace
}  // namespace elysiumkv
