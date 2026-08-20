// The two CRC32C implementations against each other.
//
// **The table path is otherwise dead code that ships.** `crc32c` dispatches to the hardware
// instruction on every machine this is built for and every machine CI runs on, so a coverage run
// shows the portable half never once executing — while a platform without the instruction depends
// on it for every block read and every footer. Comparing the two is the only way it is exercised
// anywhere, and it is exact rather than approximate: they must agree bit for bit or one of them is
// producing checksums the other will reject.

#include "sst/crc32c.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

TEST(Crc32c, TheTableAgreesWithTheKnownVector) {
    // The Castagnoli check value, so the table is pinned to the standard rather than only to
    // whatever the hardware on this machine happens to compute.
    EXPECT_EQ(crc32c_portable("123456789"), 0xE3069283u);
    EXPECT_EQ(crc32c("123456789"), 0xE3069283u);
}

TEST(Crc32c, TheTableAgreesWithTheDispatchedImplementation) {
    std::mt19937_64 rng(1234);
    std::vector<uint8_t> data(4096);
    for (uint8_t& byte : data) byte = static_cast<uint8_t>(rng());

    // Every length up to a little past the word and block strides the hardware path unrolls over,
    // which is where an implementation that handles the tail wrongly diverges.
    for (size_t length = 0; length <= 300; ++length) {
        EXPECT_EQ(crc32c(data.data(), length), crc32c_portable(data.data(), length))
            << "length " << length;
    }
    for (size_t length : {size_t{511}, size_t{512}, size_t{513}, size_t{1023}, size_t{1024},
                          size_t{4095}, size_t{4096}}) {
        EXPECT_EQ(crc32c(data.data(), length), crc32c_portable(data.data(), length))
            << "length " << length;
    }

    // Unaligned starts, for the same reason.
    for (size_t offset = 1; offset < 16; ++offset) {
        EXPECT_EQ(crc32c(data.data() + offset, 257), crc32c_portable(data.data() + offset, 257))
            << "offset " << offset;
    }
}

/// The seed is what makes a running checksum possible, so the two have to agree on it as well —
/// a mismatch there is invisible to any single-shot comparison.
TEST(Crc32c, TheTableAgreesOnSeededContinuation) {
    std::mt19937_64 rng(99);
    std::string data(1000, '\0');
    for (char& c : data) c = static_cast<char>(rng());

    for (uint32_t seed : {0u, 1u, 0xFFFFFFFFu, 0xDEADBEEFu}) {
        EXPECT_EQ(crc32c(data, seed), crc32c_portable(data, seed)) << "seed " << seed;
    }

    // Split in two and continue: the whole must equal the halves chained through the seed, for
    // both implementations.
    const std::string first = data.substr(0, 337);
    const std::string second = data.substr(337);
    EXPECT_EQ(crc32c(data), crc32c(second, crc32c(first)));
    EXPECT_EQ(crc32c_portable(data), crc32c_portable(second, crc32c_portable(first)));
}

}  // namespace
}  // namespace elysiumkv
