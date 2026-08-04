#ifndef ELYSIUMKV_SST_XXHASH_HPP
#define ELYSIUMKV_SST_XXHASH_HPP

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace elysiumkv {

/// XXH64, inline (ARCHITECTURE.md "Dependencies and artifacts" — a dependency is not warranted for ~150 lines). Used by
/// the bloom filter; double-hashing derives every probe from this one value.
namespace detail {

constexpr uint64_t kPrime1 = 0x9E3779B185EBCA87ull;
constexpr uint64_t kPrime2 = 0xC2B2AE3D27D4EB4Full;
constexpr uint64_t kPrime3 = 0x165667B19E3779F9ull;
constexpr uint64_t kPrime4 = 0x85EBCA77C2B2AE63ull;
constexpr uint64_t kPrime5 = 0x27D4EB2F165667C5ull;

inline uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }

inline uint64_t read64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}
inline uint32_t read32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline uint64_t round(uint64_t acc, uint64_t input) {
    acc += input * kPrime2;
    acc = rotl64(acc, 31);
    acc *= kPrime1;
    return acc;
}

inline uint64_t merge_round(uint64_t acc, uint64_t val) {
    val = round(0, val);
    acc ^= val;
    acc = acc * kPrime1 + kPrime4;
    return acc;
}

}  // namespace detail

inline uint64_t xxhash64(const uint8_t* data, size_t size, uint64_t seed = 0) {
    using namespace detail;
    const uint8_t* p = data;
    const uint8_t* const end = data + size;
    uint64_t h;

    if (size >= 32) {
        const uint8_t* const limit = end - 32;
        uint64_t v1 = seed + kPrime1 + kPrime2;
        uint64_t v2 = seed + kPrime2;
        uint64_t v3 = seed;
        uint64_t v4 = seed - kPrime1;
        do {
            v1 = round(v1, read64(p));
            p += 8;
            v2 = round(v2, read64(p));
            p += 8;
            v3 = round(v3, read64(p));
            p += 8;
            v4 = round(v4, read64(p));
            p += 8;
        } while (p <= limit);

        h = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        h = merge_round(h, v4);
    } else {
        h = seed + kPrime5;
    }

    h += static_cast<uint64_t>(size);

    // Sizes rather than pointer arithmetic: `p` is null for an empty input, and
    // `p + 8` on a null pointer is undefined even when the result is unused.
    while (static_cast<size_t>(end - p) >= 8) {
        h ^= round(0, read64(p));
        h = rotl64(h, 27) * kPrime1 + kPrime4;
        p += 8;
    }
    if (static_cast<size_t>(end - p) >= 4) {
        h ^= static_cast<uint64_t>(read32(p)) * kPrime1;
        h = rotl64(h, 23) * kPrime2 + kPrime3;
        p += 4;
    }
    while (p != end) {
        h ^= static_cast<uint64_t>(*p) * kPrime5;
        h = rotl64(h, 11) * kPrime1;
        ++p;
    }

    h ^= h >> 33;
    h *= kPrime2;
    h ^= h >> 29;
    h *= kPrime3;
    h ^= h >> 32;
    return h;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_XXHASH_HPP
