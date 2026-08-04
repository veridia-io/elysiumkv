#include "sst/bloom.hpp"

#include "sst/format.hpp"
#include "sst/xxhash.hpp"

#include <algorithm>

namespace elysiumkv {
namespace {

constexpr size_t kBlockBits = 512;
constexpr size_t kBlockBytes = kBlockBits / 8;  // one cache line
constexpr size_t kFilterTrailer = sizeof(uint32_t) + 1;

/// Second hash for double hashing. Must be odd so the probe sequence covers the
/// block rather than cycling through a subset.
inline uint64_t derive_h2(uint64_t hash) { return (hash >> 32) | 1u; }

inline size_t block_index(uint64_t hash, uint32_t num_blocks) {
    // Multiply-shift: uses the high bits, which are independent of the low bits
    // driving the probes.
    return static_cast<size_t>(((hash >> 32) * num_blocks) >> 32);
}

}  // namespace

BloomBuilder::BloomBuilder(int bits_per_key, int num_probes)
    : bits_per_key_(std::max(1, bits_per_key)), num_probes_(std::clamp(num_probes, 1, 32)) {}

void BloomBuilder::add(Slice key) { hashes_.push_back(xxhash64(key.data(), key.size())); }

std::string BloomBuilder::finish() {
    const size_t total_bits = std::max<size_t>(kBlockBits, hashes_.size() *
                                                               static_cast<size_t>(bits_per_key_));
    const auto num_blocks = static_cast<uint32_t>((total_bits + kBlockBits - 1) / kBlockBits);

    std::string filter(static_cast<size_t>(num_blocks) * kBlockBytes, '\0');
    auto* bits = reinterpret_cast<uint8_t*>(filter.data());

    for (uint64_t hash : hashes_) {
        const size_t base = block_index(hash, num_blocks) * kBlockBytes;
        const uint64_t h2 = derive_h2(hash);
        uint64_t h = hash;
        for (int probe = 0; probe < num_probes_; ++probe) {
            const size_t bit = h % kBlockBits;
            bits[base + bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
            h += h2;
        }
    }

    put_fixed32(filter, num_blocks);
    filter.push_back(static_cast<char>(static_cast<uint8_t>(num_probes_)));
    return filter;
}

bool bloom_may_contain(Slice filter, Slice key) {
    if (filter.size() <= kFilterTrailer) return true;

    const size_t bitmap_bytes = filter.size() - kFilterTrailer;
    const uint32_t num_blocks = decode_fixed32(filter.data() + bitmap_bytes);
    const uint8_t num_probes = filter.data()[filter.size() - 1];
    if (num_blocks == 0 || num_probes == 0 ||
        static_cast<size_t>(num_blocks) * kBlockBytes != bitmap_bytes) {
        return true;  // malformed: fall through to the data block
    }

    const uint64_t hash = xxhash64(key.data(), key.size());
    const size_t base = block_index(hash, num_blocks) * kBlockBytes;
    const uint64_t h2 = derive_h2(hash);
    uint64_t h = hash;
    for (uint8_t probe = 0; probe < num_probes; ++probe) {
        const size_t bit = h % kBlockBits;
        if ((filter.data()[base + bit / 8] & (1u << (bit % 8))) == 0) return false;
        h += h2;
    }
    return true;
}

}  // namespace elysiumkv
