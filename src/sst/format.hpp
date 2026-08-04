#ifndef ELYSIUMKV_SST_FORMAT_HPP
#define ELYSIUMKV_SST_FORMAT_HPP

#include "elysiumkv/slice.hpp"

#include <cstdint>
#include <cstring>
#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "Inside an SST". An open enum: 0x02..0xFF are reserved (range tombstones are the
/// deferred feature) and a reader must reject them as corruption rather than
/// guess.
enum class ValueType : uint8_t {
    Delete = 0x00,
    Put = 0x01,
};

inline bool is_known_value_type(uint8_t raw) {
    return raw == static_cast<uint8_t>(ValueType::Delete) ||
           raw == static_cast<uint8_t>(ValueType::Put);
}

/// uncompressed_len uint32 + compression_type uint8 + crc32c uint32.
inline constexpr size_t kBlockTrailerLength = 9;

/// ARCHITECTURE.md "Inside an SST" — the largest value a single entry may carry, and the largest key.
///
/// These exist because the read path already had a ceiling and the write path
/// had none. `SstReader::max_uncompressed()` refuses a block claiming more than
/// its bound — the backstop against a corrupted length driving a huge allocation
/// — so an entry too large to fit under that bound was accepted by put(),
/// survived flush(), and could then never be read: the write reported success
/// and the data was gone. A limit the writer does not know about is not a limit,
/// it is a trap.
///
/// A key counts against the same budget as its value because they share a block.
inline constexpr size_t kMaxValueBytes = 1u << 20;   // 1 MiB
inline constexpr size_t kMaxKeyBytes = 64u << 10;    // 64 KiB

/// The largest block a *legitimate* writer can produce: one maximal entry plus
/// its restart array and varint framing. `max_uncompressed()` must never fall
/// below this, or the reader would reject what the writer is allowed to emit.
inline constexpr size_t kMaxEntryBlockBytes = kMaxValueBytes + kMaxKeyBytes + 1024;

// --- little-endian fixed-width ------------------------------------------------

inline void put_fixed32(std::string& dst, uint32_t value) {
    uint8_t buf[4];
    buf[0] = static_cast<uint8_t>(value);
    buf[1] = static_cast<uint8_t>(value >> 8);
    buf[2] = static_cast<uint8_t>(value >> 16);
    buf[3] = static_cast<uint8_t>(value >> 24);
    dst.append(reinterpret_cast<const char*>(buf), sizeof(buf));
}

inline void put_fixed64(std::string& dst, uint64_t value) {
    uint8_t buf[8];
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<uint8_t>(value >> (8 * i));
    dst.append(reinterpret_cast<const char*>(buf), sizeof(buf));
}

inline uint32_t decode_fixed32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

inline uint64_t decode_fixed64(const uint8_t* p) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) value |= static_cast<uint64_t>(p[i]) << (8 * i);
    return value;
}

// --- varints ------------------------------------------------------------------

inline void put_varint32(std::string& dst, uint32_t value) {
    while (value >= 0x80) {
        dst.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    dst.push_back(static_cast<char>(value));
}

inline void put_varint64(std::string& dst, uint64_t value) {
    while (value >= 0x80) {
        dst.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    dst.push_back(static_cast<char>(value));
}

/// Advances `p` past the varint. Returns false on a truncated or over-long
/// encoding — every decode path is fed potentially corrupt bytes.
inline bool get_varint32(const uint8_t*& p, const uint8_t* limit, uint32_t& out) {
    uint32_t result = 0;
    for (int shift = 0; shift <= 28 && p < limit; shift += 7) {
        const uint32_t byte = *p++;
        if (byte & 0x80) {
            result |= (byte & 0x7F) << shift;
        } else {
            result |= byte << shift;
            out = result;
            return true;
        }
    }
    return false;
}

inline bool get_varint64(const uint8_t*& p, const uint8_t* limit, uint64_t& out) {
    uint64_t result = 0;
    for (int shift = 0; shift <= 63 && p < limit; shift += 7) {
        const uint64_t byte = *p++;
        if (byte & 0x80) {
            result |= (byte & 0x7F) << shift;
        } else {
            result |= byte << shift;
            out = result;
            return true;
        }
    }
    return false;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_FORMAT_HPP
