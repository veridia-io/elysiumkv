#ifndef ELYSIUMKV_SST_FORMAT_HPP
#define ELYSIUMKV_SST_FORMAT_HPP

#include "elysiumkv/slice.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Inside an SST". An open enum: 0x02..0xFF are reserved (range tombstones are the
/// deferred feature) and a reader must reject them as corruption rather than
/// guess.
enum class ValueType : uint8_t {
    Delete = 0x00,
    Put = 0x01,
};

/// ARCHITECTURE.md "A range delete is a record, not a rewrite" — a half-open range delete, `[lower, upper)`.
///
/// Bounds keep their meaning rather than their role, as everywhere else here: `lower` is
/// included and `upper` is not, which is the same convention an iterator's bounds use and the same
/// set in either direction of travel.
struct RangeTombstone {
    std::string lower;
    std::string upper;

    bool covers(Slice key) const {
        return Slice::from(lower) <= key && key < Slice::from(upper);
    }
};

/// ARCHITECTURE.md "A range delete is a record, not a rewrite" — the union of a set of range
/// tombstones: sorted by lower bound, disjoint, empties dropped.
///
/// Union is the whole of what fragmentation means here, and that follows from having no
/// sequence numbers. Every range tombstone in one file shadows exactly the same thing — everything
/// strictly older in `(level, file_number)` order — so two overlapping ranges in one file are
/// indistinguishable from the single range covering both. An engine that stamps each tombstone with
/// a sequence number has to keep the pieces apart because they shadow different sets; here there is
/// nothing to keep apart.
inline std::vector<RangeTombstone> merge_ranges(std::vector<RangeTombstone> ranges) {
    std::sort(ranges.begin(), ranges.end(), [](const RangeTombstone& a, const RangeTombstone& b) {
        return a.lower != b.lower ? a.lower < b.lower : a.upper < b.upper;
    });
    std::vector<RangeTombstone> merged;
    for (RangeTombstone& range : ranges) {
        if (range.lower >= range.upper) continue;   // an empty range deletes nothing
        // `<=` rather than `<`: [a,b) and [b,c) are contiguous, and leaving them apart would write
        // two entries that answer every query exactly as one would.
        if (!merged.empty() && range.lower <= merged.back().upper) {
            if (range.upper > merged.back().upper) merged.back().upper = std::move(range.upper);
        } else {
            merged.push_back(std::move(range));
        }
    }
    return merged;
}

/// The part of each range lying inside `[lower, upper)`. A null bound is unbounded on that side.
///
/// Compaction outputs must not overlap in what they cover, tombstones included. Two files at one
/// level are ordered by key, not by recency — deeper levels rely on never overlapping, and there is
/// no tie-break to fall back on. Handing every output the whole tombstone set, or handing it all to
/// one of them, makes an earlier-keyed sibling shadow a later-keyed one and deletes live data.
/// Clipping at the cut points tiles the range instead: each output carries exactly the part of each
/// tombstone that belongs to its own slice, and no part is lost between them.
inline std::vector<RangeTombstone> clip_ranges(const std::vector<RangeTombstone>& ranges,
                                               const std::string* lower,
                                               const std::string* upper) {
    std::vector<RangeTombstone> clipped;
    for (const RangeTombstone& range : ranges) {
        std::string low = lower != nullptr && *lower > range.lower ? *lower : range.lower;
        std::string high = upper != nullptr && *upper < range.upper ? *upper : range.upper;
        if (low < high) clipped.push_back(RangeTombstone{std::move(low), std::move(high)});
    }
    return clipped;
}

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
