#ifndef ELYSIUMKV_FUZZ_REPAIR_HPP
#define ELYSIUMKV_FUZZ_REPAIR_HPP

/* Recomputes the checksum a mutation breaks, so a target reaches the body it exists to explore.
 *
 * A framed record ends in `uncompressed_len ‖ compression_type ‖ crc32c` and the CRC covers
 * everything before it, so essentially every input libFuzzer produces is refused before the decoder
 * reads one field of the body. Left that way these targets explore the checksum and nothing beneath
 * it. Each therefore decodes twice: once as given, which is what asserts a damaged record is
 * refused, and once repaired, which is what reaches the decoder.
 *
 * Repairing belongs here and nowhere else. No reader may mend a checksum; this exists because a
 * mutation cannot produce a valid one, which is a property of the fuzzer rather than of the format.
 */

#include "sst/crc32c.hpp"
#include "sst/footer.hpp"
#include "sst/format.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace elysiumkv::fuzz {

inline void store_fixed32(uint8_t* out, uint32_t value) {
    out[0] = static_cast<uint8_t>(value);
    out[1] = static_cast<uint8_t>(value >> 8);
    out[2] = static_cast<uint8_t>(value >> 16);
    out[3] = static_cast<uint8_t>(value >> 24);
}

/// Block framing: the trailing CRC covers `payload ‖ uncompressed_len ‖ compression_type`.
inline std::vector<uint8_t> repair_block_crc(const uint8_t* data, size_t size) {
    std::vector<uint8_t> bytes(data, data + size);
    if (bytes.size() < kBlockTrailerLength) return bytes;
    const size_t covered = bytes.size() - 4;
    store_fixed32(bytes.data() + covered, crc32c(bytes.data(), covered));
    return bytes;
}

/// Footer v3 only: the CRC sits immediately before the invariant trailer and covers the body ahead
/// of it. v1 and v2 carry no checksum, so a mutation of one is already reachable.
inline std::vector<uint8_t> repair_footer_crc(const uint8_t* data, size_t size) {
    std::vector<uint8_t> bytes(data, data + size);
    const auto width = static_cast<size_t>(Footer::kFooterLengthV3);
    const auto trailer_length = static_cast<size_t>(Footer::kTrailerLength);
    if (bytes.size() < width) return bytes;
    if (decode_fixed32(bytes.data() + bytes.size() - trailer_length) != Footer::kFormatVersion3) {
        return bytes;
    }
    uint8_t* body = bytes.data() + bytes.size() - width;
    const size_t covered = width - trailer_length - 4;
    store_fixed32(body + covered, crc32c(body, covered));
    return bytes;
}

}  // namespace elysiumkv::fuzz

#endif  // ELYSIUMKV_FUZZ_REPAIR_HPP
