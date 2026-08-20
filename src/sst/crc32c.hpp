#ifndef ELYSIUMKV_SST_CRC32C_HPP
#define ELYSIUMKV_SST_CRC32C_HPP

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace elysiumkv {

/// CRC32C (Castagnoli). Hardware instruction where available, portable table
/// otherwise, selected at runtime. ARCHITECTURE.md "Inside an SST" puts this on every block read, so it is
/// worth the dispatch.
uint32_t crc32c(const uint8_t* data, size_t size, uint32_t seed = 0);

inline uint32_t crc32c(std::string_view s, uint32_t seed = 0) {
    return crc32c(reinterpret_cast<const uint8_t*>(s.data()), s.size(), seed);
}

/// "hardware" or "table" — reported by a test so a build silently falling back
/// is visible rather than merely slow.
const char* crc32c_implementation();

/// The portable table path, whichever one `crc32c` would select at runtime.
///
/// **Exposed because otherwise nothing executes it.** The dispatch takes the hardware instruction
/// on every machine this ships to and every machine CI runs on, so the table is the half that only
/// runs where the instruction is absent — the one platform nobody is testing on. A build that
/// cannot dispatch to hardware depends on it for every block read.
uint32_t crc32c_portable(const uint8_t* data, size_t size, uint32_t seed = 0);

inline uint32_t crc32c_portable(std::string_view s, uint32_t seed = 0) {
    return crc32c_portable(reinterpret_cast<const uint8_t*>(s.data()), s.size(), seed);
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_CRC32C_HPP
