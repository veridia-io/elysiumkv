#include "sst/crc32c.hpp"

#include <array>

#if defined(__x86_64__) || defined(_M_X64)
#include <nmmintrin.h>
#define ELYSIUMKV_CRC32C_X86 1
#elif defined(__aarch64__)
#define ELYSIUMKV_CRC32C_ARM 1
#if defined(__linux__)
#include <sys/auxv.h>
#endif
#endif

namespace elysiumkv {
namespace {

constexpr uint32_t kPolynomial = 0x82F63B78u;  // reversed Castagnoli

constexpr std::array<uint32_t, 256> make_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1u) ? (crc >> 1) ^ kPolynomial : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

constexpr std::array<uint32_t, 256> kTable = make_table();

uint32_t crc32c_table(const uint8_t* data, size_t size, uint32_t crc) {
    for (size_t i = 0; i < size; ++i) {
        crc = kTable[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc;
}

#if defined(ELYSIUMKV_CRC32C_X86)
bool have_hardware() {
    static const bool supported = __builtin_cpu_supports("sse4.2");
    return supported;
}

__attribute__((target("sse4.2"))) uint32_t crc32c_hardware(const uint8_t* data, size_t size,
                                                           uint32_t crc) {
    while (size >= 8) {
        // memcpy, not a cast: block payloads are byte-addressed and land at
        // arbitrary offsets, so reading one through a `const uint64_t*` is an
        // unaligned load — UB that UBSan reports even though x86 tolerates it in
        // hardware. The copy compiles to the same single `mov`.
        uint64_t chunk;
        __builtin_memcpy(&chunk, data, sizeof(chunk));
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, chunk));
        data += 8;
        size -= 8;
    }
    while (size > 0) {
        crc = _mm_crc32_u8(crc, *data++);
        --size;
    }
    return crc;
}
#elif defined(ELYSIUMKV_CRC32C_ARM)
// CRC32 is *optional* in ARMv8-A and only mandatory from ARMv8.1-A, so
// `__aarch64__` alone does not imply it — which is why this is detected rather
// than assumed. Apple silicon always has it; on Linux the kernel reports it
// through the aux vector. When the compiler was told about the feature up front
// (-march=armv8.1-a, or an -mcpu naming a core that has it) the branch folds
// away at compile time.
bool have_hardware() {
#if defined(__ARM_FEATURE_CRC32) || defined(__APPLE__)
    return true;
#elif defined(__linux__) && defined(HWCAP_CRC32)
    static const bool supported = (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
    return supported;
#else
    return false;
#endif
}

// The instructions are written out rather than called through <arm_acle.h>:
// gcc declares __crc32cd and friends only under __ARM_FEATURE_CRC32, so they do
// not exist in exactly the build that has to detect the feature at runtime. The
// target attribute is what lets the assembler accept them there.
__attribute__((target("+crc"))) uint32_t crc32c_hardware(const uint8_t* data, size_t size,
                                                         uint32_t crc) {
    while (size >= 8) {
        uint64_t chunk;
        __builtin_memcpy(&chunk, data, sizeof(chunk));
        __asm__("crc32cx %w0, %w0, %x1" : "+r"(crc) : "r"(chunk));
        data += 8;
        size -= 8;
    }
    while (size > 0) {
        const uint32_t byte = *data++;
        __asm__("crc32cb %w0, %w0, %w1" : "+r"(crc) : "r"(byte));
        --size;
    }
    return crc;
}
#else
bool have_hardware() { return false; }
uint32_t crc32c_hardware(const uint8_t* data, size_t size, uint32_t crc) {
    return crc32c_table(data, size, crc);
}
#endif

}  // namespace

uint32_t crc32c(const uint8_t* data, size_t size, uint32_t seed) {
    uint32_t crc = seed ^ 0xFFFFFFFFu;
    crc = have_hardware() ? crc32c_hardware(data, size, crc) : crc32c_table(data, size, crc);
    return crc ^ 0xFFFFFFFFu;
}

const char* crc32c_implementation() { return have_hardware() ? "hardware" : "table"; }

uint32_t crc32c_portable(const uint8_t* data, size_t size, uint32_t seed) {
    return crc32c_table(data, size, seed ^ 0xFFFFFFFFu) ^ 0xFFFFFFFFu;
}

}  // namespace elysiumkv
