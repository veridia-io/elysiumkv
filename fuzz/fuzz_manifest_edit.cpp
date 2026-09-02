/* Structure-aware fuzzing of the four decoders (ARCHITECTURE.md "Invariants and sanitizers").
 *
 * These are the surface a store you did not write reaches. Every other gate drives the engine
 * through its own encoders, so it only decodes bytes it produced, and a decoder is exactly where a
 * reader meets bytes it has no reason to trust: a rolled-back binary, a partially-written object, a
 * bucket somebody else can write to.
 *
 * The contract each target asserts is the same and is deliberately weak: decode must return,
 * with a status or a value, and must not crash, read out of bounds or allocate without bound.
 * There is no expectation that malformed bytes are rejected — a CRC that happens to match makes a
 * decode legitimate — only that failing to reject them is not fatal. The sanitizers supply the
 * teeth; this supplies the inputs.
 */
#include "version/version_edit.hpp"

#include "repair.hpp"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    (void)elysiumkv::decode_version_edit(elysiumkv::Slice(data, size));
    const auto repaired = elysiumkv::fuzz::repair_block_crc(data, size);
    (void)elysiumkv::decode_version_edit(elysiumkv::Slice(repaired.data(), repaired.size()));
    return 0;
}
