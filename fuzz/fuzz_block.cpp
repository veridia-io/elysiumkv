/* See fuzz_manifest_edit.cpp for what these targets assert and why.
 *
 * The bound passed to `unframe_block` is what stops a declared uncompressed length from becoming an
 * allocation the input chose, so it is part of the contract rather than a tuning knob: a target
 * that passed SIZE_MAX would be fuzzing a different function from the one the engine calls. */
#include "sst/compression.hpp"

#include "repair.hpp"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    constexpr size_t kMaxUncompressed = 4u << 20;
    (void)elysiumkv::unframe_block(elysiumkv::Slice(data, size), kMaxUncompressed);
    const auto repaired = elysiumkv::fuzz::repair_block_crc(data, size);
    (void)elysiumkv::unframe_block(elysiumkv::Slice(repaired.data(), repaired.size()),
                                   kMaxUncompressed);
    return 0;
}
