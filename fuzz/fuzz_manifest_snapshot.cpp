/* See fuzz_manifest_edit.cpp for what these targets assert and why. */
#include "version/version_edit.hpp"

#include "repair.hpp"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    (void)elysiumkv::decode_version_snapshot(elysiumkv::Slice(data, size));
    const auto repaired = elysiumkv::fuzz::repair_block_crc(data, size);
    (void)elysiumkv::decode_version_snapshot(elysiumkv::Slice(repaired.data(), repaired.size()));
    return 0;
}
