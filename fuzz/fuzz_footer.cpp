/* See fuzz_manifest_edit.cpp for what these targets assert and why.
 *
 * Both halves, in the order a reader uses them: the trailer says how wide the footer is, and that
 * width then indexes into the same bytes. A width the input controls feeding an offset into a
 * buffer the input sized is the shape worth exploring here. */
#include "sst/footer.hpp"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    const elysiumkv::Slice bytes(data, size);
    (void)elysiumkv::Footer::footer_length_from_trailer(bytes);
    (void)elysiumkv::Footer::decode(bytes);
    return 0;
}
