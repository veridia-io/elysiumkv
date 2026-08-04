#ifndef ELYSIUMKV_SST_COMPRESSION_HPP
#define ELYSIUMKV_SST_COMPRESSION_HPP

#include "elysiumkv/options.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "Inside an SST" block framing: appends `payload ‖ uncompressed_len ‖ compression_type ‖
/// crc32c` to `out`. The CRC covers the *stored* bytes, so corruption is caught
/// before anything reaches the decompressor.
///
/// A codec that fails to shrink the block falls back to `None` — the per-block
/// type byte means the reader neither knows nor cares.
Status frame_block(Slice content, Compression codec, std::string& out);

/// Validates the CRC, bounds `uncompressed_len`, and only then decompresses.
/// `max_uncompressed` is `max(16 * block_bytes, 1 MiB)` (ARCHITECTURE.md "Inside an SST"): a corrupted
/// length field must not request an arbitrary allocation. The bound is a
/// backstop behind the CRC and is not optional.
Result<Buffer> unframe_block(Slice raw, size_t max_uncompressed);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_COMPRESSION_HPP
