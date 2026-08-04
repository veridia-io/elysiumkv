#include "sst/compression.hpp"

#include "sst/crc32c.hpp"
#include "sst/format.hpp"

#include <lz4.h>
#include <zstd.h>

#include <limits>

namespace elysiumkv {
namespace {

Status compress_lz4(Slice content, std::string& out) {
    if (content.size() > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) return Status::Io;
    const int bound = LZ4_compressBound(static_cast<int>(content.size()));
    if (bound <= 0) return Status::Io;

    const size_t start = out.size();
    out.resize(start + static_cast<size_t>(bound));
    const int written = LZ4_compress_default(reinterpret_cast<const char*>(content.data()),
                                             out.data() + start, static_cast<int>(content.size()),
                                             bound);
    if (written <= 0) {
        out.resize(start);
        return Status::Io;
    }
    out.resize(start + static_cast<size_t>(written));
    return Status::Ok;
}

Status compress_zstd(Slice content, std::string& out) {
    const size_t bound = ZSTD_compressBound(content.size());
    const size_t start = out.size();
    out.resize(start + bound);
    const size_t written =
        ZSTD_compress(out.data() + start, bound, content.data(), content.size(), 3);
    if (ZSTD_isError(written) != 0) {
        out.resize(start);
        return Status::Io;
    }
    out.resize(start + written);
    return Status::Ok;
}

}  // namespace

Status frame_block(Slice content, Compression codec, std::string& out) {
    if (content.size() > std::numeric_limits<uint32_t>::max()) return Status::Io;

    const size_t payload_start = out.size();
    Compression stored = codec;

    switch (codec) {
        case Compression::None:
            out.append(content.as_string_view());
            break;
        case Compression::Lz4:
            if (compress_lz4(content, out) != Status::Ok) return Status::Io;
            break;
        case Compression::Zstd:
            if (compress_zstd(content, out) != Status::Ok) return Status::Io;
            break;
    }

    // A block the codec failed to shrink is stored raw. Nothing downstream
    // depends on a file's blocks sharing a codec.
    if (stored != Compression::None && out.size() - payload_start >= content.size()) {
        out.resize(payload_start);
        out.append(content.as_string_view());
        stored = Compression::None;
    }

    put_fixed32(out, static_cast<uint32_t>(content.size()));
    out.push_back(static_cast<char>(static_cast<uint8_t>(stored)));

    const auto* framed = reinterpret_cast<const uint8_t*>(out.data()) + payload_start;
    const size_t framed_len = out.size() - payload_start;  // payload + len + type
    put_fixed32(out, crc32c(framed, framed_len));
    return Status::Ok;
}

Result<Buffer> unframe_block(Slice raw, size_t max_uncompressed) {
    if (raw.size() < kBlockTrailerLength) return std::unexpected(Status::Corrupt);

    const size_t covered = raw.size() - 4;  // payload ‖ uncompressed_len ‖ compression_type
    const uint32_t stored_crc = decode_fixed32(raw.data() + covered);
    if (crc32c(raw.data(), covered) != stored_crc) return std::unexpected(Status::Corrupt);

    const uint8_t codec_byte = raw.data()[covered - 1];
    if (!is_known_compression(codec_byte)) return std::unexpected(Status::Corrupt);
    const auto codec = static_cast<Compression>(codec_byte);
    const uint32_t uncompressed_len = decode_fixed32(raw.data() + covered - 5);

    // The CRC normally catches a corrupted length first; this bound is the
    // backstop, and it runs before any allocation (ARCHITECTURE.md "Inside an SST").
    if (uncompressed_len > max_uncompressed) return std::unexpected(Status::Corrupt);

    const size_t payload_len = raw.size() - kBlockTrailerLength;
    const uint8_t* payload = raw.data();

    if (codec == Compression::None) {
        if (payload_len != uncompressed_len) return std::unexpected(Status::Corrupt);
        return Buffer(payload, payload + payload_len);
    }

    Buffer out(uncompressed_len);
    if (codec == Compression::Lz4) {
        if (payload_len > static_cast<size_t>(std::numeric_limits<int>::max())) {
            return std::unexpected(Status::Corrupt);
        }
        const int produced = LZ4_decompress_safe(
            reinterpret_cast<const char*>(payload), reinterpret_cast<char*>(out.data()),
            static_cast<int>(payload_len), static_cast<int>(uncompressed_len));
        if (produced < 0 || static_cast<uint32_t>(produced) != uncompressed_len) {
            return std::unexpected(Status::Corrupt);
        }
    } else {
        const size_t produced = ZSTD_decompress(out.data(), uncompressed_len, payload, payload_len);
        if (ZSTD_isError(produced) != 0 || produced != uncompressed_len) {
            return std::unexpected(Status::Corrupt);
        }
    }
    return out;
}

}  // namespace elysiumkv
