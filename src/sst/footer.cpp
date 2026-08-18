#include "sst/footer.hpp"

#include "sst/crc32c.hpp"
#include "sst/format.hpp"

namespace elysiumkv {

std::string Footer::encode() const {
    std::string out;
    out.reserve(static_cast<size_t>(kFooterLengthV2));
    put_fixed64(out, filter.offset);
    put_fixed32(out, filter.length);
    put_fixed64(out, index.offset);
    put_fixed32(out, index.length);
    put_fixed64(out, num_entries);
    // The new field goes *before* the invariant trailer, which is what keeps the trailer invariant:
    // a reader still finds `format_version` and the magic at a fixed distance from the end.
    if (format_version >= kFormatVersion2) {
        put_fixed64(out, range_del.offset);
        put_fixed32(out, range_del.length);
    }
    // Over everything written so far, which is the whole footer body. The trailer that follows is
    // not covered and does not need to be: the magic is checked before the version is trusted.
    if (format_version >= kFormatVersion3) put_fixed32(out, crc32c(out));
    put_fixed32(out, format_version);
    put_fixed64(out, kMagic);
    return out;
}

Result<int> Footer::footer_length_from_trailer(Slice trailer) {
    if (trailer.size() < static_cast<size_t>(kTrailerLength)) return std::unexpected(Status::Corrupt);

    const uint8_t* p = trailer.data() + trailer.size() - kTrailerLength;
    const uint32_t version = decode_fixed32(p);
    const uint64_t magic = decode_fixed64(p + 4);
    if (magic != kMagic) return std::unexpected(Status::Corrupt);
    if (version == kFormatVersion1) return kFooterLengthV1;
    if (version == kFormatVersion2) return kFooterLengthV2;
    if (version == kFormatVersion3) return kFooterLengthV3;
    return std::unexpected(Status::Unsupported);
}

Result<Footer> Footer::decode(Slice bytes) {
    if (bytes.size() < static_cast<size_t>(kTrailerLength)) return std::unexpected(Status::Corrupt);

    // The version is read from the invariant trailer first, and the rest of the footer is located
    // relative to it — never the other way round. See the note on the trailer in the header.
    const uint8_t* trailer = bytes.data() + bytes.size() - kTrailerLength;
    const uint32_t version = decode_fixed32(trailer);
    const uint64_t magic = decode_fixed64(trailer + 4);
    if (magic != kMagic) return std::unexpected(Status::Corrupt);

    const int width = version == kFormatVersion1   ? kFooterLengthV1
                      : version == kFormatVersion2 ? kFooterLengthV2
                      : version == kFormatVersion3 ? kFooterLengthV3
                                                   : 0;
    if (width == 0) return std::unexpected(Status::Unsupported);
    if (bytes.size() < static_cast<size_t>(width)) return std::unexpected(Status::Corrupt);

    const uint8_t* p = bytes.data() + bytes.size() - width;
    Footer footer;
    footer.filter.offset = decode_fixed64(p);
    footer.filter.length = decode_fixed32(p + 8);
    footer.index.offset = decode_fixed64(p + 12);
    footer.index.length = decode_fixed32(p + 20);
    footer.num_entries = decode_fixed64(p + 24);
    footer.format_version = version;
    if (version >= kFormatVersion2) {
        footer.range_del.offset = decode_fixed64(p + 32);
        footer.range_del.length = decode_fixed32(p + 40);
    }
    if (version >= kFormatVersion3) {
        // **Checked before any handle above is used.** The point is to fail here rather than at a
        // block read that a damaged offset sent somewhere arbitrary.
        const size_t body = static_cast<size_t>(width - kTrailerLength - 4);
        footer.crc = decode_fixed32(p + body);
        if (crc32c(p, body) != footer.crc) return std::unexpected(Status::Corrupt);
    }
    return footer;
}

}  // namespace elysiumkv
