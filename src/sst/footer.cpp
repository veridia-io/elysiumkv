#include "sst/footer.hpp"

#include "sst/format.hpp"

namespace elysiumkv {

std::string Footer::encode() const {
    std::string out;
    out.reserve(static_cast<size_t>(kFooterLengthV1));
    put_fixed64(out, filter.offset);
    put_fixed32(out, filter.length);
    put_fixed64(out, index.offset);
    put_fixed32(out, index.length);
    put_fixed64(out, num_entries);
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
    if (version != kFormatVersion1) return std::unexpected(Status::Corrupt);
    return kFooterLengthV1;
}

Result<Footer> Footer::decode(Slice bytes) {
    if (bytes.size() < static_cast<size_t>(kFooterLengthV1)) return std::unexpected(Status::Corrupt);

    const uint8_t* p = bytes.data() + bytes.size() - kFooterLengthV1;
    Footer footer;
    footer.filter.offset = decode_fixed64(p);
    footer.filter.length = decode_fixed32(p + 8);
    footer.index.offset = decode_fixed64(p + 12);
    footer.index.length = decode_fixed32(p + 20);
    footer.num_entries = decode_fixed64(p + 24);
    footer.format_version = decode_fixed32(p + 32);
    const uint64_t magic = decode_fixed64(p + 36);

    if (magic != kMagic || footer.format_version != kFormatVersion1) {
        return std::unexpected(Status::Corrupt);
    }
    return footer;
}

}  // namespace elysiumkv
