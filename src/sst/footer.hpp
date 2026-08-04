#ifndef ELYSIUMKV_SST_FOOTER_HPP
#define ELYSIUMKV_SST_FOOTER_HPP

#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <cstdint>
#include <string>

namespace elysiumkv {

/// Offset and full framed length of a block — payload plus the 9-byte trailer,
/// so one range read fetches everything needed to validate and decode it.
struct BlockHandle {
    uint64_t offset = 0;
    uint32_t length = 0;
};

/// ARCHITECTURE.md "The invariant trailer". The last 12 bytes are an **invariant trailer** and must stay fixed
/// across every future format version: the reader seeks to
/// `file_len - kTrailerLength`, validates the magic, then dispatches on
/// `format_version` to learn the full footer width. Without this, footer width
/// would depend on a version stored inside the footer, and any format change
/// would make existing files unparseable.
struct Footer {
    static constexpr int kTrailerLength = 12;    // never changes
    static constexpr int kFooterLengthV1 = 44;   // version-scoped
    static constexpr uint32_t kFormatVersion1 = 1;
    /// Spells "ELYSIUM1" in ASCII.
    ///
    /// **Frozen.** The magic sits in the invariant trailer above, so a reader
    /// checks it before it knows anything else about the file; changing it again would make every
    /// existing file fail that check and be reported as `Corrupt`. Doing so later means teaching
    /// the reader to accept both values — a format change with a migration, not a rename.
    static constexpr uint64_t kMagic = 0x454C595349554D31ull;

    BlockHandle filter;
    BlockHandle index;
    uint64_t num_entries = 0;
    uint32_t format_version = kFormatVersion1;

    /// Exactly kFooterLengthV1 bytes.
    std::string encode() const;

    /// `trailer` is the last kTrailerLength bytes of the file. Reports the
    /// footer width for that version, or Corrupt for a bad magic or a version
    /// this build does not know.
    static Result<int> footer_length_from_trailer(Slice trailer);

    /// `bytes` is the last `footer_length_from_trailer(...)` bytes of the file.
    static Result<Footer> decode(Slice bytes);
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_FOOTER_HPP
