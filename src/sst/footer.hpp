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
    /// v2 appends one block handle: the range tombstones. 44 + 12.
    static constexpr int kFooterLengthV2 = 56;
    /// v3 appends a CRC over everything before it. 56 + 4.
    static constexpr int kFooterLengthV3 = 60;
    /// The widest footer this build knows, and therefore how many bytes a reader must have in hand
    /// before it can decode one. **Named rather than spelled as the current newest version**: it
    /// was written as `kFooterLengthV2` in two places in the reader, so adding v3 silently handed
    /// `decode` four bytes too few and every read came back `Corrupt`.
    static constexpr int kMaxFooterLength = kFooterLengthV3;
    static constexpr uint32_t kFormatVersion1 = 1;
    static constexpr uint32_t kFormatVersion2 = 2;
    static constexpr uint32_t kFormatVersion3 = 3;
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

    /// The range tombstones, or a zero handle when the file carries none.
    ///
    /// **A file with no range tombstones is still written as v1**, which is the whole reason the
    /// version is per file rather than per build. Only a file that actually needs the new block
    /// becomes unreadable to a reader that predates it, and a reader that cannot honour a range
    /// tombstone must refuse the file rather than return the keys it covers.
    BlockHandle range_del;

    /// **The only structure in the format that used to validate nothing.** Every block carries a
    /// CRC, so damage inside one is caught where it happens; damage in the footer instead produced
    /// plausible handles, and the read failed later at a block that was never the problem. Covers
    /// the footer bytes before it — the trailer that follows is self-validating, since the magic is
    /// checked before anything else and the version against a known set.
    ///
    /// **Written on every v3 file**, unlike `range_del`, whose presence is what a file may or may
    /// not need. A checksum every file lacks is a checksum.
    uint32_t crc = 0;

    /// kFooterLengthV1 or kFooterLengthV2 bytes, depending on `format_version`.
    std::string encode() const;

    /// `trailer` is the last kTrailerLength bytes of the file. Reports the footer width for that
    /// version, `Corrupt` for a bad magic, or **`Unsupported` for a version this build does not
    /// know** — the magic is eight bytes, so garbage does not reach that branch, and telling an
    /// operator their intact bytes are damaged sends them to a restore they do not need.
    static Result<int> footer_length_from_trailer(Slice trailer);

    /// `bytes` is the last `footer_length_from_trailer(...)` bytes of the file.
    static Result<Footer> decode(Slice bytes);
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_FOOTER_HPP
