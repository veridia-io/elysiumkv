#ifndef ELYSIUMKV_VERSION_MANIFEST_PAYLOAD_HPP
#define ELYSIUMKV_VERSION_MANIFEST_PAYLOAD_HPP

#include "crypt/provider_registry.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <cstdint>
#include <string>
#include <string_view>

namespace elysiumkv {

/// The framing every manifest payload gets on its way to the catalog: compress, then encrypt,
/// then hand to the catalog to chunk.
///
/// That order is not a preference. Ciphertext does not compress, so encrypting first would inflate
/// every manifest write — and chunking is a transport concern belonging to whichever catalog has a
/// size cap, which is why it is outermost and not here. Compressing first leaks a little about
/// content through length, which is accepted for a list of file metadata.
///
/// This lives in the engine, not in a catalog. `ManifestCatalog` promises that bytes are
/// opaque; a catalog that encrypted would be a catalog that had to be trusted, and every embedder's
/// own would have to implement this correctly too. Unlike an SST there is no ranged read to
/// preserve, so the whole payload is sealed and opened as one.
///
/// Header, little-endian, 32 bytes:
///
/// ```
/// 0   magic          uint32   "EKV\x02"
/// 4   header_version uint16
/// 6   provider_len   uint16
/// 8   metadata_len   uint32
/// 12  codec          uint32   0 none, 1 zstd
/// 16  plain_len      uint64   before compression
/// 24  packed_len     uint64   after compression; what was sealed
/// 32  provider ‖ metadata ‖ ciphertext
/// ```
///
/// `header_version` describes this framing only. Everything about the construction is inside
/// `metadata`, which the engine does not parse — layout and algorithm version independently.
struct ManifestPayload {
    static constexpr size_t kHeaderBytes = 32;

    /// A payload's manifest address, bound into its authentication so one cannot be replayed at
    /// another. Fixed-width, so a generation containing the separator cannot make one ambiguous.
    /// Shared rather than derived twice: an address the reader spells differently authenticates
    /// nothing, and the failure would look like corruption.
    static std::string snapshot_address(uint64_t generation);
    static std::string edit_address(uint64_t generation, uint64_t seq);

    /// Seals under the registry's primary provider. `address` comes from the two above and is
    /// bound into every chunk's authentication, so a payload lifted to another address is refused
    /// rather than opened.
    static Result<std::string> seal(const ProviderRegistry& registry, uint64_t generation,
                                    std::string_view address, Slice plaintext);

    /// The inverse, routed by the recorded provider id.
    ///
    /// The three failures are deliberately different statuses, because replay treats them
    /// differently: `Config` for a provider this process has not registered, which is an operator's
    /// to fix; `Corrupt` for framing that is not ours, a truncated payload, or authentication that
    /// does not hold.
    static Result<std::string> open(const ProviderRegistry& registry, uint64_t generation,
                                    std::string_view address, Slice framed, std::string& error);
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_MANIFEST_PAYLOAD_HPP
