#ifndef ELYSIUMKV_MANIFEST_CATALOG_HPP
#define ELYSIUMKV_MANIFEST_CATALOG_HPP

#include "elysiumkv/blob_store.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <cstdint>
#include <future>
#include <optional>
#include <string>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Ownership is one compare-and-set" — the one pluggable metadata seam. All manifest state lives here:
/// the generation objects and the pointer naming which generation is live. The
/// two are one concern — moving the pointer is only meaningful against objects
/// the same component wrote.
///
/// Bytes are opaque. Record encoding, CRC framing, replay, gap detection,
/// rolling policy and the GC ordering rule all stay in the engine. An
/// implementation stores bytes at an address and swaps a pointer; it can get
/// storage and CAS wrong and nothing else.
///
/// Objects are immutable and write-once. A put at an existing address is a
/// programming error, not an overwrite. A successful object write, pointer CAS or deletion is
/// durable across power loss; the engine may immediately delete data made obsolete by it.
class ManifestCatalog {
public:
    struct Entry {
        uint64_t generation = 0;
        std::string token;  ///< opaque fencing handle
    };

    // --- pointer
    virtual Result<std::optional<Entry>> read() = 0;

    /// Atomically install a new generation. Returns nullopt if `expected` no
    /// longer matches — another writer installed first and this process has been
    /// fenced. The caller must not retry blindly: a lost CAS means its Version
    /// is stale, so the engine reports Status::Fenced and requires a reopen.
    virtual Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                         uint64_t generation) = 0;

    // --- generation objects (opaque bytes)
    virtual std::future<Status> put_snapshot(uint64_t generation, Slice bytes) = 0;
    virtual std::future<GetResult> get_snapshot(uint64_t generation) = 0;
    virtual std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) = 0;
    virtual std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) = 0;
    virtual std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) = 0;
    virtual std::future<Status> delete_generation(uint64_t generation) = 0;

    /// Every generation this catalog holds, in any order.
    ///
    /// Optional. The default reports `Status::Unsupported`, and the engine falls back to
    /// probing a bounded window around the live pointer. A catalog that can enumerate cheaply
    /// should, because that is what makes reclaim complete rather than limited to nearby residue.
    ///
    /// Not a pure virtual, deliberately: an embedder's own catalog compiled against an earlier
    /// header keeps working, and leaks at worst the generations the fallback already covers.
    virtual std::future<Result<std::vector<uint64_t>>> list_generations() {
        return make_ready_future(
            Result<std::vector<uint64_t>>(std::unexpected(Status::Unsupported)));
    }

    virtual ~ManifestCatalog() = default;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MANIFEST_CATALOG_HPP
