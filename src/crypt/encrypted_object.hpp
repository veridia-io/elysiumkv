#ifndef ELYSIUMKV_CRYPT_ENCRYPTED_OBJECT_HPP
#define ELYSIUMKV_CRYPT_ENCRYPTED_OBJECT_HPP

#include "elysiumkv/blob_store.hpp"
#include "elysiumkv/encryption.hpp"

#include <memory>
#include <string>

namespace elysiumkv {

/// One encrypted object, presented as a `BlobStore` so that everything above it is unchanged
/// (ARCHITECTURE.md "Encryption sits at the object boundary").
///
/// The engine's half of the boundary, not the provider's. A provider owns cryptography — keys,
/// nonces, suites, its own metadata. This owns the mapping between the offsets the engine reads at
/// and the offsets the bytes actually live at, and that has to exist in exactly one place: a
/// provider free to choose its own layout would reimplement this arithmetic, and an error in it is
/// invisible to everything else. `ObjectCipher` reports a chunk size and a per-chunk overhead, and
/// those two numbers are the whole contract needed here.
///
/// Constructed per object, at the point of use — never registered in a tier's store chain. So
/// it cannot be composed in the wrong order, and it does not terminate `authoritative_store()`'s
/// walk the way a chain decorator would.
///
/// Above it: logical offsets and plaintext. Below it: physical offsets and ciphertext, which is
/// what the caches and the compaction window then hold.
///
/// ```
///  logical    [ chunk 0 (C) ][ chunk 1 (C) ][ chunk 2 (<=C) ]
///  physical   [ chunk 0 (C) ][T][ chunk 1 (C) ][T][ chunk 2 ][T]
/// ```
class EncryptedObject final : public BlobStore {
public:
    /// The identity the chunks authenticate against is the cipher's, not the file's current
    /// number. Migration copies an object byte for byte and renumbers the copy, so the two differ
    /// there — and binding to the live number would stop every migrated file from opening.
    ///
    /// `logical_bytes` is the object's plaintext length — `FileMetadata::file_bytes` for an SST.
    /// It is needed to locate the final, possibly short, chunk, and it is bound into chunk 0's
    /// authentication so that a lie about an object's length cannot be believed.
    EncryptedObject(BlobStore& delegate, std::shared_ptr<ObjectCipher> cipher,
                    std::string object_name, uint64_t logical_bytes);

    /// The physical size an object of `logical_bytes` occupies under this cipher.
    static uint64_t physical_size(uint64_t logical_bytes, size_t chunk_bytes,
                                  size_t overhead_bytes);

    /// Seals a whole object. Used by the write path, which never writes part of one.
    static Result<std::string> seal_object(ObjectCipher& cipher, Slice plaintext);

    std::string id() const override { return delegate_.id(); }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    GetResult get_sync(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    std::future<ListResult> list(std::string_view prefix) override;

private:
    GetResult read(std::string_view name, uint64_t offset, size_t len);

    BlobStore& delegate_;
    std::shared_ptr<ObjectCipher> cipher_;
    std::string object_name_;
    uint64_t object_id_;
    uint64_t logical_bytes_;
    size_t chunk_bytes_;
    size_t overhead_bytes_;
};

/// The associated data a chunk is authenticated against.
///
/// Binds the chunk to its position and its object, so a chunk lifted from elsewhere — or
/// replayed at a different index — fails to open rather than decrypting to something plausible.
/// Chunk zero additionally binds the object's logical length, which is what closes truncation:
/// a shortened object is a claim about its length, and a claim that does not match what was sealed
/// is refused.
std::string chunk_aad(uint64_t object_id, uint64_t chunk, uint64_t logical_bytes);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_CRYPT_ENCRYPTED_OBJECT_HPP
