#ifndef ELYSIUMKV_ENCRYPTION_HPP
#define ELYSIUMKV_ENCRYPTION_HPP

#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace elysiumkv {

/// Key material, and the handling rules that come with it.
///
/// **Move-only, and zeroed on destruction.** A value that copies key material freely leaves plaintext
/// keys scattered through the heap for the allocator to hand out later. `std::string` is the wrong
/// container for the same reason: its reallocation leaves the old buffer behind untouched.
///
/// Never logged, never in a `Status` message, never in `Stats`.
class SecretKey {
public:
    SecretKey() = default;
    SecretKey(const uint8_t* data, size_t size) : bytes_(data, data + size) {}

    SecretKey(SecretKey&&) noexcept = default;
    SecretKey& operator=(SecretKey&&) noexcept = default;
    SecretKey(const SecretKey&) = delete;
    SecretKey& operator=(const SecretKey&) = delete;

    ~SecretKey();

    const uint8_t* data() const { return bytes_.data(); }
    size_t size() const { return bytes_.size(); }
    bool empty() const { return bytes_.empty(); }

private:
    std::vector<uint8_t> bytes_;
};

/// A fresh data key: the plaintext to encrypt with, and the wrapped form to persist.
struct DataKey {
    SecretKey key;
    /// Safe to write down. Opaque to the engine — only the manager that produced it can open it.
    std::string envelope;
};

/// Wrapping and unwrapping, and nothing else.
///
/// **The engine owns the cryptography; the embedder owns the key custody.** That split is what keeps
/// a KMS integration from becoming a cipher integration, and it is the seam almost every embedder
/// needs — supplying a whole `EncryptionProvider` is for an organisation that must use a specific
/// construction.
class EncryptionKeyManager {
public:
    virtual Result<DataKey> new_data_key() = 0;
    virtual Result<SecretKey> open_data_key(Slice envelope) = 0;
    virtual ~EncryptionKeyManager() = default;
};

/// The construction one object is encrypted under.
///
/// **Chunked, with a fixed overhead per chunk.** The engine owns the logical-to-physical offset
/// mapping, because that invariant has to live in exactly one place — a cipher free to choose an
/// arbitrary layout would have to reimplement it and would eventually get it wrong somewhere the
/// engine could not check. A chunk size and a per-chunk overhead express every construction worth
/// using here: zero overhead is length-preserving, sixteen is a GCM tag.
///
/// **Both must be constant for the life of the object.** They are read once and cached; a cipher
/// that varies them per call corrupts reads in a way nothing can detect.
class ObjectCipher {
public:
    virtual size_t chunk_bytes() const = 0;
    virtual size_t overhead_bytes() const = 0;

    /// The identity the object's chunks are authenticated against.
    ///
    /// **Recorded at creation, not the file's current number.** Migration copies an object between
    /// tiers byte for byte and gives the copy a new file number; if the authentication were bound to
    /// the live number, every migrated file would stop opening. So the id is fixed when the object
    /// is first written and travels with it.
    virtual uint64_t object_id() const = 0;

    /// Encrypts one chunk. `out` is appended to, never cleared.
    virtual Status seal(uint64_t chunk, Slice plaintext, Slice aad, std::string& out) = 0;
    /// The inverse. A failure here is `Status::Corrupt`: the bytes are not what was written.
    virtual Status open(uint64_t chunk, Slice ciphertext, Slice aad, std::string& out) = 0;

    virtual ~ObjectCipher() = default;
};

/// What `EncryptionProvider::create` hands back.
struct NewObject {
    std::shared_ptr<ObjectCipher> cipher;
    /// Persisted verbatim beside the object and handed back to `open`. Opaque to the engine.
    std::string metadata;
};

/// One construction: a suite, a key policy, and whatever metadata it needs to reopen an object.
///
/// **A provider does not know its own name.** Its id is the key it is registered under in
/// `EncryptionOptions::providers`, so that value exists in one place and a self-reported id can
/// never disagree with the one objects were recorded against.
class EncryptionProvider {
public:
    /// Begin a new object. `object_id` is its file number, which is unique and never reused — so it
    /// is sound to derive a nonce basis from it.
    virtual Result<NewObject> create(uint64_t object_id) = 0;

    /// Reconstruct the cipher for an existing object from what `create` recorded.
    virtual Result<std::shared_ptr<ObjectCipher>> open(uint64_t object_id, Slice metadata) = 0;

    virtual ~EncryptionProvider() = default;
};

/// Overwrites `size` bytes at `data` with zeroes, in a way the optimiser may not remove.
///
/// **For buffers key material passed through**, which is a category the compiler cannot see: a
/// plain loop before a buffer dies is a dead store and is deleted.
void secure_zero(void* data, size_t size);

/// The identifier reserved for the built-in passthrough.
///
/// **Empty, deliberately, and load-bearing.** A file written with encryption disabled and one
/// written before encryption existed record the same thing, so there is one case rather than two.
/// It is also how the engine knows an object needs no cipher — the id says so, and the id is the
/// only thing that has to say so. There is no predicate on the provider asking the same question a
/// second way, because two sources for one fact is how they come to disagree.
///
/// An embedder registering this id is refused at open.
inline constexpr std::string_view kNoEncryptionProviderId = "";

}  // namespace elysiumkv

#endif  // ELYSIUMKV_ENCRYPTION_HPP
