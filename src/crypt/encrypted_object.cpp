#include "crypt/encrypted_object.hpp"

#include <algorithm>
#include <utility>

namespace elysiumkv {
namespace {

void put_le64(std::string& out, uint64_t value) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>(value >> (8 * i)));
}

/// Chunks needed to hold `logical_bytes`. Zero for an empty object: an object with no bytes has no
/// chunks, and therefore occupies nothing physically.
uint64_t chunk_count(uint64_t logical_bytes, size_t chunk_bytes) {
    return (logical_bytes + chunk_bytes - 1) / chunk_bytes;
}

/// The plaintext length of chunk `k`, which is short for the last one.
uint64_t chunk_plain_bytes(uint64_t k, uint64_t logical_bytes, size_t chunk_bytes) {
    const uint64_t start = k * chunk_bytes;
    if (start >= logical_bytes) return 0;
    return std::min<uint64_t>(chunk_bytes, logical_bytes - start);
}

}  // namespace

std::string chunk_aad(uint64_t object_id, uint64_t chunk, uint64_t logical_bytes) {
    std::string aad;
    aad.reserve(24);
    put_le64(aad, object_id);
    put_le64(aad, chunk);
    // Only chunk zero. Binding it to every chunk would be no stronger — an attacker who can change
    // the claimed length can only present chunk zero as chunk zero — and would mean the length had
    // to be known before any chunk could be opened, including by a reader seeking to the end.
    if (chunk == 0) put_le64(aad, logical_bytes);
    return aad;
}

EncryptedObject::EncryptedObject(BlobStore& delegate, std::shared_ptr<ObjectCipher> cipher,
                                 std::string object_name, uint64_t logical_bytes)
    : delegate_(delegate),
      cipher_(std::move(cipher)),
      object_name_(std::move(object_name)),
      object_id_(cipher_->object_id()),
      logical_bytes_(logical_bytes),
      chunk_bytes_(cipher_->chunk_bytes()),
      overhead_bytes_(cipher_->overhead_bytes()) {}

uint64_t EncryptedObject::physical_size(uint64_t logical_bytes, size_t chunk_bytes,
                                        size_t overhead_bytes) {
    return logical_bytes + chunk_count(logical_bytes, chunk_bytes) * overhead_bytes;
}

Result<std::string> EncryptedObject::seal_object(ObjectCipher& cipher, Slice plaintext) {
    const size_t chunk_bytes = cipher.chunk_bytes();
    const uint64_t logical = plaintext.size();
    const uint64_t chunks = chunk_count(logical, chunk_bytes);

    std::string sealed;
    sealed.reserve(static_cast<size_t>(
        physical_size(logical, chunk_bytes, cipher.overhead_bytes())));

    for (uint64_t k = 0; k < chunks; ++k) {
        const uint64_t start = k * chunk_bytes;
        const uint64_t length = chunk_plain_bytes(k, logical, chunk_bytes);
        // The cipher's recorded identity, not whatever number the caller has in hand.
        const std::string aad = chunk_aad(cipher.object_id(), k, logical);
        const Status status = cipher.seal(k, Slice(plaintext.data() + start,
                                                   static_cast<size_t>(length)),
                                          Slice::from(aad), sealed);
        if (status != Status::Ok) return std::unexpected(status);
    }
    return sealed;
}

std::future<GetResult> EncryptedObject::get(std::string_view name, uint64_t offset, size_t len) {
    return make_ready_future(read(name, offset, len));
}

GetResult EncryptedObject::get_sync(std::string_view name, uint64_t offset, size_t len) {
    return read(name, offset, len);
}

GetResult EncryptedObject::read(std::string_view name, uint64_t offset, size_t len) {
    // One view serves one object. Anything else is a wiring error rather than a read that could
    // succeed, and it must not be answered from the wrong key.
    if (name != object_name_) return std::unexpected(Status::Config);

    // Past the end is an empty answer, not an error — the contract every store here keeps.
    if (offset >= logical_bytes_) return Buffer{};

    const uint64_t available = logical_bytes_ - offset;
    const uint64_t want = (len == kReadToEnd || len > available) ? available : len;
    if (want == 0) return Buffer{};

    const uint64_t first = offset / chunk_bytes_;
    const uint64_t last = (offset + want - 1) / chunk_bytes_;
    const uint64_t stride = chunk_bytes_ + overhead_bytes_;

    const uint64_t physical_begin = first * stride;
    const uint64_t physical_end =
        last * stride + chunk_plain_bytes(last, logical_bytes_, chunk_bytes_) + overhead_bytes_;

    auto fetched = delegate_.get_sync(object_name_, physical_begin,
                                      static_cast<size_t>(physical_end - physical_begin));
    if (!fetched) return std::unexpected(fetched.error());
    // The exact length is computable, so a short read is damage rather than a legitimately short
    // object: the logical length said these bytes exist.
    if (fetched->size() != physical_end - physical_begin) return std::unexpected(Status::Corrupt);

    std::string plaintext;
    plaintext.reserve(static_cast<size_t>((last - first + 1) * chunk_bytes_));
    for (uint64_t k = first; k <= last; ++k) {
        const uint64_t at = (k - first) * stride;
        const uint64_t sealed_len =
            chunk_plain_bytes(k, logical_bytes_, chunk_bytes_) + overhead_bytes_;
        const std::string aad = chunk_aad(object_id_, k, logical_bytes_);
        const Status status = cipher_->open(k, Slice(fetched->data() + at,
                                                     static_cast<size_t>(sealed_len)),
                                            Slice::from(aad), plaintext);
        if (status != Status::Ok) return std::unexpected(status);
    }

    const size_t skip = static_cast<size_t>(offset - first * chunk_bytes_);
    const auto* begin = reinterpret_cast<const uint8_t*>(plaintext.data()) + skip;
    return Buffer(begin, begin + want);
}

std::future<Status> EncryptedObject::put(std::string_view name, Slice bytes) {
    if (name != object_name_) return make_ready_future(Status::Config);
    auto sealed = seal_object(*cipher_, bytes);
    if (!sealed) return make_ready_future(sealed.error());
    return delegate_.put(name, Slice::from(*sealed));
}

std::future<Status> EncryptedObject::remove(std::string_view name) {
    return delegate_.remove(name);
}

std::future<ListResult> EncryptedObject::list(std::string_view prefix) {
    return delegate_.list(prefix);
}

}  // namespace elysiumkv
