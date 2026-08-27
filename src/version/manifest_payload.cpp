#include "version/manifest_payload.hpp"

#include "sst/format.hpp"

#include <zstd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

namespace elysiumkv {
namespace {

constexpr uint32_t kMagic = 0x0256'4B45;  // "EKV\x02", little-endian
constexpr uint16_t kHeaderVersion = 1;
constexpr uint32_t kCodecNone = 0;
constexpr uint32_t kCodecZstd = 1;
constexpr int kCompressionLevel = 3;

/// A manifest snapshot for a mature store is a few hundred KB. The bound is a backstop against a
/// header that claims an absurd length, and nothing more — the authentication below is what
/// actually establishes the length is the one that was sealed.
constexpr uint64_t kMaxPayloadBytes = 256ull << 20;

uint64_t chunk_count(uint64_t logical, size_t chunk_bytes) {
    if (logical == 0) return 0;
    return (logical + chunk_bytes - 1) / chunk_bytes;
}

uint64_t chunk_plain_bytes(uint64_t chunk, uint64_t logical, size_t chunk_bytes) {
    const uint64_t start = chunk * chunk_bytes;
    const uint64_t remaining = logical - start;
    return remaining < chunk_bytes ? remaining : chunk_bytes;
}

/// The address is bound into every chunk, not only chunk zero. A manifest payload has a
/// meaning that depends entirely on where it sits — an edit replayed at another sequence number
/// would apply the wrong change to the wrong generation — and unlike an SST there is no file
/// number inside the bytes to catch it.
///
/// Chunk zero additionally binds the whole header, which is where the lengths, the codec, the
/// provider id and the provider's own metadata live. Binding only the lengths would leave the rest
/// of the header malleable — inert as far as decryption goes, but a header nobody authenticates is
/// a header that becomes load-bearing later without anyone noticing.
std::string payload_aad(uint64_t chunk, std::string_view address, std::string_view header) {
    std::string aad;
    aad.reserve(8 + address.size() + (chunk == 0 ? header.size() : 0));
    put_fixed64(aad, chunk);
    aad.append(address);
    if (chunk == 0) aad.append(header);
    return aad;
}

Status compress(Slice plaintext, uint32_t& codec, std::string& out) {
    const size_t bound = ZSTD_compressBound(plaintext.size());
    out.resize(bound);
    const size_t written =
        ZSTD_compress(out.data(), bound, plaintext.data(), plaintext.size(), kCompressionLevel);
    if (ZSTD_isError(written) != 0) return Status::Io;
    // A payload that does not shrink is stored as it is. The codec field means the reader neither
    // knows nor cares which happened, the same trade `frame_block` makes per block.
    if (written >= plaintext.size()) {
        codec = kCodecNone;
        out.assign(reinterpret_cast<const char*>(plaintext.data()), plaintext.size());
        return Status::Ok;
    }
    codec = kCodecZstd;
    out.resize(written);
    return Status::Ok;
}

Result<std::string> decompress(Slice packed, uint32_t codec, uint64_t plain_len) {
    if (codec == kCodecNone) {
        if (packed.size() != plain_len) return std::unexpected(Status::Corrupt);
        return std::string(reinterpret_cast<const char*>(packed.data()), packed.size());
    }
    if (codec != kCodecZstd) return std::unexpected(Status::Corrupt);

    std::string out(static_cast<size_t>(plain_len), '\0');
    if (plain_len > 0) {
        const size_t produced =
            ZSTD_decompress(out.data(), out.size(), packed.data(), packed.size());
        if (ZSTD_isError(produced) != 0 || produced != plain_len) {
            return std::unexpected(Status::Corrupt);
        }
    }
    return out;
}

}  // namespace

std::string ManifestPayload::snapshot_address(uint64_t generation) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "snap#%012llu", static_cast<unsigned long long>(generation));
    return buf;
}

std::string ManifestPayload::edit_address(uint64_t generation, uint64_t seq) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "edit#%012llu#%012llu",
                  static_cast<unsigned long long>(generation),
                  static_cast<unsigned long long>(seq));
    return buf;
}

Result<std::string> ManifestPayload::seal(const ProviderRegistry& registry, uint64_t generation,
                                          std::string_view address, Slice plaintext) {
    EncryptionProvider* provider = registry.primary_provider();
    if (provider == nullptr) return std::unexpected(Status::Config);

    uint32_t codec = kCodecNone;
    std::string packed;
    if (const Status status = compress(plaintext, codec, packed); status != Status::Ok) {
        return std::unexpected(status);
    }

    // The generation, not a per-payload counter: it is what the provider records as the object's
    // identity, and the address in the AAD is what actually separates one payload from another.
    auto made = provider->create(generation);
    if (!made) return std::unexpected(made.error());

    const size_t chunk_bytes = made->cipher->chunk_bytes();
    const uint64_t packed_len = packed.size();
    const uint64_t chunks = chunk_count(packed_len, chunk_bytes);

    std::string framed;
    framed.reserve(kHeaderBytes + registry.primary.size() + made->metadata.size() +
                   packed.size() + static_cast<size_t>(chunks) * made->cipher->overhead_bytes());
    put_fixed32(framed, kMagic);
    framed.push_back(static_cast<char>(kHeaderVersion & 0xFF));
    framed.push_back(static_cast<char>(kHeaderVersion >> 8));
    framed.push_back(static_cast<char>(registry.primary.size() & 0xFF));
    framed.push_back(static_cast<char>(registry.primary.size() >> 8));
    put_fixed32(framed, static_cast<uint32_t>(made->metadata.size()));
    put_fixed32(framed, codec);
    put_fixed64(framed, plaintext.size());
    put_fixed64(framed, packed_len);
    framed.append(registry.primary);
    framed.append(made->metadata);

    const std::string header = framed;   // everything written so far: header, id, metadata
    for (uint64_t k = 0; k < chunks; ++k) {
        const uint64_t start = k * chunk_bytes;
        const uint64_t length = chunk_plain_bytes(k, packed_len, chunk_bytes);
        const std::string aad = payload_aad(k, address, header);
        const Status status = made->cipher->seal(
            k, Slice(reinterpret_cast<const uint8_t*>(packed.data()) + start,
                     static_cast<size_t>(length)),
            Slice::from(aad), framed);
        if (status != Status::Ok) return std::unexpected(status);
    }
    return framed;
}

Result<std::string> ManifestPayload::open(const ProviderRegistry& registry, uint64_t generation,
                                          std::string_view address, Slice framed,
                                          std::string& error) {
    if (framed.size() < kHeaderBytes || decode_fixed32(framed.data()) != kMagic) {
        // Not our framing at all: a payload from an older format, or a write that never landed
        // whole. Replay treats this as an unacknowledged edit, which is why it must not be
        // conflated with the two failures below.
        return std::unexpected(Status::Corrupt);
    }
    const auto* p = framed.data();
    const uint16_t header_version =
        static_cast<uint16_t>(p[4]) | static_cast<uint16_t>(static_cast<uint16_t>(p[5]) << 8);
    if (header_version != kHeaderVersion) {
        error = "manifest payload framing version " + std::to_string(header_version) +
                " is newer than this build understands";
        return std::unexpected(Status::Unsupported);
    }
    const size_t provider_len =
        static_cast<size_t>(p[6]) | (static_cast<size_t>(p[7]) << 8);
    const size_t metadata_len = decode_fixed32(p + 8);
    const uint32_t codec = decode_fixed32(p + 12);
    const uint64_t plain_len = decode_fixed64(p + 16);
    const uint64_t packed_len = decode_fixed64(p + 24);
    if (plain_len > kMaxPayloadBytes || packed_len > kMaxPayloadBytes) {
        return std::unexpected(Status::Corrupt);
    }
    if (framed.size() < kHeaderBytes + provider_len + metadata_len) {
        return std::unexpected(Status::Corrupt);
    }

    const std::string id(reinterpret_cast<const char*>(p + kHeaderBytes), provider_len);
    EncryptionProvider* provider = registry.find(id);
    if (provider == nullptr) {
        // The bytes are intact; what is missing is the configuration that reads them. Reporting
        // corruption would send an operator to a restore, and worse, replay would treat it as a
        // torn write and quietly open on a truncated history.
        error = "manifest payload records encryption provider '" + id +
                "', which is not registered";
        return std::unexpected(Status::Config);
    }

    // The same id `create` was given. The stock provider takes the one recorded in the metadata
    // instead, but an embedder's need not, so it is passed rather than left to chance.
    auto cipher = provider->open(generation, Slice(p + kHeaderBytes + provider_len, metadata_len));
    if (!cipher) {
        // The provider is registered and still could not open the payload, which in practice means
        // it holds a different key than the one that wrote this. Saying so is the difference
        // between checking a key and suspecting the disk.
        error = "encryption provider '" + id + "' could not open a manifest payload (" +
                std::string(status_name(cipher.error())) +
                "): most often the key it holds is not the one the store was written with";
        return std::unexpected(cipher.error());
    }

    const size_t chunk_bytes = (*cipher)->chunk_bytes();
    const size_t overhead = (*cipher)->overhead_bytes();
    const uint64_t chunks = chunk_count(packed_len, chunk_bytes);
    const uint64_t expected = packed_len + chunks * overhead;
    const size_t body = kHeaderBytes + provider_len + metadata_len;
    if (framed.size() - body != expected) {
        // The exact length is computable from the header the payload carries, so a mismatch is
        // never ambiguous: this is a truncated or padded object, not a decision to make later.
        return std::unexpected(Status::Corrupt);
    }

    const std::string_view header(reinterpret_cast<const char*>(p), body);
    std::string packed;
    packed.reserve(static_cast<size_t>(packed_len));
    for (uint64_t k = 0; k < chunks; ++k) {
        const uint64_t length = chunk_plain_bytes(k, packed_len, chunk_bytes) + overhead;
        const uint64_t start = k * (chunk_bytes + overhead);
        const std::string aad = payload_aad(k, address, header);
        const Status status = (*cipher)->open(
            k, Slice(p + body + start, static_cast<size_t>(length)), Slice::from(aad), packed);
        if (status != Status::Ok) return std::unexpected(status);
    }
    return decompress(Slice(reinterpret_cast<const uint8_t*>(packed.data()), packed.size()), codec,
                      plain_len);
}

}  // namespace elysiumkv
