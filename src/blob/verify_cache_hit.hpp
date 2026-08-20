#ifndef ELYSIUMKV_BLOB_VERIFY_CACHE_HIT_HPP
#define ELYSIUMKV_BLOB_VERIFY_CACHE_HIT_HPP

#include "elysiumkv/blob_store.hpp"

#include <cstdio>
#include <cstdlib>
#include <string_view>

namespace elysiumkv {

/// ARCHITECTURE.md "Invariants and sanitizers" — a cache that checks itself. Compiled only under `ELYSIUMKV_PARANOID`,
/// which the debug and sanitizer presets set: every cache hit is re-fetched from the
/// delegate and compared.
///
/// This is the check a cache most needs and least often has. A cache that returns the
/// wrong bytes does not crash and does not fail its own unit tests — it produces a
/// wrong answer somewhere far away, and the report is "a scan came back one row short".
/// Verifying against the layer below turns that into a message naming the object, the
/// range and both lengths, at the moment it happens.
inline void verify_cache_hit(BlobStore& delegate, const char* layer, std::string_view name,
                            uint64_t offset, size_t len, const Buffer& served) {
    auto truth = delegate.get(name, offset, len).get();
    // Cannot verify is not a mismatch. A cache serving a range while the store below
    // is unreachable is the cache doing its job, and the fault-injection suite creates
    // that on purpose. Aborting here would make the checker's own presence turn a
    // deliberately injected failure into a crash.
    if (!truth) return;
    if (truth->size() == served.size() && *truth == served) return;

    std::fprintf(stderr,
                 "%s served the wrong bytes for %.*s [offset %llu, len %zu]: %zu bytes cached, "
                 "%zu bytes below%s\n",
                 layer, static_cast<int>(name.size()), name.data(),
                 static_cast<unsigned long long>(offset), len, served.size(), truth->size(),
                 truth->size() == served.size() ? " (same length, different content)" : "");
    std::abort();
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_VERIFY_CACHE_HIT_HPP
