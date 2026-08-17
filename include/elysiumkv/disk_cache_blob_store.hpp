#ifndef ELYSIUMKV_DISK_CACHE_BLOB_STORE_HPP
#define ELYSIUMKV_DISK_CACHE_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <filesystem>
#include <memory>

namespace elysiumkv {

/// ARCHITECTURE.md "Caches chain" — a local-disk cache in front of a slower store, which in practice means a
/// remote one. This is the layer that turns a cold S3 tier from "every read is a
/// network round trip" into "every read after the first is local".
///
/// **No fsync, no crash-consistency protocol, and wiping it at startup is valid.**
/// That falls straight out of ARCHITECTURE.md "Caches chain" — the authoritative store below is written first
/// and acknowledged before anything is cached, so a cache entry is never the only
/// copy. A missing or torn entry costs one refetch. This deliberately does *not*
/// verify contents — blocks carry CRC32C (ARCHITECTURE.md "Inside an SST"), so a torn entry is caught where
/// every other corruption is caught, and a checksum here would be a second,
/// redundant one over the same bytes.
///
/// The cache directory is this layer's alone. It holds one subdirectory per cached
/// object, which is what makes evicting an object one `remove_all`.
class DiskCacheBlobStore final : public CacheBlobStore {
public:
    /// `directory` is created if missing and is assumed to be this cache's
    /// exclusive property — anything already in it is treated as cache content, so
    /// it must not be a store's directory or anything else that matters.
    /// `fetch_granularity` rounds a miss out to a chunk of that size, so a scan costs one read per
    /// chunk instead of one per block and a later read of a neighbouring range is already held.
    /// Zero fetches exactly what was asked. Defaulted so existing call sites keep their behaviour.
    DiskCacheBlobStore(std::shared_ptr<BlobStore> delegate, std::filesystem::path directory,
                       size_t max_cache_bytes, bool cache_on_write,
                       size_t fetch_granularity = 0);
    ~DiskCacheBlobStore() override;

    DiskCacheBlobStore(const DiskCacheBlobStore&) = delete;
    DiskCacheBlobStore& operator=(const DiskCacheBlobStore&) = delete;

    BlobStore& delegate() override;
    size_t max_cache_bytes() const override;
    bool cache_on_write() const override;

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    std::future<Status> remove_many(const std::vector<std::string>& names) override;
    std::future<ListResult> list(std::string_view prefix) override;

    /// ARCHITECTURE.md "Invariants and sanitizers" — **the cache checks itself.** Under `ELYSIUMKV_PARANOID` every hit is
    /// re-fetched from the delegate and compared, and a mismatch aborts naming the
    /// object and the range. A cache serving wrong bytes does not crash and does not
    /// fail its own unit tests; it produces a wrong answer somewhere far away.
    ///
    /// It is a runtime switch because it necessarily *reads from the delegate*, so a
    /// test measuring reads to the delegate has to turn it off. No effect in a build
    /// without the paranoid checks.
    void set_verify_against_delegate(bool verify);

    size_t cached_bytes() const;
    uint64_t hits() const override;
    uint64_t misses() const override;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DISK_CACHE_BLOB_STORE_HPP
