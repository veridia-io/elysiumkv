#ifndef ELYSIUMKV_MEMORY_CACHE_BLOB_STORE_HPP
#define ELYSIUMKV_MEMORY_CACHE_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <memory>

namespace elysiumkv {

class MemoryBudget;

/// ARCHITECTURE.md "Caches chain" — an in-memory cache in front of a slower store.
///
/// **It earns its place over a *remote* delegate and mostly not otherwise.** Over
/// local files it largely duplicates the OS page cache, which does the same job with
/// better eviction for free; there, a larger `BlockCache` is the better spend because
/// that one caches *decoded* blocks. And over hot data `BlockCache` and this are
/// substitutes rather than complements: the block cache intercepts first, so a range
/// held in both is stored twice and read once. The non-overlapping role is buffering
/// against a remote store — which is why this exists at all.
///
/// **Reports to the shared `MemoryBudget` (ARCHITECTURE.md "A process-wide memory budget")**, so several instances in one
/// process cannot each size themselves as though they were alone. When the budget
/// refuses, the cache simply does not populate: a cache that cannot hold something
/// is a slow read, never an error.
class MemoryCacheBlobStore final : public CacheBlobStore {
public:
    /// `budget` may be null, which means this cache is bounded only by
    /// `max_cache_bytes` — appropriate for a single-instance process and nothing
    /// else.
    /// `fetch_granularity` rounds a miss out to a chunk of that size, so a scan costs one read per
    /// chunk instead of one per block and a later read of a neighbouring range is already held.
    /// Zero fetches exactly what was asked. Defaulted so existing call sites keep their behaviour.
    MemoryCacheBlobStore(std::shared_ptr<BlobStore> delegate, std::shared_ptr<MemoryBudget> budget,
                         size_t max_cache_bytes, bool cache_on_write,
                         size_t fetch_granularity = 0);
    ~MemoryCacheBlobStore() override;

    MemoryCacheBlobStore(const MemoryCacheBlobStore&) = delete;
    MemoryCacheBlobStore& operator=(const MemoryCacheBlobStore&) = delete;

    BlobStore& delegate() override;
    size_t max_cache_bytes() const override;
    bool cache_on_write() const override;

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    GetResult get_sync(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    std::future<Status> remove_many(const std::vector<std::string>& names) override;
    std::future<ListResult> list(std::string_view prefix) override;

private:
    /// The body of `get`. Split out so the counters are noted on one path rather
    /// than on each of the four the lookup can take.
    GetResult serve_get(std::string_view name, uint64_t offset, size_t len);

public:

    /// ARCHITECTURE.md "Invariants and sanitizers" — **the cache checks itself.** Under `ELYSIUMKV_PARANOID` every hit is
    /// re-fetched from the delegate and compared, and a mismatch aborts naming the
    /// object and the range. A cache serving wrong bytes does not crash and does not
    /// fail its own unit tests; it produces a wrong answer somewhere far away.
    ///
    /// It is a runtime switch because it necessarily *reads from the delegate*, so a
    /// test measuring reads to the delegate has to turn it off. No effect in a build
    /// without the paranoid checks.
    void set_verify_against_delegate(bool verify);

    /// Bytes currently held. Diagnostics and tests — a cache whose hit rate cannot be
    /// observed cannot be sized.
    size_t cached_bytes() const;
    uint64_t hits() const override;
    uint64_t misses() const override;
    void invalidate(std::string_view name) override;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MEMORY_CACHE_BLOB_STORE_HPP
