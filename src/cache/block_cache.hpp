#ifndef ELYSIUMKV_CACHE_BLOCK_CACHE_HPP
#define ELYSIUMKV_CACHE_BLOCK_CACHE_HPP

#include "cache/block.hpp"

#include <cstdint>
#include <memory>

namespace elysiumkv {

/// ARCHITECTURE.md "Reads don't copy" — immutable *decompressed* blocks, keyed by (file_number, block_offset).
/// A different cache from the blob-store cache layers of ARCHITECTURE.md "Caches chain" — this one sits
/// above the whole blob chain and saves decode work as well as I/O.
class BlockCache {
public:
    virtual std::shared_ptr<const Block> get(uint64_t file_number, uint64_t block_offset) = 0;
    virtual void insert(uint64_t file_number, uint64_t block_offset,
                        std::shared_ptr<const Block> block) = 0;
    /// Called when an SST is unlinked.
    virtual void evict_file(uint64_t file_number) = 0;
    virtual size_t approximate_bytes() const = 0;

    /// ARCHITECTURE.md "A process-wide memory budget" — the first thing shed when the shared memory budget is exceeded.
    /// Evicts least-recently-used entries until at least `bytes` have been released, and
    /// returns how many actually were; less than asked means the cache is empty.
    ///
    /// The block cache goes first because it is the only consumer whose loss is pure
    /// latency: a memtable cannot be dropped without losing writes, and a blob cache
    /// entry is refetchable but so is this. It is also the cheapest to release, being
    /// bytes this process is holding purely as an optimisation.
    virtual size_t evict_at_least(size_t bytes) = 0;

    /// ARCHITECTURE.md "Statistics are a buffer, not a struct" — hit and miss counts, on the
    /// interface rather than on one implementation: an embedder that supplies its own cache, as
    /// every binding does through the C ABI, would otherwise read both fields as a permanent zero,
    /// which answers the question wrongly rather than not answering it.
    ///
    /// A decorating cache (ARCHITECTURE.md "Caches chain") reports its own layer; the chain is not summed
    /// here, because "the hit rate" of a chain is not a single number.
    virtual uint64_t hits() const = 0;
    virtual uint64_t misses() const = 0;

    virtual ~BlockCache() = default;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_CACHE_BLOCK_CACHE_HPP
