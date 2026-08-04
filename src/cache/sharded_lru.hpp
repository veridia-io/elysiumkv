#ifndef ELYSIUMKV_CACHE_SHARDED_LRU_HPP
#define ELYSIUMKV_CACHE_SHARDED_LRU_HPP

#include "elysiumkv/memory_budget.hpp"
#include "cache/block_cache.hpp"

#include <array>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace elysiumkv {

/// ARCHITECTURE.md "Reads don't copy" — sharded LRU, one mutex per shard. Roughly 300 lines; no dependency is
/// warranted for this.
///
/// Blocks are handed out as `shared_ptr<const Block>`, so a reader's use
/// outlives eviction naturally — that is what backs `Pinned` at the public API,
/// and why eviction never has to consult a pin count.
class ShardedLruBlockCache final : public BlockCache {
public:
    static constexpr size_t kNumShards = 16;

    /// `budget` is optional; when present the cache reports every byte it holds
    /// and releases them on eviction.
    explicit ShardedLruBlockCache(size_t capacity_bytes, MemoryBudget* budget = nullptr);
    ~ShardedLruBlockCache() override;

    std::shared_ptr<const Block> get(uint64_t file_number, uint64_t block_offset) override;
    void insert(uint64_t file_number, uint64_t block_offset,
                std::shared_ptr<const Block> block) override;
    void evict_file(uint64_t file_number) override;
    size_t approximate_bytes() const override;
    size_t evict_at_least(size_t bytes) override;

    uint64_t hits() const override { return hits_.load(std::memory_order_relaxed); }
    uint64_t misses() const override { return misses_.load(std::memory_order_relaxed); }

private:
    struct Key {
        uint64_t file_number;
        uint64_t block_offset;
        bool operator==(const Key&) const = default;
    };
    struct KeyHash {
        /// Finalised, not just combined. Block offsets are multiples of the
        /// block size, so their low bits are constant — without a mix step every
        /// block of a file lands in the same shard, collapsing the cache to a
        /// sixteenth of its capacity and serialising every lookup behind one
        /// mutex.
        size_t operator()(const Key& key) const {
            uint64_t h = key.file_number * 0x9E3779B97F4A7C15ull + key.block_offset;
            h ^= h >> 33;
            h *= 0xFF51AFD7ED558CCDull;
            h ^= h >> 33;
            h *= 0xC4CEB9FE1A85EC53ull;
            h ^= h >> 33;
            return static_cast<size_t>(h);
        }
    };
    struct Entry {
        Key key;
        std::shared_ptr<const Block> block;
        size_t charge;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::list<Entry> lru;  // front = most recently used
        std::unordered_map<Key, std::list<Entry>::iterator, KeyHash> index;
        size_t bytes = 0;
    };

    Shard& shard_for(const Key& key);
    /// Caller holds the shard's lock. Returns bytes released.
    size_t evict_locked(Shard& shard, size_t capacity);

    std::array<Shard, kNumShards> shards_;
    size_t shard_capacity_;
    MemoryBudget* budget_;
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_CACHE_SHARDED_LRU_HPP
