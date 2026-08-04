#include "cache/sharded_lru.hpp"

#include <algorithm>
#include <utility>

namespace elysiumkv {

ShardedLruBlockCache::ShardedLruBlockCache(size_t capacity_bytes, MemoryBudget* budget)
    : shard_capacity_(std::max<size_t>(1, capacity_bytes / kNumShards)), budget_(budget) {}

ShardedLruBlockCache::~ShardedLruBlockCache() {
    if (budget_ == nullptr) return;
    for (Shard& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        budget_->release(shard.bytes);
        shard.bytes = 0;
    }
}

ShardedLruBlockCache::Shard& ShardedLruBlockCache::shard_for(const Key& key) {
    return shards_[KeyHash{}(key) % kNumShards];
}

std::shared_ptr<const Block> ShardedLruBlockCache::get(uint64_t file_number,
                                                       uint64_t block_offset) {
    const Key key{file_number, block_offset};
    Shard& shard = shard_for(key);
    std::lock_guard<std::mutex> lock(shard.mutex);

    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
    hits_.fetch_add(1, std::memory_order_relaxed);
    return it->second->block;
}

size_t ShardedLruBlockCache::evict_locked(Shard& shard, size_t capacity) {
    size_t released = 0;
    while (shard.bytes > capacity && !shard.lru.empty()) {
        const Entry& victim = shard.lru.back();
        shard.bytes -= victim.charge;
        released += victim.charge;
        shard.index.erase(victim.key);
        shard.lru.pop_back();
    }
    return released;
}

void ShardedLruBlockCache::insert(uint64_t file_number, uint64_t block_offset,
                                  std::shared_ptr<const Block> block) {
    if (block == nullptr) return;
    const size_t charge = block->size() + sizeof(Entry);
    const Key key{file_number, block_offset};
    Shard& shard = shard_for(key);

    size_t acquire = 0;
    size_t release = 0;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (auto existing = shard.index.find(key); existing != shard.index.end()) {
            // Blocks are immutable, so a re-insert is a no-op beyond recency.
            shard.lru.splice(shard.lru.begin(), shard.lru, existing->second);
            return;
        }
        shard.lru.push_front(Entry{key, std::move(block), charge});
        shard.index.emplace(key, shard.lru.begin());
        shard.bytes += charge;
        acquire = charge;
        release = evict_locked(shard, shard_capacity_);
    }
    if (budget_ != nullptr) {
        // **Charged unconditionally, because the release side is unconditional.** This
        // used to call `try_acquire`, which declines rather than charging when it would
        // exceed the total — while eviction below released bytes regardless. Under a
        // budget small enough to refuse anything, the two sides drifted apart and
        // `used()` underflowed to near 2^64. It went unnoticed while nothing acted on the
        // number; the moment the write path started shedding on it, it mattered.
        //
        // Unconditional is also the honest shape: the block is already in the cache by
        // the time this runs, so there is nothing a refusal could undo.
        (void)budget_->try_acquire_over(acquire);
        if (release > 0) budget_->release(release);
    }
}

void ShardedLruBlockCache::evict_file(uint64_t file_number) {
    size_t released = 0;
    for (Shard& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto it = shard.lru.begin(); it != shard.lru.end();) {
            if (it->key.file_number != file_number) {
                ++it;
                continue;
            }
            shard.bytes -= it->charge;
            released += it->charge;
            shard.index.erase(it->key);
            it = shard.lru.erase(it);
        }
    }
    // A whole-shard scan, but this runs only when an SST is unlinked — far
    // rarer than a lookup, and it keeps the hot path free of a per-file index.
    if (budget_ != nullptr && released > 0) budget_->release(released);
}

size_t ShardedLruBlockCache::evict_at_least(size_t bytes) {
    if (bytes == 0) return 0;

    // Round-robin across shards, taking each shard's least-recently-used entry, rather
    // than draining one shard at a time: a shard holds an arbitrary sixteenth of the
    // keyspace, so emptying one would evict a whole slice of hot data while leaving the
    // rest untouched. Approximating global LRU is the point of sharding at all.
    size_t released = 0;
    bool progress = true;
    while (released < bytes && progress) {
        progress = false;
        for (Shard& shard : shards_) {
            if (released >= bytes) break;
            std::lock_guard<std::mutex> lock(shard.mutex);
            if (shard.lru.empty()) continue;
            const Entry& victim = shard.lru.back();
            const size_t charge = victim.charge;
            shard.bytes -= charge;
            shard.index.erase(victim.key);
            shard.lru.pop_back();
            released += charge;
            progress = true;
        }
    }
    if (budget_ != nullptr && released > 0) budget_->release(released);
    return released;
}

size_t ShardedLruBlockCache::approximate_bytes() const {
    size_t total = 0;
    for (const Shard& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        total += shard.bytes;
    }
    return total;
}

}  // namespace elysiumkv
