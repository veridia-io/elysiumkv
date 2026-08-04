#ifndef ELYSIUMKV_MEMTABLE_ARENA_HPP
#define ELYSIUMKV_MEMTABLE_ARENA_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace elysiumkv {

class MemoryBudget;

/// ARCHITECTURE.md "A write" — a bump allocator. Every memtable entry has identical lifetime, so the
/// whole arena is released as one block after the flush completes and no reader
/// holds it. This removes per-entry deallocation entirely.
///
/// Allocation is single-writer (the application thread). Readers never allocate,
/// so `memory_usage()` is the only method they touch and it is atomic.
class Arena {
public:
    Arena() = default;
    /// ARCHITECTURE.md "A process-wide memory budget" — **the memtable is the largest consumer, and it was the one that did not
    /// report.** `memtable_bytes` is per instance, so an embedder running one instance
    /// per shard multiplied it by the shard count while the budget said nothing. Every
    /// block this arena takes is charged here and released when the arena dies, which
    /// for a memtable is after its flush completes.
    ///
    /// Charging is unconditional: an arena cannot decline an allocation for a write the
    /// engine has already accepted. Declining is the *write path's* job, which is why
    /// ARCHITECTURE.md "A process-wide memory budget" — the shedding order ends in "stall writes" rather than "fail an allocation".
    ///
    /// **Safe to call at any point, including after allocating**, because it charges what
    /// is already held. The first version of this required being called before the first
    /// allocation — and the engine promptly broke that rule, since a skip-list allocates
    /// its head node in its constructor. The budget then released one block it had never
    /// been charged for and `used()` underflowed to near 2^64. An ordering requirement
    /// that the only caller cannot meet is a bug, not a contract.
    void set_budget(MemoryBudget* budget);
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    ~Arena();

    uint8_t* allocate(size_t bytes);
    /// Aligned to `alignof(std::max_align_t)`; used for skip-list nodes, which
    /// contain atomics.
    uint8_t* allocate_aligned(size_t bytes);

    size_t memory_usage() const { return memory_usage_.load(std::memory_order_relaxed); }

private:
    uint8_t* allocate_fallback(size_t bytes);
    uint8_t* allocate_new_block(size_t block_bytes);

    MemoryBudget* budget_ = nullptr;
    uint8_t* head_ = nullptr;
    size_t remaining_ = 0;
    std::vector<uint8_t*> blocks_;
    std::atomic<size_t> memory_usage_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MEMTABLE_ARENA_HPP
