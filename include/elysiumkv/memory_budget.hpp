#ifndef ELYSIUMKV_MEMORY_BUDGET_HPP
#define ELYSIUMKV_MEMORY_BUDGET_HPP

#include <atomic>
#include <cstddef>

namespace elysiumkv {

/// ARCHITECTURE.md "A process-wide memory budget" — per process, not per instance.
///
/// Public because `Options::memory_budget` is: this lived under `src/` while being the
/// declared type of a public field, so an embedder could see the field and had no way to
/// construct a value for it. Many embedders run several
/// instances in one process (one per shard, partition or tenant), so memtable
/// and cache sizing multiplies by instance count; a per-instance constant is the
/// wrong unit.
///
/// One budget covers memtable arenas, the block cache, and any
/// MemoryCacheBlobStore. When exhausted, shed in this order: evict block cache,
/// flush memtables, then stall writes.
class MemoryBudget {
public:
    explicit MemoryBudget(size_t total_bytes) : total_(total_bytes) {}

    bool try_acquire(size_t bytes) {
        size_t used = used_.load(std::memory_order_relaxed);
        while (true) {
            if (used + bytes > total_) return false;
            if (used_.compare_exchange_weak(used, used + bytes, std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    /// Charges unconditionally and reports whether the budget is now exceeded. For a
    /// consumer that cannot decline — a memtable arena serving a write the engine has
    /// already accepted — the truthful accounting is worth more than a refusal it would
    /// have to ignore. The write path is what acts on the overage (ARCHITECTURE.md "A process-wide memory budget").
    bool try_acquire_over(size_t bytes) {
        return used_.fetch_add(bytes, std::memory_order_acq_rel) + bytes <= total_;
    }

    void release(size_t bytes) { used_.fetch_sub(bytes, std::memory_order_acq_rel); }

    /// Bytes over the limit, or zero.
    size_t overage() const {
        const size_t used = used_.load(std::memory_order_relaxed);
        return used > total_ ? used - total_ : 0;
    }

    size_t used() const { return used_.load(std::memory_order_relaxed); }
    size_t total() const { return total_; }

private:
    const size_t total_;
    std::atomic<size_t> used_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MEMORY_BUDGET_HPP
