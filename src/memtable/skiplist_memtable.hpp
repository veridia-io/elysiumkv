#ifndef ELYSIUMKV_MEMTABLE_SKIPLIST_MEMTABLE_HPP
#define ELYSIUMKV_MEMTABLE_SKIPLIST_MEMTABLE_HPP

#include "memtable/arena.hpp"
#include "memtable/memtable.hpp"
#include "version/watermark.hpp"

#include <atomic>
#include <cstdint>

namespace elysiumkv {

class MemoryBudget;

/// ARCHITECTURE.md "A write" — a concurrent skip list over a bump arena.
///
/// Concurrency contract, and it is exactly the and nothing wider: **one writer**
/// (the application thread) with **concurrent readers** (the flush thread, plus
/// that same application thread). Nodes are spliced in with release stores and
/// read with acquire loads, so a reader either sees a complete node or does not
/// see it at all.
///
/// A repeated key replaces the value in place by swapping a single pointer to an
/// immutable record — one atomic, so a reader can never observe a value stitched
/// together from two writes.
///
/// `std::map` is not acceptable here: no concurrent read during flush, and a
/// per-node allocation for every entry.
class SkiplistMemtable final : public Memtable {
public:
    /// ARCHITECTURE.md "A process-wide memory budget" — the arena charges the shared budget. Set before the first write, which for
    /// the engine is at construction: a memtable that starts charging halfway through
    /// would leave the budget understating what it holds.
    void set_memory_budget(MemoryBudget* budget) { arena_.set_budget(budget); }

    SkiplistMemtable();

    void put(Slice key, Slice value) override;
    void remove(Slice key) override;
    std::optional<Entry> get(Slice key) const override;
    std::unique_ptr<InternalIterator> ascending() const override;
    std::unique_ptr<InternalIterator> ascending_from(Slice key) const override;
    size_t approximate_bytes() const override;

    /// Wall-clock time the memtable was created, in ms. A flushed L0 file
    /// inherits it as `min_write_time_ms` (ARCHITECTURE.md "The manifest is snapshots plus edits"), which is the sole input to the
    /// recovery horizon.
    uint64_t creation_time_ms() const { return creation_time_ms_; }
    void set_creation_time_ms(uint64_t ms) { creation_time_ms_ = ms; }

    /// The watermark interval the flush will stamp onto this memtable's file. Both halves are
    /// set by `DbImpl`, which owns the notion of "the last established watermark": `low` when
    /// the memtable is created, `high` every time `set_watermark` is called while it is live.
    /// Guarded by `mem_mutex_`, the same lock as the write path — which is what makes "every
    /// write completed before the call" a well-defined set.
    const WatermarkInterval& watermark() const { return watermark_; }
    /// Records a watermark established while this memtable is live. Only the upper bound moves:
    /// the lower bound is fixed at creation, because it is what asserts the memtable holds no
    /// write at or below it, and a later call says nothing about writes already in here.
    void set_watermark_high(uint64_t position) { watermark_.high = position; }
    void set_watermark_bounds(const WatermarkInterval& bounds) { watermark_ = bounds; }

    /// Distinct keys held, live and deleted alike — the skiplist deduplicates on insert, so an
    /// overwrite does not add one.
    uint64_t num_entries() const { return entries_.load(std::memory_order_relaxed); }
    /// How many of those are deletes. Maintained across type transitions, since overwriting a put
    /// with a delete changes the kind without changing the count.
    uint64_t num_tombstones() const { return tombstones_.load(std::memory_order_relaxed); }

    struct ValueRecord {
        uint32_t size;
        ValueType type;
        // bytes follow
        const uint8_t* bytes() const { return reinterpret_cast<const uint8_t*>(this + 1); }
        Slice slice() const { return Slice(bytes(), size); }
    };

    struct Node {
        std::atomic<const ValueRecord*> value;
        const uint8_t* key_data;
        uint32_t key_len;
        int height;
        std::atomic<Node*> next[1];  // `height` entries; the rest follow

        Slice key() const { return Slice(key_data, key_len); }
    };

private:
    friend class SkiplistIterator;

    static constexpr int kMaxHeight = 12;

    Node* new_node(Slice key, int height);
    const ValueRecord* new_value(ValueType type, Slice value);
    int random_height();
    /// Last node with key < target, per level, into `prev` when non-null.
    Node* find_greater_or_equal(Slice key, Node** prev) const;
    void insert(Slice key, ValueType type, Slice value);

    Arena arena_;
    Node* head_ = nullptr;
    std::atomic<int> max_height_{1};
    std::atomic<uint64_t> entries_{0};
    std::atomic<uint64_t> tombstones_{0};
    uint64_t creation_time_ms_ = 0;
    WatermarkInterval watermark_;
    uint32_t rng_state_ = 0x2545F491u;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MEMTABLE_SKIPLIST_MEMTABLE_HPP
