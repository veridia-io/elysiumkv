#include "memtable/skiplist_memtable.hpp"

#include <cstring>

namespace elysiumkv {
namespace {

using Node = SkiplistMemtable::Node;

}  // namespace

/// Forward iteration over the list. Holds no lock: the writer only ever splices
/// nodes in, never removes, so a reader's position stays valid.
class SkiplistIterator final : public InternalIterator {
public:
    explicit SkiplistIterator(const SkiplistMemtable* table) : table_(table) {}

    bool valid() const override { return node_ != nullptr; }
    Status status() const override { return Status::Ok; }

    void seek_to_first() override {
        node_ = table_->head_->next[0].load(std::memory_order_acquire);
        load_value();
    }
    void seek(Slice target) override {
        node_ = table_->find_greater_or_equal(target, nullptr);
        load_value();
    }
    void next() override {
        if (node_ == nullptr) return;
        node_ = node_->next[0].load(std::memory_order_acquire);
        load_value();
    }

    void seek_to_last() override {
        set(table_->find_last());
    }
    void seek_for_prev(Slice target) override {
        Node* at_or_after = table_->find_greater_or_equal(target, nullptr);
        if (at_or_after != nullptr && at_or_after->key() == target) {
            set(at_or_after);  // an exact hit satisfies <=
        } else {
            set(table_->find_less_than(target));
        }
    }
    void prev() override {
        if (node_ == nullptr) return;
        set(table_->find_less_than(node_->key()));
    }

    Slice key() const override { return node_->key(); }
    Slice value() const override { return value_ == nullptr ? Slice() : value_->slice(); }
    ValueType type() const override {
        return value_ == nullptr ? ValueType::Put : value_->type;
    }

private:
    /// `head_` is the sentinel before the first key, so reaching it means the scan is done.
    void set(Node* node) {
        node_ = node == table_->head_ ? nullptr : node;
        load_value();
    }

    /// One acquire load per entry: the record is immutable, so key and value can
    /// never disagree even if the writer replaces the value mid-iteration.
    void load_value() {
        value_ = node_ == nullptr ? nullptr : node_->value.load(std::memory_order_acquire);
    }

    const SkiplistMemtable* table_;
    Node* node_ = nullptr;
    const SkiplistMemtable::ValueRecord* value_ = nullptr;
};

SkiplistMemtable::SkiplistMemtable() {
    head_ = new_node(Slice(), kMaxHeight);
    for (int i = 0; i < kMaxHeight; ++i) {
        head_->next[i].store(nullptr, std::memory_order_relaxed);
    }
}

Node* SkiplistMemtable::new_node(Slice key, int height) {
    const size_t bytes = sizeof(Node) + sizeof(std::atomic<Node*>) * static_cast<size_t>(height - 1);
    auto* node = reinterpret_cast<Node*>(arena_.allocate_aligned(bytes));
    node->value.store(nullptr, std::memory_order_relaxed);
    node->height = height;
    node->key_len = static_cast<uint32_t>(key.size());
    if (key.empty()) {
        node->key_data = nullptr;
    } else {
        uint8_t* copy = arena_.allocate(key.size());
        std::memcpy(copy, key.data(), key.size());
        node->key_data = copy;
    }
    return node;
}

const SkiplistMemtable::ValueRecord* SkiplistMemtable::new_value(ValueType type, Slice value) {
    auto* record = reinterpret_cast<ValueRecord*>(
        arena_.allocate_aligned(sizeof(ValueRecord) + value.size()));
    record->size = static_cast<uint32_t>(value.size());
    record->type = type;
    if (!value.empty()) {
        std::memcpy(const_cast<uint8_t*>(record->bytes()), value.data(), value.size());
    }
    return record;
}

int SkiplistMemtable::random_height() {
    // xorshift32: deterministic, so a seeded differential run reproduces the
    // exact list shape.
    rng_state_ ^= rng_state_ << 13;
    rng_state_ ^= rng_state_ >> 17;
    rng_state_ ^= rng_state_ << 5;

    int height = 1;
    uint32_t bits = rng_state_;
    while (height < kMaxHeight && (bits & 3u) == 0) {  // p = 1/4
        ++height;
        bits >>= 2;
    }
    return height;
}

Node* SkiplistMemtable::find_greater_or_equal(Slice key, Node** prev) const {
    Node* node = head_;
    int level = max_height_.load(std::memory_order_acquire) - 1;
    while (true) {
        Node* next = node->next[level].load(std::memory_order_acquire);
        if (next != nullptr && next->key() < key) {
            node = next;
            continue;
        }
        if (prev != nullptr) prev[level] = node;
        if (level == 0) return next;
        --level;
    }
}

void SkiplistMemtable::insert(Slice key, ValueType type, Slice value) {
    Node* prev[kMaxHeight];
    Node* next = find_greater_or_equal(key, prev);

    // Deduplicate on insert (ARCHITECTURE.md "Positional recency"): one entry per key, so the flushed SST needs no
    // tie-break. Swapping one pointer to an immutable record keeps concurrent
    // readers coherent.
    if (next != nullptr && next->key() == key) {
        // The key is already here, so the *count* does not move — but its kind may. A put
        // overwritten by a delete turns a live record into a tombstone and vice versa, and the
        // tombstone count has to follow or the entry count stops being tight.
        const ValueRecord* previous = next->value.load(std::memory_order_relaxed);
        const bool was_delete = previous != nullptr && previous->type == ValueType::Delete;
        const bool is_delete = type == ValueType::Delete;
        next->value.store(new_value(type, value), std::memory_order_release);
        if (was_delete != is_delete) {
            tombstones_.fetch_add(is_delete ? 1u : ~0ull, std::memory_order_relaxed);
        }
        return;
    }

    const int height = random_height();
    const int previous_height = max_height_.load(std::memory_order_relaxed);
    if (height > previous_height) {
        for (int i = previous_height; i < height; ++i) prev[i] = head_;
        // Readers concurrently traversing at the old height still find the node
        // through level 0, so publishing the height before the links is safe.
        max_height_.store(height, std::memory_order_release);
    }

    Node* node = new_node(key, height);
    node->value.store(new_value(type, value), std::memory_order_relaxed);
    for (int i = 0; i < height; ++i) {
        node->next[i].store(prev[i]->next[i].load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
        prev[i]->next[i].store(node, std::memory_order_release);
    }
    entries_.fetch_add(1, std::memory_order_relaxed);
    if (type == ValueType::Delete) tombstones_.fetch_add(1, std::memory_order_relaxed);
}

void SkiplistMemtable::put(Slice key, Slice value) { insert(key, ValueType::Put, value); }

void SkiplistMemtable::remove(Slice key) { insert(key, ValueType::Delete, Slice()); }

void SkiplistMemtable::delete_range(Slice lower, Slice upper) {
    if (!(lower < upper)) return;   // an empty range deletes nothing

    // Every key already here is turned into a point delete, because the range this call records
    // will shadow only *older* files and these keys are about to be flushed into the very file that
    // carries it. Replacing an existing key swaps one value pointer and adds no node, so walking
    // the list while doing it does not disturb the walk.
    for (auto it = ascending_from(lower); it->valid(); it->next()) {
        if (!(it->key() < upper)) break;
        if (it->type() == ValueType::Delete) continue;   // already a tombstone; leave the count alone
        insert(it->key(), ValueType::Delete, Slice());
    }

    auto* node = reinterpret_cast<RangeNode*>(arena_.allocate_aligned(sizeof(RangeNode)));
    uint8_t* lower_copy = arena_.allocate(lower.size());
    std::memcpy(lower_copy, lower.data(), lower.size());
    uint8_t* upper_copy = arena_.allocate(upper.size());
    std::memcpy(upper_copy, upper.data(), upper.size());
    node->lower = Slice(lower_copy, lower.size());
    node->upper = Slice(upper_copy, upper.size());
    node->next = ranges_.load(std::memory_order_relaxed);
    // Release: a reader that sees this head sees a fully written node behind it.
    ranges_.store(node, std::memory_order_release);
    range_count_.fetch_add(1, std::memory_order_relaxed);
}

bool SkiplistMemtable::range_deletes(Slice key) const {
    for (const RangeNode* node = ranges_.load(std::memory_order_acquire); node != nullptr;
         node = node->next) {
        if (node->lower <= key && key < node->upper) return true;
    }
    return false;
}

std::vector<RangeTombstone> SkiplistMemtable::range_tombstones() const {
    std::vector<RangeTombstone> out;
    for (const RangeNode* node = ranges_.load(std::memory_order_acquire); node != nullptr;
         node = node->next) {
        out.push_back(RangeTombstone{
            std::string(node->lower.data(), node->lower.data() + node->lower.size()),
            std::string(node->upper.data(), node->upper.data() + node->upper.size())});
    }
    return merge_ranges(std::move(out));
}

std::optional<Entry> SkiplistMemtable::get(Slice key) const {
    Node* node = find_greater_or_equal(key, nullptr);
    if (node == nullptr || node->key() != key) return std::nullopt;

    const ValueRecord* record = node->value.load(std::memory_order_acquire);
    if (record == nullptr) return std::nullopt;
    return Entry{record->type, record->slice()};
}

Node* SkiplistMemtable::find_less_than(Slice key) const {
    Node* node = head_;
    int level = max_height_.load(std::memory_order_acquire) - 1;
    while (true) {
        Node* next = node->next[level].load(std::memory_order_acquire);
        if (next != nullptr && next->key() < key) {
            node = next;
            continue;
        }
        if (level == 0) return node;
        --level;
    }
}

Node* SkiplistMemtable::find_last() const {
    Node* node = head_;
    int level = max_height_.load(std::memory_order_acquire) - 1;
    while (true) {
        Node* next = node->next[level].load(std::memory_order_acquire);
        if (next != nullptr) {
            node = next;
            continue;
        }
        if (level == 0) return node;
        --level;
    }
}

std::unique_ptr<InternalIterator> SkiplistMemtable::descending() const {
    auto it = std::make_unique<SkiplistIterator>(this);
    it->seek_to_last();
    return it;
}

std::unique_ptr<InternalIterator> SkiplistMemtable::descending_from(Slice key) const {
    auto it = std::make_unique<SkiplistIterator>(this);
    it->seek_for_prev(key);
    return it;
}

std::unique_ptr<InternalIterator> SkiplistMemtable::ascending() const {
    auto it = std::make_unique<SkiplistIterator>(this);
    it->seek_to_first();
    return it;
}

std::unique_ptr<InternalIterator> SkiplistMemtable::ascending_from(Slice key) const {
    auto it = std::make_unique<SkiplistIterator>(this);
    it->seek(key);
    return it;
}

size_t SkiplistMemtable::approximate_bytes() const { return arena_.memory_usage(); }

}  // namespace elysiumkv
