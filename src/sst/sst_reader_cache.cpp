#include "sst/sst_reader_cache.hpp"

#include "elysiumkv/memory_budget.hpp"

#include <utility>

namespace elysiumkv {

SstReaderCache::SstReaderCache(size_t max_bytes, MemoryBudget* budget)
    : max_bytes_(max_bytes), budget_(budget) {}

SstReaderCache::~SstReaderCache() { clear(); }

std::shared_ptr<SstReader> SstReaderCache::get(uint64_t file_number) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = entries_.find(file_number);
    if (found == entries_.end()) {
        misses_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }
    hits_.fetch_add(1, std::memory_order_relaxed);
    recency_.splice(recency_.begin(), recency_, found->second.recency);
    found->second.recency = recency_.begin();
    return found->second.reader;
}

std::shared_ptr<SstReader> SstReaderCache::insert(uint64_t file_number,
                                                 std::shared_ptr<SstReader> reader) {
    if (reader == nullptr) return reader;
    const size_t cost = reader->memory_bytes();

    std::lock_guard<std::mutex> lock(mutex_);
    // Already resident — two threads opened the same file at once. Keep the first,
    // and hand back what the caller brought: both readers are equally valid, and
    // replacing the resident one would leave the other thread's shared_ptr as the
    // only reference to a reader nothing can find again.
    if (entries_.find(file_number) != entries_.end()) return reader;

    if (max_bytes_ != 0) {
        make_room_locked(cost, file_number);
        // Still no room, so this reader is larger than the whole cache. Serve it
        // without caching rather than evicting everything for one entry.
        if (bytes_ + cost > max_bytes_) return reader;
    }
    if (budget_ != nullptr && !budget_->try_acquire(cost)) {
        // ARCHITECTURE.md "A process-wide memory budget" — the budget is process-wide, so competing with a dozen other
        // instances is the normal case. A refusal means "do not become resident",
        // never "fail the read".
        return reader;
    }

    recency_.push_front(file_number);
    Entry entry;
    entry.reader = reader;
    entry.bytes = cost;
    entry.recency = recency_.begin();
    entries_.emplace(file_number, std::move(entry));
    bytes_ += cost;
    return reader;
}

void SstReaderCache::forget(uint64_t file_number) {
    std::lock_guard<std::mutex> lock(mutex_);
    drop_locked(file_number);
}

void SstReaderCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (budget_ != nullptr && bytes_ > 0) budget_->release(bytes_);
    entries_.clear();
    recency_.clear();
    bytes_ = 0;
}

size_t SstReaderCache::bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bytes_;
}

size_t SstReaderCache::count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

void SstReaderCache::make_room_locked(size_t wanted, uint64_t keep) {
    while (bytes_ + wanted > max_bytes_ && !recency_.empty()) {
        // From the back: least recently used. The entry being inserted is never a
        // victim, so a reader cannot evict itself on the way in.
        uint64_t victim = 0;
        bool found = false;
        for (auto it = recency_.rbegin(); it != recency_.rend(); ++it) {
            if (*it == keep) continue;
            victim = *it;
            found = true;
            break;
        }
        if (!found) return;
        drop_locked(victim);
    }
}

void SstReaderCache::drop_locked(uint64_t file_number) {
    auto found = entries_.find(file_number);
    if (found == entries_.end()) return;
    bytes_ -= found->second.bytes;
    if (budget_ != nullptr) budget_->release(found->second.bytes);
    recency_.erase(found->second.recency);
    // The reader itself dies here only if nobody else holds it. A live iterator or an
    // in-flight lookup keeps its own shared_ptr, which is what makes eviction safe to
    // do at any moment.
    entries_.erase(found);
}

}  // namespace elysiumkv
