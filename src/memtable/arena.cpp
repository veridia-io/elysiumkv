#include "memtable/arena.hpp"

#include "elysiumkv/memory_budget.hpp"

#include <cstdlib>
#include <new>

namespace elysiumkv {
namespace {

constexpr size_t kBlockBytes = 4096;
constexpr size_t kAlignment = alignof(std::max_align_t);

}  // namespace

void Arena::set_budget(MemoryBudget* budget) {
    if (budget_ == budget) return;
    const size_t held = memory_usage_.load(std::memory_order_relaxed);
    if (budget_ != nullptr) budget_->release(held);
    budget_ = budget;
    if (budget_ != nullptr && held > 0) (void)budget_->try_acquire_over(held);
}

Arena::~Arena() {
    for (uint8_t* block : blocks_) ::operator delete[](block);
    if (budget_ != nullptr) budget_->release(memory_usage_.load(std::memory_order_relaxed));
}

uint8_t* Arena::allocate_new_block(size_t block_bytes) {
    auto* block = static_cast<uint8_t*>(::operator new[](block_bytes));
    blocks_.push_back(block);
    const size_t charge = block_bytes + sizeof(uint8_t*);
    memory_usage_.fetch_add(charge, std::memory_order_relaxed);
    // Reported, not requested: the allocation has already happened. `used()` may
    // therefore exceed `total()`, which is exactly the signal the write path sheds on.
    if (budget_ != nullptr) (void)budget_->try_acquire_over(charge);
    return block;
}

uint8_t* Arena::allocate_fallback(size_t bytes) {
    if (bytes > kBlockBytes / 4) {
        // Large request: give it its own block rather than wasting the tail of
        // the current one.
        return allocate_new_block(bytes);
    }
    head_ = allocate_new_block(kBlockBytes);
    remaining_ = kBlockBytes;

    uint8_t* result = head_;
    head_ += bytes;
    remaining_ -= bytes;
    return result;
}

uint8_t* Arena::allocate(size_t bytes) {
    if (bytes == 0) bytes = 1;
    if (bytes <= remaining_) {
        uint8_t* result = head_;
        head_ += bytes;
        remaining_ -= bytes;
        return result;
    }
    return allocate_fallback(bytes);
}

uint8_t* Arena::allocate_aligned(size_t bytes) {
    const size_t slop = reinterpret_cast<uintptr_t>(head_) & (kAlignment - 1);
    const size_t padding = slop == 0 ? 0 : kAlignment - slop;
    if (bytes + padding <= remaining_) {
        uint8_t* result = head_ + padding;
        head_ += bytes + padding;
        remaining_ -= bytes + padding;
        return result;
    }
    // A fresh block from operator new[] is already maximally aligned.
    return allocate_fallback(bytes);
}

}  // namespace elysiumkv
