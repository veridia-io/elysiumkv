#include "support/alloc_counter.hpp"

#include <cstdlib>
#include <new>

namespace elysiumkv::test {

std::atomic<uint64_t>& AllocationCounters::allocations() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

std::atomic<uint64_t>& AllocationCounters::bytes() {
    static std::atomic<uint64_t> counter{0};
    return counter;
}

}  // namespace elysiumkv::test

// Replacing global operator new/delete instruments every allocation in the test
// binary, gtest's included. That is fine: the assertions measure a *difference*
// across a quiet region, not an absolute.
void* operator new(size_t size) {
    elysiumkv::test::AllocationCounters::allocations().fetch_add(1, std::memory_order_relaxed);
    elysiumkv::test::AllocationCounters::bytes().fetch_add(size, std::memory_order_relaxed);
    if (size == 0) size = 1;
    void* p = std::malloc(size);
    if (p == nullptr) throw std::bad_alloc();
    return p;
}

void* operator new[](size_t size) { return operator new(size); }

void* operator new(size_t size, const std::nothrow_t&) noexcept {
    elysiumkv::test::AllocationCounters::allocations().fetch_add(1, std::memory_order_relaxed);
    elysiumkv::test::AllocationCounters::bytes().fetch_add(size, std::memory_order_relaxed);
    return std::malloc(size == 0 ? 1 : size);
}

void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
    return operator new(size, tag);
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
