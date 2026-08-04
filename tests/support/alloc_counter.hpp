#ifndef ELYSIUMKV_TESTS_SUPPORT_ALLOC_COUNTER_HPP
#define ELYSIUMKV_TESTS_SUPPORT_ALLOC_COUNTER_HPP

#include <atomic>
#include <cstddef>

namespace elysiumkv::test {

/// ARCHITECTURE.md "Benchmarks" — **allocations per operation**, counted via an instrumented allocator.
/// "The single most useful number for catching correct-but-slow code early, and
/// it is a hard assertion rather than a trend line."
///
/// The counters are global and thread-local-free: measurement happens on one
/// thread around a quiet region, and the sanitizer builds are excluded because
/// their runtimes allocate on their own account.
struct AllocationCounters {
    static std::atomic<uint64_t>& allocations();
    static std::atomic<uint64_t>& bytes();
};

inline uint64_t allocation_count() {
    return AllocationCounters::allocations().load(std::memory_order_relaxed);
}

/// Counts allocations over a scope.
class AllocationScope {
public:
    AllocationScope() : start_(allocation_count()) {}
    uint64_t count() const { return allocation_count() - start_; }
    void reset() { start_ = allocation_count(); }

private:
    uint64_t start_;
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_ALLOC_COUNTER_HPP
