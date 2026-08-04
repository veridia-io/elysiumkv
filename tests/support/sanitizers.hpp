#ifndef ELYSIUMKV_TESTS_SUPPORT_SANITIZERS_HPP
#define ELYSIUMKV_TESTS_SUPPORT_SANITIZERS_HPP

namespace elysiumkv::test {

/// True when the binary is built with a sanitizer whose runtime allocates and
/// retains memory on its own account — redzones, quarantines, shadow maps.
///
/// The allocation-count and resident-memory assertions of ARCHITECTURE.md "Benchmarks" measure the
/// engine, and under a sanitizer they would be measuring the sanitizer. The
/// sanitizers remain a build gate for *correctness* (ARCHITECTURE.md "Invariants and sanitizers"); these two
/// assertions are simply asked of the release and debug builds instead.
inline constexpr bool running_under_sanitizer() {
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
    return true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || \
    __has_feature(memory_sanitizer)
    return true;
#else
    return false;
#endif
#else
    return false;
#endif
}

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_SANITIZERS_HPP
