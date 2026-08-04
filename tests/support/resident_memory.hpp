#ifndef ELYSIUMKV_TESTS_SUPPORT_RESIDENT_MEMORY_HPP
#define ELYSIUMKV_TESTS_SUPPORT_RESIDENT_MEMORY_HPP

#include <cstddef>
#include <cstdio>

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <unistd.h>
#endif

namespace elysiumkv::test {

/// Resident set size in bytes, or 0 where it cannot be read. Used by the
/// bounded-growth soak (ARCHITECTURE.md "Benchmarks"): a block cache that never evicts, or arena memory
/// retained after flush, is correct at every observable point and fatal at hour
/// four — and leak checking at exit catches neither.
inline size_t resident_bytes() {
#if defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info),
                  &count) != KERN_SUCCESS) {
        return 0;
    }
    return static_cast<size_t>(info.resident_size);
#elif defined(__linux__)
    std::FILE* statm = std::fopen("/proc/self/statm", "r");
    if (statm == nullptr) return 0;
    long total = 0;
    long resident = 0;
    const int read = std::fscanf(statm, "%ld %ld", &total, &resident);
    std::fclose(statm);
    if (read != 2) return 0;
    return static_cast<size_t>(resident) * static_cast<size_t>(::sysconf(_SC_PAGESIZE));
#else
    return 0;
#endif
}

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_RESIDENT_MEMORY_HPP
