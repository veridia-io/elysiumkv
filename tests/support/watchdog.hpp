#ifndef ELYSIUMKV_TESTS_SUPPORT_WATCHDOG_HPP
#define ELYSIUMKV_TESTS_SUPPORT_WATCHDOG_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>

namespace elysiumkv::test {

/// ARCHITECTURE.md "Benchmarks" — liveness. A deadlocked compaction or a write stall that never
/// releases shows up as a hang, not a mismatch, and a stopped suite is easy to
/// misread as a slow one. Every replay carries a per-operation timeout; when it
/// expires the process aborts with the operation index, so CI reports a failure
/// with a location rather than a timeout with none.
class OperationWatchdog {
public:
    OperationWatchdog(std::chrono::milliseconds timeout, std::string label)
        : timeout_(timeout), label_(std::move(label)) {
        last_beat_ = std::chrono::steady_clock::now();
        thread_ = std::thread([this] { run(); });
    }

    OperationWatchdog(const OperationWatchdog&) = delete;
    OperationWatchdog& operator=(const OperationWatchdog&) = delete;

    ~OperationWatchdog() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        wake_.notify_all();
        thread_.join();
    }

    void beat(size_t op_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        op_index_ = op_index;
        last_beat_ = std::chrono::steady_clock::now();
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopped_) {
            wake_.wait_for(lock, std::chrono::milliseconds(100));
            if (stopped_) return;
            if (std::chrono::steady_clock::now() - last_beat_ <= timeout_) continue;

            std::fprintf(stderr,
                         "\nLIVENESS: %s made no progress for %lld ms at operation %zu.\n"
                         "This is a hang, not a slow run.\n",
                         label_.c_str(), static_cast<long long>(timeout_.count()), op_index_);
            std::fflush(stderr);
            std::abort();
        }
    }

    std::chrono::milliseconds timeout_;
    std::string label_;
    std::mutex mutex_;
    std::condition_variable wake_;
    std::chrono::steady_clock::time_point last_beat_;
    size_t op_index_ = 0;
    bool stopped_ = false;
    std::thread thread_;
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_WATCHDOG_HPP
