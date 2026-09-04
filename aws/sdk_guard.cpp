#include "sdk_guard.hpp"

namespace elysiumkv::aws_detail {

std::mutex SdkGuard::mutex_;
size_t SdkGuard::refs_ = 0;
Aws::SDKOptions* SdkGuard::options_ = nullptr;

SdkGuard::SdkGuard() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (refs_++ == 0) {
        options_ = new Aws::SDKOptions();
        Aws::InitAPI(*options_);
    }
}

SdkGuard::~SdkGuard() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (--refs_ == 0) {
        Aws::ShutdownAPI(*options_);
        delete options_;
        options_ = nullptr;
    }
}

std::shared_ptr<Aws::Client::RetryStrategy> no_retry_strategy() {
    // StandardRetryStrategy counts the initial request as an attempt, so one means no retries.
    return Aws::MakeShared<Aws::Client::StandardRetryStrategy>("elysiumkv", 1);
}

}  // namespace elysiumkv::aws_detail
