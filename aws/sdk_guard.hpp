#ifndef ELYSIUMKV_AWS_SDK_GUARD_HPP
#define ELYSIUMKV_AWS_SDK_GUARD_HPP

#include <aws/core/Aws.h>
#include <aws/core/client/RetryStrategy.h>

#include <cstddef>
#include <mutex>

namespace elysiumkv::aws_detail {

/// Keeps the process-wide AWS SDK alive while any ElysiumKV AWS client exists.
class SdkGuard {
public:
    SdkGuard();
    ~SdkGuard();

    SdkGuard(const SdkGuard&) = delete;
    SdkGuard& operator=(const SdkGuard&) = delete;

private:
    static std::mutex mutex_;
    static size_t refs_;
    static Aws::SDKOptions* options_;
};

std::shared_ptr<Aws::Client::RetryStrategy> no_retry_strategy();

}  // namespace elysiumkv::aws_detail

#endif  // ELYSIUMKV_AWS_SDK_GUARD_HPP
