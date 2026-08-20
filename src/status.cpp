#include "elysiumkv/status.hpp"

#include <string>

namespace elysiumkv {

std::string_view status_name(Status s) noexcept {
    switch (s) {
        case Status::Ok:       return "ok";
        case Status::NotFound: return "not_found";
        case Status::Corrupt:  return "corrupt";
        case Status::Unusable: return "unusable";
        case Status::Fenced:   return "fenced";
        case Status::Config:   return "config";
        case Status::Io:       return "io";
        case Status::Stalled:  return "stalled";
        case Status::Unsupported: return "unsupported";
        case Status::Stale:       return "stale";
        case Status::RecoveryRequired: return "recovery required";
    }
    return "unknown";
}

namespace {

/// One slot per thread, in one translation unit. Two definitions would be two slots, and a
/// failure recorded through one would be invisible through the other — the same trap the C ABI's
/// error slot documents. A `std::string` rather than a pointer because the message is built from
/// pieces and has to outlive them.
std::string& slot() noexcept {
    static thread_local std::string message;
    return message;
}

}  // namespace

std::string_view last_error() noexcept { return slot(); }

namespace internal {

void set_last_error(std::string_view message) noexcept {
    // Nothing here may throw: it runs on failure paths, several of them already unwinding from an
    // allocation that failed. A message lost to a bad_alloc is the right trade against a second
    // exception thrown while reporting the first.
    try {
        slot().assign(message);
    } catch (...) {
        slot().clear();
    }
}

}  // namespace internal

}  // namespace elysiumkv
