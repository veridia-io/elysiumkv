#ifndef ELYSIUMKV_STATUS_HPP
#define ELYSIUMKV_STATUS_HPP

#include <cstdint>
#include <expected>
#include <string_view>

namespace elysiumkv {

/// ARCHITECTURE.md "Absence is an answer, not an error". Errors are values. Exceptions may be used inside the implementation but
/// must not escape a public entry point.
enum class Status : uint8_t {
    Ok = 0,
    NotFound,   ///< key or object absent — positive evidence of absence
    Corrupt,    ///< terminal: a DURABLE store lost data
    Unusable,   ///< terminal: instance must be closed and reopened
    Fenced,     ///< terminal: another writer owns the store
    Config,     ///< terminal: invalid configuration
    Io,         ///< retryable: could not determine
    Stalled,    ///< write blocked by backpressure
};

/// Stable lowercase name, for error text and test failure messages.
std::string_view status_name(Status) noexcept;

/// True for statuses that mean "the instance is finished": the caller must close
/// and reopen rather than retry. ARCHITECTURE.md "A tier is not a level", ARCHITECTURE.md "Ownership is one compare-and-set".
constexpr bool is_terminal(Status s) noexcept {
    return s == Status::Corrupt || s == Status::Unusable || s == Status::Fenced ||
           s == Status::Config;
}

/// True for the one status class that means "ask again later". Never evidence of
/// absence — the positive-evidence rule depends on this distinction.
constexpr bool is_retryable(Status s) noexcept { return s == Status::Io; }

template <typename T>
using Result = std::expected<T, Status>;

}  // namespace elysiumkv

#endif  // ELYSIUMKV_STATUS_HPP
