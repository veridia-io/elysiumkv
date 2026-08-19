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
    /// terminal: the data is intact but this build cannot read it — a manifest written by a
    /// newer format version. Distinct from `Corrupt`, which says the bytes are damaged; the
    /// operator's remedy here is a different binary, not a restore.
    Unsupported,
    /// **Only a read-only instance sees this**: its version is older than the writer's retention
    /// window, so an object that version references has been legitimately collected.
    ///
    /// Neither terminal nor retryable, for the same reason `Stalled` is neither: it is a definite
    /// answer with a specific remedy, and repeating the same call will keep producing it. The
    /// remedy is `refresh()` or a reopen. **It must never be reported as `Corrupt`** — the data is
    /// not damaged and there is nothing to restore. Distinguished from real loss by re-reading the
    /// manifest pointer: if it has advanced past the version holding the missing file, the writer
    /// collected it legitimately.
    Stale,
    /// A transient store lost its contents, so what survives is **wrong rather than merely
    /// incomplete**: a key whose newer value lived there now reads as its older one. Reads are
    /// refused until the embedder replays the gap and calls `mark_recovery_complete()`.
    ///
    /// Neither terminal nor retryable, like `Stale`: a definite answer with a specific remedy that
    /// is not reopening. Writes are **not** refused — the replay that clears this is made of them,
    /// and an embedder whose replay reads as well as writes sets
    /// `Options::allow_reads_before_recovery`.
    RecoveryRequired,
};

/// Stable lowercase name, for error text and test failure messages.
std::string_view status_name(Status) noexcept;

/// True for statuses that mean "the instance is finished": the caller must close
/// and reopen rather than retry. ARCHITECTURE.md "A tier is not a level", ARCHITECTURE.md "Ownership is one compare-and-set".
constexpr bool is_terminal(Status s) noexcept {
    return s == Status::Corrupt || s == Status::Unusable || s == Status::Fenced ||
           s == Status::Config || s == Status::Unsupported;
}

/// True for the one status class that means "ask again later". Never evidence of
/// absence — the positive-evidence rule depends on this distinction.
constexpr bool is_retryable(Status s) noexcept { return s == Status::Io; }

template <typename T>
using Result = std::expected<T, Status>;

/// Why the most recent failing call **on this thread** failed, or empty if it had nothing to add.
///
/// **For the calls whose status cannot say which check fired.** `open` is the case that forces
/// this: a dozen configuration errors all report `Status::Config`, and an operator holding only
/// that has to guess. A `Result` carries a `Status` and nothing else, and widening it to carry a
/// message would put an allocation on every error path — including the ones, like `NotFound`, that
/// are not errors at all and are taken constantly.
///
/// Thread-local, so one thread's failure is never read as another's, and overwritten by the next
/// failing call that has something to say. Read it immediately after the call that failed.
///
/// **Not every failure sets it.** Empty means "no more than the status says", never "no failure".
std::string_view last_error() noexcept;

namespace internal {
/// Records the message `last_error()` returns. Engine-internal; an embedder has nothing to report
/// here, and a binding reports through its own boundary.
void set_last_error(std::string_view message) noexcept;
}  // namespace internal

}  // namespace elysiumkv

#endif  // ELYSIUMKV_STATUS_HPP
