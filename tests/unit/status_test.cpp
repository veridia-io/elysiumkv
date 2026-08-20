#include "elysiumkv/status.hpp"

#include <gtest/gtest.h>

#include <set>

namespace elysiumkv {
namespace {

/// Every enumerator, and adding one here is not optional. `status_name` has a `switch` with no
/// default, so a new status compiles — the switch falls through and returns "unknown" — and every
/// property below is then simply never asked of it. Keep this list exhaustive.
constexpr Status kAll[] = {
    Status::Ok,     Status::NotFound, Status::Corrupt,     Status::Unusable,
    Status::Fenced, Status::Config,   Status::Io,          Status::Stalled,
    Status::Unsupported, Status::Stale, Status::RecoveryRequired,
};

TEST(Status, NamesAreDistinctAndKnown) {
    std::set<std::string_view> names;
    for (Status s : kAll) {
        std::string_view name = status_name(s);
        EXPECT_NE(name, "unknown") << static_cast<int>(s);
        EXPECT_TRUE(names.insert(name).second) << name;
    }
    EXPECT_EQ(names.size(), std::size(kAll));
}

// ARCHITECTURE.md "Immutable named objects" — Io means "could not determine" and must never be read as absence.
// Everything the engine treats as evidence of loss goes through NotFound.
TEST(Status, IoIsTheOnlyRetryableClass) {
    for (Status s : kAll) {
        EXPECT_EQ(is_retryable(s), s == Status::Io) << status_name(s);
    }
    EXPECT_FALSE(is_retryable(Status::NotFound));
    EXPECT_FALSE(is_terminal(Status::Io));
}

TEST(Status, TerminalStatusesRequireReopen) {
    EXPECT_TRUE(is_terminal(Status::Corrupt));
    EXPECT_TRUE(is_terminal(Status::Unusable));
    EXPECT_TRUE(is_terminal(Status::Fenced));
    EXPECT_TRUE(is_terminal(Status::Config));
    // A manifest this build cannot read will not become readable by retrying. The remedy is a
    // different binary, so the caller must close rather than loop.
    EXPECT_TRUE(is_terminal(Status::Unsupported));
    EXPECT_FALSE(is_terminal(Status::Ok));
    EXPECT_FALSE(is_terminal(Status::NotFound));
    EXPECT_FALSE(is_terminal(Status::Stalled));
    // A stale read-only instance is not finished: `refresh()` recovers it without a reopen.
    EXPECT_FALSE(is_terminal(Status::Stale));
    // Nor is a store waiting on its replay: the remedy is `mark_recovery_complete()`, and writes —
    // which is what the replay is made of — are never refused.
    EXPECT_FALSE(is_terminal(Status::RecoveryRequired));
}

// ARCHITECTURE.md "Absence is an answer, not an error" — `Unsupported` says the data is intact and
// this build cannot read it. Collapsing it into `Corrupt` would tell an operator their bytes are
// damaged and send them to a restore; collapsing it into `Config` would suggest they mis-set an
// option. It has to be distinguishable from both, and programmatically — a better message in
// `last_error` is not enough, because a caller cannot branch on prose.
TEST(Status, UnsupportedIsNeitherCorruptionNorMisconfiguration) {
    EXPECT_NE(status_name(Status::Unsupported), status_name(Status::Corrupt));
    EXPECT_NE(status_name(Status::Unsupported), status_name(Status::Config));
    EXPECT_FALSE(is_retryable(Status::Unsupported));
}

TEST(Status, ResultCarriesStatusAsTheErrorType) {
    Result<int> ok = 7;
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(*ok, 7);

    Result<int> err = std::unexpected(Status::NotFound);
    ASSERT_FALSE(err.has_value());
    EXPECT_EQ(err.error(), Status::NotFound);
}

}  // namespace
}  // namespace elysiumkv
