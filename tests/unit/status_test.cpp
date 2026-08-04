#include "elysiumkv/status.hpp"

#include <gtest/gtest.h>

#include <set>

namespace elysiumkv {
namespace {

constexpr Status kAll[] = {
    Status::Ok,     Status::NotFound, Status::Corrupt, Status::Unusable,
    Status::Fenced, Status::Config,   Status::Io,      Status::Stalled,
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
    EXPECT_FALSE(is_terminal(Status::Ok));
    EXPECT_FALSE(is_terminal(Status::NotFound));
    EXPECT_FALSE(is_terminal(Status::Stalled));
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
