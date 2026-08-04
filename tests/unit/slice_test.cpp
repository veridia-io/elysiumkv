#include "elysiumkv/slice.hpp"

#include <gtest/gtest.h>

#include <string>

namespace elysiumkv {
namespace {

Slice s(std::string_view v) { return Slice::from(v); }

TEST(Slice, ComparesBytewiseNotByCharSign) {
    // 0x80 must sort above 0x01 — a signed char comparison would invert this.
    const std::string high("\x80", 1);
    const std::string low("\x01", 1);
    EXPECT_TRUE(s(low) < s(high));
    EXPECT_FALSE(s(high) < s(low));
}

TEST(Slice, ShorterPrefixSortsFirst) {
    EXPECT_TRUE(s("ab") < s("abc"));
    EXPECT_TRUE(s("") < s("a"));
    EXPECT_EQ(s("abc"), s("abc"));
    EXPECT_NE(s("abc"), s("abd"));
}

TEST(Slice, EmbeddedNulsAreOrdinaryBytes) {
    const std::string a("a\0b", 3);
    const std::string b("a\0c", 3);
    EXPECT_EQ(s(a).size(), 3u);
    EXPECT_TRUE(s(a) < s(b));
}

TEST(Slice, StartsWith) {
    EXPECT_TRUE(starts_with(s("user:42"), s("user:")));
    EXPECT_FALSE(starts_with(s("user"), s("user:")));
    EXPECT_TRUE(starts_with(s("anything"), s("")));
}

// The bound the prefix iterator uses to prune SSTs (ARCHITECTURE.md "Absence is an answer, not an error").
TEST(Slice, PrefixUpperBound) {
    std::string bound;
    ASSERT_TRUE(prefix_upper_bound(s("user:"), bound));
    EXPECT_EQ(bound, "user;");

    ASSERT_TRUE(prefix_upper_bound(s(std::string("a\xFF", 2)), bound));
    EXPECT_EQ(bound, "b");

    // All-0xFF and empty prefixes have no upper bound: the scan runs to the end
    // of the keyspace instead.
    EXPECT_FALSE(prefix_upper_bound(s(std::string("\xFF\xFF", 2)), bound));
    EXPECT_FALSE(prefix_upper_bound(s(""), bound));
}

}  // namespace
}  // namespace elysiumkv
