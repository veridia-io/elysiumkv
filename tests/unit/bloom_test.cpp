#include "sst/bloom.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "user:%08d", i);
    return buf;
}

std::string build_filter(int keys, int bits_per_key = 10, int probes = 6) {
    BloomBuilder builder(bits_per_key, probes);
    for (int i = 0; i < keys; ++i) {
        const std::string key = key_at(i);
        builder.add(Slice::from(key));
    }
    return builder.finish();
}

// The one property a bloom filter must never violate: a key that was added is
// never reported absent. A false negative would lose data silently.
TEST(Bloom, NoFalseNegatives) {
    for (int count : {1, 10, 1000, 100000}) {
        const std::string filter = build_filter(count);
        for (int i = 0; i < count; ++i) {
            const std::string key = key_at(i);
            EXPECT_TRUE(bloom_may_contain(Slice::from(filter), Slice::from(key)))
                << count << " " << key;
        }
    }
}

TEST(Bloom, FalsePositiveRateIsNearTheTarget) {
    constexpr int kKeys = 100000;
    const std::string filter = build_filter(kKeys);

    int false_positives = 0;
    constexpr int kProbes = 100000;
    for (int i = 0; i < kProbes; ++i) {
        const std::string key = "absent:" + std::to_string(i);
        if (bloom_may_contain(Slice::from(filter), Slice::from(key))) ++false_positives;
    }
    const double rate = static_cast<double>(false_positives) / kProbes;
    // 10 bits/key with 6 probes is ~1%; blocking costs a little. Anything above
    // 3% means the hashing is not spreading.
    EXPECT_LT(rate, 0.03) << "false positive rate " << rate;
}

TEST(Bloom, SizeTracksBitsPerKey) {
    EXPECT_LT(build_filter(10000, 4).size(), build_filter(10000, 10).size());
    EXPECT_LT(build_filter(10000, 10).size(), build_filter(10000, 20).size());

    // ~10 bits per key, plus the 5-byte trailer and block rounding.
    const size_t bytes = build_filter(10000, 10).size();
    EXPECT_GE(bytes, 10000u * 10 / 8);
    EXPECT_LT(bytes, 10000u * 10 / 8 + 128);
}

// A damaged or empty filter must never turn into a wrong answer — it costs a
// data-block read instead.
TEST(Bloom, MalformedFiltersAreConservative) {
    const std::string key = key_at(0);
    EXPECT_TRUE(bloom_may_contain(Slice(), Slice::from(key)));
    EXPECT_TRUE(bloom_may_contain(Slice::from(std::string("abc")), Slice::from(key)));

    std::string filter = build_filter(100);
    filter.resize(filter.size() - 7);  // block count no longer matches the bitmap
    EXPECT_TRUE(bloom_may_contain(Slice::from(filter), Slice::from(key)));
}

TEST(Bloom, EmptyKeyIsAnOrdinaryKey) {
    BloomBuilder builder(10, 6);
    builder.add(Slice());
    const std::string filter = builder.finish();
    EXPECT_TRUE(bloom_may_contain(Slice::from(filter), Slice()));
}

}  // namespace
}  // namespace elysiumkv
