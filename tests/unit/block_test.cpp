#include "cache/block.hpp"
#include "sst/block_builder.hpp"
#include "sst/block_reader.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

std::shared_ptr<const Block> make_block(Slice content) {
    return std::make_shared<const Block>(Buffer(content.data(), content.data() + content.size()));
}

struct Entry {
    std::string key;
    ValueType type;
    std::string value;
};

std::shared_ptr<const Block> build(const std::vector<Entry>& entries, int restart_interval = 16) {
    BlockBuilder builder(restart_interval);
    for (const Entry& e : entries) {
        builder.add(Slice::from(e.key), e.type, Slice::from(e.value));
    }
    return make_block(builder.finish());
}

std::vector<Entry> sample(int count) {
    std::vector<Entry> entries;
    for (int i = 0; i < count; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "user:%08d", i);
        entries.push_back({key, i % 7 == 0 ? ValueType::Delete : ValueType::Put,
                           i % 7 == 0 ? "" : "value-" + std::to_string(i)});
    }
    return entries;
}

TEST(Block, RoundTripsEveryEntryInOrder) {
    const auto entries = sample(200);
    BlockIterator it(build(entries));

    size_t i = 0;
    for (it.seek_to_first(); it.valid(); it.next(), ++i) {
        ASSERT_LT(i, entries.size());
        EXPECT_EQ(it.key().to_string(), entries[i].key);
        EXPECT_EQ(it.type(), entries[i].type);
        EXPECT_EQ(it.value().to_string(), entries[i].value);
    }
    EXPECT_EQ(i, entries.size());
    EXPECT_EQ(it.status(), Status::Ok);
}

TEST(Block, PrefixCompressionShrinksSharedKeys) {
    std::vector<Entry> shared;
    std::vector<Entry> distinct;
    for (int i = 0; i < 100; ++i) {
        char suffix[16];
        std::snprintf(suffix, sizeof(suffix), "%04d", i);
        shared.push_back({"a-very-long-common-prefix-" + std::string(suffix), ValueType::Put, "v"});
        distinct.push_back({std::string(suffix) + "-a-very-long-common-prefix", ValueType::Put, "v"});
    }
    EXPECT_LT(build(shared)->size(), build(distinct)->size());
}

TEST(Block, SeeksToTheFirstKeyAtOrAfterTheTarget) {
    const auto entries = sample(200);
    BlockIterator it(build(entries));

    it.seek(Slice::from(std::string("user:00000100")));
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key().to_string(), "user:00000100");

    // Between two keys: lands on the next one.
    it.seek(Slice::from(std::string("user:00000100x")));
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key().to_string(), "user:00000101");

    it.seek(Slice::from(std::string("")));
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key().to_string(), entries.front().key);

    it.seek(Slice::from(std::string("zzz")));
    EXPECT_FALSE(it.valid());
    EXPECT_EQ(it.status(), Status::Ok) << "running off the end is exhaustion, not failure";
}

TEST(Block, SeekWorksAtEveryRestartInterval) {
    for (int interval : {1, 2, 16, 1000}) {
        const auto entries = sample(100);
        BlockIterator it(build(entries, interval));
        for (const Entry& e : entries) {
            it.seek(Slice::from(e.key));
            ASSERT_TRUE(it.valid()) << interval << " " << e.key;
            EXPECT_EQ(it.key().to_string(), e.key);
            EXPECT_EQ(it.value().to_string(), e.value);
        }
    }
}

TEST(Block, TombstonesCarryNoValue) {
    BlockIterator it(build({{"k", ValueType::Delete, ""}}));
    it.seek_to_first();
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.type(), ValueType::Delete);
    EXPECT_TRUE(it.value().empty());
}

TEST(Block, EmptyKeysAndValuesAreOrdinary) {
    BlockIterator it(build({{"", ValueType::Put, ""}, {"k", ValueType::Put, ""}}));
    it.seek_to_first();
    ASSERT_TRUE(it.valid());
    EXPECT_TRUE(it.key().empty());
    it.next();
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key().to_string(), "k");
}

// ARCHITECTURE.md "Inside an SST" — 0x02..0xFF are reserved. A reader must reject them, not guess — this is
// what keeps range deletes (ARCHITECTURE.md "What we deliberately did not build") addable without silently misreading old files.
TEST(Block, ReservedTypeBytesAreCorruption) {
    BlockBuilder builder(16);
    builder.add(Slice::from(std::string("k")), ValueType::Put, Slice::from(std::string("v")));
    std::string content = builder.finish().to_string();

    // shared_len=0, unshared_len=1, then the type byte.
    content[2] = static_cast<char>(0x02);
    BlockIterator it(make_block(Slice::from(content)));
    it.seek_to_first();
    EXPECT_FALSE(it.valid());
    EXPECT_EQ(it.status(), Status::Corrupt);
}

TEST(Block, StructurallyInvalidBlocksAreCorruptNotUndefined) {
    EXPECT_EQ(BlockIterator(make_block(Slice::from(std::string("")))).status(), Status::Corrupt);
    EXPECT_EQ(BlockIterator(make_block(Slice::from(std::string("ab")))).status(), Status::Corrupt);

    // num_restarts claims more restarts than the block can hold.
    std::string bogus(8, '\0');
    bogus[4] = static_cast<char>(0xFF);
    EXPECT_EQ(BlockIterator(make_block(Slice::from(bogus))).status(), Status::Corrupt);
}

TEST(Block, KeyLengthPastTheEndOfTheBlockIsCorrupt) {
    BlockBuilder builder(16);
    builder.add(Slice::from(std::string("k")), ValueType::Put, Slice::from(std::string("v")));
    std::string content = builder.finish().to_string();

    // byte 0 is shared_len, byte 1 unshared_len: claim more key than exists.
    content[1] = static_cast<char>(0x7F);
    BlockIterator it(make_block(Slice::from(content)));
    it.seek_to_first();
    EXPECT_FALSE(it.valid());
    EXPECT_EQ(it.status(), Status::Corrupt);
}

// A byte flipped in the middle of a block misaligns every entry after it. The
// iterator may stop early or report Corrupt, but it must never read outside the
// block — which is what the sanitizer builds are checking here.
TEST(Block, MisalignedEntriesNeverReadOutOfBounds) {
    const auto entries = sample(200);
    std::string content;
    {
        BlockBuilder builder(16);
        for (const Entry& e : entries) builder.add(Slice::from(e.key), e.type, Slice::from(e.value));
        content = builder.finish().to_string();
    }

    for (size_t i = 0; i < content.size(); i += 29) {
        std::string damaged = content;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x5A);

        BlockIterator it(make_block(Slice::from(damaged)));
        size_t seen = 0;
        for (it.seek_to_first(); it.valid(); it.next()) {
            ASSERT_LE(it.key().size(), damaged.size()) << i;
            ASSERT_LE(it.value().size(), damaged.size()) << i;
            if (++seen > entries.size() * 2) FAIL() << "iteration did not terminate at " << i;
        }
        EXPECT_TRUE(it.status() == Status::Ok || it.status() == Status::Corrupt);
    }
}

}  // namespace
}  // namespace elysiumkv
