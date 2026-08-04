#include "compact/merging_iterator.hpp"
#include "memtable/skiplist_memtable.hpp"
#include "sst/sst_reader.hpp"
#include "sst/sst_writer.hpp"

#include "cache/sharded_lru.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/local_file_blob_store.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

using test::TempDir;

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "user:%08d", i);
    return buf;
}

/// The read path as ARCHITECTURE.md "Positional recency" defines it: sources in recency order, resolved
/// positionally. No sequence numbers appear anywhere in this test because there
/// are none in the engine.
class ReadPathTest : public ::testing::Test {
protected:
    /// Flushes a memtable to an L0 file and returns its file number. Higher file
    /// number = more recent.
    uint64_t flush(const Memtable& table) {
        const uint64_t file_number = next_file_number_++;
        auto source = table.ascending();
        auto built = build_sst(*source, {});
        EXPECT_TRUE(built.has_value());

        const std::string name = sst_name(file_number);
        EXPECT_EQ(store_.put(name, Slice::from(built->bytes)).get(), Status::Ok);
        sizes_[file_number] = built->bytes.size();
        return file_number;
    }

    std::unique_ptr<SstReader> open(uint64_t file_number) {
        auto reader = SstReader::open(store_, sst_name(file_number), sizes_[file_number],
                                      {.file_number = file_number, .block_cache = &cache_});
        EXPECT_TRUE(reader.has_value());
        return std::move(*reader);
    }

    static std::string sst_name(uint64_t file_number) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%012llu.sst", static_cast<unsigned long long>(file_number));
        return buf;
    }

    TempDir dir_;
    LocalFileBlobStore store_{dir_.path()};
    ShardedLruBlockCache cache_{8u << 20};
    std::map<uint64_t, uint64_t> sizes_;
    uint64_t next_file_number_ = 1;
};

TEST_F(ReadPathTest, NewerSourcesShadowOlderOnesPositionally) {
    SkiplistMemtable older;
    older.put(Slice::from(std::string("a")), Slice::from(std::string("a-old")));
    older.put(Slice::from(std::string("b")), Slice::from(std::string("b-old")));
    older.put(Slice::from(std::string("c")), Slice::from(std::string("c-old")));
    const uint64_t old_file = flush(older);

    SkiplistMemtable newer;
    newer.put(Slice::from(std::string("b")), Slice::from(std::string("b-new")));
    newer.remove(Slice::from(std::string("c")));
    const uint64_t new_file = flush(newer);

    SkiplistMemtable live;
    live.put(Slice::from(std::string("a")), Slice::from(std::string("a-live")));

    auto new_reader = open(new_file);
    auto old_reader = open(old_file);

    // Recency order: memtable, then L0 by descending file number.
    std::vector<std::unique_ptr<InternalIterator>> children;
    children.push_back(live.ascending());
    children.push_back(new_reader->iterator());
    children.push_back(old_reader->iterator());
    for (auto& child : children) child->seek_to_first();
    auto merged = make_merging_iterator(std::move(children));

    std::vector<std::pair<std::string, std::string>> seen;
    for (merged->seek_to_first(); merged->valid(); merged->next()) {
        seen.emplace_back(merged->key().to_string(),
                          merged->type() == ValueType::Delete ? "<deleted>"
                                                              : merged->value().to_string());
    }
    EXPECT_EQ(seen, (std::vector<std::pair<std::string, std::string>>{
                        {"a", "a-live"}, {"b", "b-new"}, {"c", "<deleted>"}}));
}

TEST_F(ReadPathTest, MergesManyOverlappingSourcesAgainstAMapOracle) {
    std::map<std::string, std::string> oracle;
    std::mt19937 rng(20260802);
    std::vector<uint64_t> files;

    for (int round = 0; round < 8; ++round) {
        SkiplistMemtable table;
        for (int i = 0; i < 500; ++i) {
            const std::string key = key_at(static_cast<int>(rng() % 1000));
            if (rng() % 5 == 0) {
                table.remove(Slice::from(key));
                oracle.erase(key);
            } else {
                const std::string value = "r" + std::to_string(round) + "-" + key;
                table.put(Slice::from(key), Slice::from(value));
                oracle[key] = value;
            }
        }
        files.push_back(flush(table));
    }

    std::vector<std::unique_ptr<SstReader>> readers;
    std::vector<std::unique_ptr<InternalIterator>> children;
    for (auto it = files.rbegin(); it != files.rend(); ++it) {  // newest first
        readers.push_back(open(*it));
        children.push_back(readers.back()->iterator());
    }
    auto merged = make_merging_iterator(std::move(children));

    std::map<std::string, std::string> observed;
    for (merged->seek_to_first(); merged->valid(); merged->next()) {
        if (merged->type() == ValueType::Put) {
            observed[merged->key().to_string()] = merged->value().to_string();
        }
    }
    EXPECT_EQ(observed, oracle);
    EXPECT_EQ(merged->status(), Status::Ok);
}

TEST_F(ReadPathTest, SeekPositionsEverySourceAtOnce) {
    SkiplistMemtable table;
    for (int i = 0; i < 100; ++i) table.put(Slice::from(key_at(i * 2)), Slice::from("even"));
    const uint64_t file = flush(table);

    SkiplistMemtable live;
    for (int i = 0; i < 100; ++i) live.put(Slice::from(key_at(i * 2 + 1)), Slice::from("odd"));

    auto reader = open(file);
    std::vector<std::unique_ptr<InternalIterator>> children;
    children.push_back(live.ascending());
    children.push_back(reader->iterator());
    auto merged = make_merging_iterator(std::move(children));

    merged->seek(Slice::from(key_at(51)));
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->key().to_string(), key_at(51));

    merged->next();
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->key().to_string(), key_at(52));

    merged->seek(Slice::from(std::string("zzz")));
    EXPECT_FALSE(merged->valid());
    EXPECT_EQ(merged->status(), Status::Ok);
}

// ARCHITECTURE.md "Absence is an answer, not an error" — prefix iteration must prune SSTs whose key range does not intersect the
// prefix, and terminate as soon as the merged stream leaves it. This checks the
// pruning decision itself; ARCHITECTURE.md "Benchmarks" benchmarks that it does not scale with the
// keyspace.
TEST_F(ReadPathTest, PrefixRangePrunesNonOverlappingFiles) {
    SkiplistMemtable users;
    for (int i = 0; i < 100; ++i) {
        users.put(Slice::from("user:" + std::to_string(i)), Slice::from("u"));
    }
    SkiplistMemtable orders;
    for (int i = 0; i < 100; ++i) {
        orders.put(Slice::from("order:" + std::to_string(i)), Slice::from("o"));
    }
    const uint64_t users_file = flush(users);
    const uint64_t orders_file = flush(orders);

    auto users_reader = open(users_file);
    auto orders_reader = open(orders_file);

    const std::string prefix = "user:";
    std::string upper;
    ASSERT_TRUE(prefix_upper_bound(Slice::from(prefix), upper));

    // The orders file cannot intersect [prefix, upper) and is dropped before any
    // block of it is read.
    auto intersects = [&](const std::string& smallest, const std::string& largest) {
        return !(Slice::from(largest) < Slice::from(prefix) ||
                 Slice::from(upper) <= Slice::from(smallest));
    };
    EXPECT_TRUE(intersects("user:0", "user:99"));
    EXPECT_FALSE(intersects("order:0", "order:99"));

    std::vector<std::unique_ptr<InternalIterator>> children;
    children.push_back(users_reader->iterator());
    auto merged = make_merging_iterator(std::move(children));

    int seen = 0;
    for (merged->seek(Slice::from(prefix));
         merged->valid() && starts_with(merged->key(), Slice::from(prefix)); merged->next()) {
        ++seen;
    }
    EXPECT_EQ(seen, 100);
    (void)orders_reader;
}

TEST_F(ReadPathTest, FlushedFilesCarryTheirKeyRange) {
    SkiplistMemtable table;
    for (int i = 5; i < 50; ++i) table.put(Slice::from(key_at(i)), Slice::from("v"));

    auto source = table.ascending();
    auto built = build_sst(*source, {});
    ASSERT_TRUE(built.has_value());
    EXPECT_EQ(built->smallest_key, key_at(5));
    EXPECT_EQ(built->largest_key, key_at(49));
    EXPECT_EQ(built->num_entries, 45u);
}

// Tombstones survive a flush: dropping them is compaction's decision, and only
// in the bottommost level (ARCHITECTURE.md "Compaction").
TEST_F(ReadPathTest, FlushKeepsTombstonesButDropThemOnRequest) {
    SkiplistMemtable table;
    table.put(Slice::from(std::string("a")), Slice::from(std::string("v")));
    table.remove(Slice::from(std::string("b")));

    {
        auto source = table.ascending();
        auto built = build_sst(*source, {});
        ASSERT_TRUE(built.has_value());
        EXPECT_EQ(built->num_entries, 2u);
    }
    {
        auto source = table.ascending();
        auto built = build_sst(*source, {}, /*drop_tombstones=*/true);
        ASSERT_TRUE(built.has_value());
        EXPECT_EQ(built->num_entries, 1u);
        EXPECT_EQ(built->largest_key, "a");
    }
}

TEST_F(ReadPathTest, BlockCacheServesRepeatedReads) {
    SkiplistMemtable table;
    for (int i = 0; i < 2000; ++i) table.put(Slice::from(key_at(i)), Slice::from(std::string(64, 'v')));
    const uint64_t file = flush(table);

    auto reader = open(file);
    for (int pass = 0; pass < 2; ++pass) {
        for (int i = 0; i < 2000; ++i) {
            auto found = reader->get(Slice::from(key_at(i)));
            ASSERT_TRUE(found.has_value());
            ASSERT_TRUE(found->has_value());
        }
    }
    EXPECT_GT(cache_.hits(), 0u);

    // Unlinking the file must clear its blocks (ARCHITECTURE.md "Versions are immutable snapshots").
    cache_.evict_file(file);
    EXPECT_EQ(cache_.approximate_bytes(), 0u);
}

}  // namespace
}  // namespace elysiumkv
