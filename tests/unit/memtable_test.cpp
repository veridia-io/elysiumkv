#include "memtable/skiplist_memtable.hpp"

#include "memtable/arena.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace elysiumkv {
namespace {

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "user:%08d", i);
    return buf;
}

TEST(Arena, AllocatesDistinctAlignedRegions) {
    Arena arena;
    std::vector<uint8_t*> pointers;
    for (int i = 0; i < 1000; ++i) {
        const auto bytes = static_cast<size_t>(i % 97 + 1);
        uint8_t* p = arena.allocate_aligned(bytes);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % alignof(std::max_align_t), 0u);
        std::memset(p, i & 0xFF, bytes);
        pointers.push_back(p);
    }
    for (size_t i = 1; i < pointers.size(); ++i) EXPECT_NE(pointers[i], pointers[i - 1]);
    EXPECT_GT(arena.memory_usage(), 0u);
}

TEST(Arena, ServesRequestsLargerThanABlock) {
    Arena arena;
    uint8_t* big = arena.allocate(1u << 20);
    std::memset(big, 0xAB, 1u << 20);
    EXPECT_GE(arena.memory_usage(), 1u << 20);
}

TEST(Memtable, ReadsBackWhatWasWritten) {
    SkiplistMemtable table;
    for (int i = 0; i < 1000; ++i) table.put(Slice::from(key_at(i)), Slice::from("v" + std::to_string(i)));

    for (int i = 0; i < 1000; ++i) {
        auto entry = table.get(Slice::from(key_at(i)));
        ASSERT_TRUE(entry.has_value()) << i;
        EXPECT_EQ(entry->type, ValueType::Put);
        EXPECT_EQ(entry->value.to_string(), "v" + std::to_string(i));
    }
    EXPECT_FALSE(table.get(Slice::from(std::string("absent"))).has_value());
    EXPECT_EQ(table.num_entries(), 1000u);
}

// ARCHITECTURE.md "Positional recency" — the memtable deduplicates by key on insert, so a flushed SST contains
// exactly one entry per key. There is no sequence number to break a tie with.
TEST(Memtable, RepeatedKeysAreDeduplicatedInPlace) {
    SkiplistMemtable table;
    const std::string key = "k";
    for (int i = 0; i < 100; ++i) table.put(Slice::from(key), Slice::from(std::to_string(i)));

    auto entry = table.get(Slice::from(key));
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->value.to_string(), "99");
    EXPECT_EQ(table.num_entries(), 1u);

    size_t seen = 0;
    for (auto it = table.ascending(); it->valid(); it->next()) ++seen;
    EXPECT_EQ(seen, 1u);
}

TEST(Memtable, DeleteReplacesAValueWithATombstone) {
    SkiplistMemtable table;
    table.put(Slice::from(std::string("k")), Slice::from(std::string("v")));
    table.remove(Slice::from(std::string("k")));

    auto entry = table.get(Slice::from(std::string("k")));
    ASSERT_TRUE(entry.has_value()) << "a tombstone is a hit, not an absence";
    EXPECT_EQ(entry->type, ValueType::Delete);
    EXPECT_TRUE(entry->value.empty());

    // And a delete of a key that was never present is still an entry: the level
    // below may hold it.
    table.remove(Slice::from(std::string("never")));
    auto absent = table.get(Slice::from(std::string("never")));
    ASSERT_TRUE(absent.has_value());
    EXPECT_EQ(absent->type, ValueType::Delete);
}

// --- range deletes -------------------------------------------------------------

/* A range delete does two things, and the second is the interesting one.
 *
 * It records the range, so the SST this memtable flushes into shadows every older file. And it
 * turns the keys the memtable *already holds* into point deletes on the spot — because a range
 * tombstone shadows nothing in the file that carries it, and those keys are about to be written
 * into that very file.
 */
TEST(Memtable, DeleteRangeTurnsTheKeysItAlreadyHoldsIntoTombstones) {
    SkiplistMemtable table;
    for (int i = 0; i < 10; ++i) table.put(Slice::from(key_at(i)), Slice::from("v"));

    table.delete_range(Slice::from(key_at(3)), Slice::from(key_at(7)));

    for (int i = 0; i < 10; ++i) {
        auto entry = table.get(Slice::from(key_at(i)));
        ASSERT_TRUE(entry.has_value()) << i;
        const bool inside = i >= 3 && i < 7;
        EXPECT_EQ(entry->type, inside ? ValueType::Delete : ValueType::Put) << i;
    }
    EXPECT_EQ(table.num_entries(), 10u) << "a point delete replaces in place, it does not add a key";
    EXPECT_EQ(table.num_tombstones(), 4u);
}

/* The ordering-free file format rests on this case. A write landing after a range delete must
 * survive it, and the file that carries both cannot say which came first — so the memtable, which
 * does know, resolves it at call time. The later put simply overwrites the point delete.
 */
TEST(Memtable, AWriteAfterARangeDeleteSurvivesIt) {
    SkiplistMemtable table;
    table.put(Slice::from(key_at(5)), Slice::from("before"));
    table.delete_range(Slice::from(key_at(0)), Slice::from(key_at(9)));
    table.put(Slice::from(key_at(5)), Slice::from("after"));

    auto entry = table.get(Slice::from(key_at(5)));
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->type, ValueType::Put);
    EXPECT_EQ(entry->value.to_string(), "after");

    // …and the range is still recorded, because older files must still be shadowed.
    EXPECT_TRUE(table.range_deletes(Slice::from(key_at(5))));
}

TEST(Memtable, ARecordedRangeIsHalfOpen) {
    SkiplistMemtable table;
    table.delete_range(Slice::from("b"), Slice::from("d"));

    EXPECT_FALSE(table.range_deletes(Slice::from("a")));
    EXPECT_TRUE(table.range_deletes(Slice::from("b"))) << "lower bound included";
    EXPECT_TRUE(table.range_deletes(Slice::from("c")));
    EXPECT_FALSE(table.range_deletes(Slice::from("d"))) << "upper bound excluded";
}

TEST(Memtable, AnEmptyOrInvertedRangeRecordsNothing) {
    SkiplistMemtable table;
    table.put(Slice::from("b"), Slice::from("v"));
    table.delete_range(Slice::from("c"), Slice::from("c"));
    table.delete_range(Slice::from("z"), Slice::from("a"));

    EXPECT_TRUE(table.range_tombstones().empty());
    EXPECT_EQ(table.get(Slice::from("b"))->type, ValueType::Put);
}

/* Overlapping ranges collapse to their union, which is all the fragmentation a file needs: every
 * tombstone in one file shadows the same thing, so two overlapping ranges are indistinguishable
 * from the one covering both.
 */
TEST(Memtable, OverlappingAndAdjacentRangesMergeIntoADisjointCover) {
    SkiplistMemtable table;
    table.delete_range(Slice::from("m"), Slice::from("p"));
    table.delete_range(Slice::from("a"), Slice::from("c"));
    table.delete_range(Slice::from("b"), Slice::from("d"));   // overlaps the one before
    table.delete_range(Slice::from("d"), Slice::from("e"));   // adjacent to it
    table.delete_range(Slice::from("n"), Slice::from("o"));   // wholly inside the first

    const std::vector<RangeTombstone> merged = table.range_tombstones();
    ASSERT_EQ(merged.size(), 2u);
    EXPECT_EQ(merged[0].lower, "a");
    EXPECT_EQ(merged[0].upper, "e") << "overlapping and adjacent both collapse";
    EXPECT_EQ(merged[1].lower, "m");
    EXPECT_EQ(merged[1].upper, "p") << "a range inside another adds nothing";
}

TEST(Memtable, SeveralRangesEachStillAnswer) {
    SkiplistMemtable table;
    table.delete_range(Slice::from("a"), Slice::from("c"));
    table.delete_range(Slice::from("x"), Slice::from("z"));

    for (const char* covered : {"a", "b", "x", "y"}) {
        EXPECT_TRUE(table.range_deletes(Slice::from(std::string(covered)))) << covered;
    }
    for (const char* clear : {"", "c", "d", "z"}) {
        EXPECT_FALSE(table.range_deletes(Slice::from(std::string(clear)))) << clear;
    }
}

TEST(Memtable, IteratesInBytewiseOrderRegardlessOfInsertionOrder) {
    SkiplistMemtable table;
    std::vector<int> order(500);
    for (int i = 0; i < 500; ++i) order[static_cast<size_t>(i)] = i;
    std::shuffle(order.begin(), order.end(), std::mt19937(1234));
    for (int i : order) table.put(Slice::from(key_at(i)), Slice::from(std::to_string(i)));

    int expected = 0;
    for (auto it = table.ascending(); it->valid(); it->next(), ++expected) {
        EXPECT_EQ(it->key().to_string(), key_at(expected));
    }
    EXPECT_EQ(expected, 500);
}

TEST(Memtable, SeeksToTheFirstKeyAtOrAfterTheTarget) {
    SkiplistMemtable table;
    for (int i = 0; i < 100; ++i) table.put(Slice::from(key_at(i * 10)), Slice::from("v"));

    auto exact = table.ascending_from(Slice::from(key_at(500)));
    ASSERT_TRUE(exact->valid());
    EXPECT_EQ(exact->key().to_string(), key_at(500));

    auto between = table.ascending_from(Slice::from(key_at(505)));
    ASSERT_TRUE(between->valid());
    EXPECT_EQ(between->key().to_string(), key_at(510));

    auto past = table.ascending_from(Slice::from(std::string("zzz")));
    EXPECT_FALSE(past->valid());
}

TEST(Memtable, EmptyKeysAndValuesAreOrdinary) {
    SkiplistMemtable table;
    table.put(Slice(), Slice());
    auto entry = table.get(Slice());
    ASSERT_TRUE(entry.has_value());
    EXPECT_TRUE(entry->value.empty());

    auto it = table.ascending();
    ASSERT_TRUE(it->valid());
    EXPECT_TRUE(it->key().empty());
}

TEST(Memtable, ApproximateBytesGrowsWithContent) {
    SkiplistMemtable table;
    const size_t empty = table.approximate_bytes();
    for (int i = 0; i < 5000; ++i) {
        table.put(Slice::from(key_at(i)), Slice::from(std::string(100, 'v')));
    }
    EXPECT_GT(table.approximate_bytes(), empty + 5000u * 100);
}

// ARCHITECTURE.md "A write" — the whole reason for a concurrent skip list: the flush thread reads while
// the application thread writes. Run under TSan, this is the test that says the
// memory ordering is right.
TEST(Memtable, ReadersSeeConsistentEntriesWhileTheWriterRuns) {
    SkiplistMemtable table;
    constexpr int kKeys = 20000;
    std::atomic<bool> writing{true};
    std::atomic<int> highest{-1};

    std::thread writer([&] {
        for (int i = 0; i < kKeys; ++i) {
            table.put(Slice::from(key_at(i)), Slice::from("value-" + std::to_string(i)));
            highest.store(i, std::memory_order_release);
        }
        writing.store(false, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&] {
            while (writing.load(std::memory_order_acquire)) {
                const int limit = highest.load(std::memory_order_acquire);
                // Everything the writer has published must be readable, whole.
                for (int i = 0; i <= limit; i += 97) {
                    auto entry = table.get(Slice::from(key_at(i)));
                    ASSERT_TRUE(entry.has_value()) << i;
                    ASSERT_EQ(entry->value.to_string(), "value-" + std::to_string(i));
                }
                // And iteration must never see a half-built node.
                for (auto it = table.ascending(); it->valid(); it->next()) {
                    ASSERT_FALSE(it->key().empty());
                    ASSERT_EQ(it->value().to_string().rfind("value-", 0), 0u);
                }
            }
        });
    }

    writer.join();
    for (auto& reader : readers) reader.join();

    for (int i = 0; i < kKeys; ++i) {
        ASSERT_TRUE(table.get(Slice::from(key_at(i))).has_value()) << i;
    }
}

}  // namespace
}  // namespace elysiumkv
