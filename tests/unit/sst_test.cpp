#include "sst/sst_reader.hpp"
#include "sst/sst_writer.hpp"

#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

using test::TempDir;

struct Entry {
    std::string key;
    ValueType type;
    std::string value;
};

std::vector<Entry> sample(int count, size_t value_size = 32) {
    std::vector<Entry> entries;
    entries.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "user:%08d", i);
        const bool tombstone = i % 11 == 0;
        entries.push_back({key, tombstone ? ValueType::Delete : ValueType::Put,
                           tombstone ? "" : std::string(value_size, static_cast<char>('a' + i % 26))});
    }
    return entries;
}

class SstTest : public ::testing::TestWithParam<Compression> {
protected:
    static constexpr std::string_view kName = "000000000001.sst";

    SstBuildResult write(const std::vector<Entry>& entries, SstOptions options = {}) {
        options.compression = GetParam();
        SstWriter writer(options);
        for (const Entry& e : entries) writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
        auto built = writer.finish();
        EXPECT_TRUE(built.has_value());
        EXPECT_EQ(store_.put(kName, Slice::from(built->bytes)).get(), Status::Ok);
        file_size_ = built->bytes.size();
        return std::move(*built);
    }

    std::unique_ptr<SstReader> open(size_t block_bytes = 4096) {
        auto reader = SstReader::open(store_, std::string(kName), file_size_,
                                      {.block_bytes = block_bytes});
        EXPECT_TRUE(reader.has_value())
            << (reader.has_value() ? "" : status_name(reader.error()));
        return reader.has_value() ? std::move(*reader) : nullptr;
    }

    TempDir dir_;
    DiskBlobStore store_{dir_.path()};
    uint64_t file_size_ = 0;
};


/// **Opening a reader must not fetch the bloom filter.** Only `get` consults it, and a compaction
/// opens a reader per input — so an eager load was a third round trip and, at ten bits per key,
/// about 1.25 MB per million entries transferred and thrown away on every merge. Against a remote
/// store the round trip is the expensive half.
TEST(SstFilterTest, OpeningAReaderDoesNotFetchTheFilter) {
    TempDir dir;
    auto disk = std::make_shared<DiskBlobStore>(dir.path());
    test::FaultInjectingBlobStore store(disk);

    SstWriter writer({.bloom_bits_per_key = 10, .compression = Compression::None});
    for (const Entry& e : sample(2000)) {
        writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
    }
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(store.put("000000000001.sst", Slice::from(built->bytes)).get(), Status::Ok);

    const uint64_t before_open = store.call_count(test::FaultInjectingBlobStore::Op::Get);
    auto reader = SstReader::open(store, "000000000001.sst", built->bytes.size(), {});
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());

    const uint64_t at_open = store.call_count(test::FaultInjectingBlobStore::Op::Get);
    EXPECT_EQ(at_open - before_open, 2u)
        << "the footer tail and the index block, and nothing else";

    // And it is still there when something actually asks: lazy, not dropped.
    auto found = (*reader)->get(Slice::from(std::string("user:00000042")));
    ASSERT_TRUE(found.has_value()) << status_name(found.error());
    EXPECT_TRUE(found->has_value()) << "a key that was written must still be found";
    EXPECT_GT(store.call_count(test::FaultInjectingBlobStore::Op::Get), at_open);
}

TEST_P(SstTest, PointLookupsFindEveryEntry) {
    const auto entries = sample(2000);
    const auto built = write(entries);
    EXPECT_EQ(built.num_entries, entries.size());
    EXPECT_EQ(built.smallest_key, entries.front().key);
    EXPECT_EQ(built.largest_key, entries.back().key);

    auto reader = open();
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->num_entries(), entries.size());

    for (const Entry& e : entries) {
        auto found = reader->get(Slice::from(e.key));
        ASSERT_TRUE(found.has_value()) << e.key;
        ASSERT_TRUE(found->has_value()) << e.key;
        EXPECT_EQ((*found)->type, e.type) << e.key;
        EXPECT_EQ((*found)->value.to_string(), e.value) << e.key;
    }
}

TEST_P(SstTest, AbsentKeysAreReportedAbsent) {
    write(sample(500));
    auto reader = open();
    ASSERT_NE(reader, nullptr);

    for (std::string_view key : {"", "aaa", "user:", "user:00000000x", "zzzz"}) {
        auto found = reader->get(Slice::from(key));
        ASSERT_TRUE(found.has_value()) << key;
        EXPECT_FALSE(found->has_value()) << key;
    }
}

TEST_P(SstTest, IterationVisitsEveryEntryIncludingTombstones) {
    const auto entries = sample(2000);
    write(entries);
    auto reader = open();
    ASSERT_NE(reader, nullptr);

    auto it = reader->iterator();
    size_t i = 0;
    for (it->seek_to_first(); it->valid(); it->next(), ++i) {
        ASSERT_LT(i, entries.size());
        EXPECT_EQ(it->key().to_string(), entries[i].key);
        EXPECT_EQ(it->type(), entries[i].type);
        EXPECT_EQ(it->value().to_string(), entries[i].value);
    }
    EXPECT_EQ(i, entries.size());
    EXPECT_EQ(it->status(), Status::Ok);
}

TEST_P(SstTest, SeekCrossesBlockBoundaries) {
    const auto entries = sample(2000);
    write(entries);
    auto reader = open();
    ASSERT_NE(reader, nullptr);

    auto it = reader->iterator();
    for (size_t i = 0; i < entries.size(); i += 37) {
        it->seek(Slice::from(entries[i].key));
        ASSERT_TRUE(it->valid()) << entries[i].key;
        EXPECT_EQ(it->key().to_string(), entries[i].key);

        // And keeps going into the next block.
        it->next();
        if (i + 1 < entries.size()) {
            ASSERT_TRUE(it->valid());
            EXPECT_EQ(it->key().to_string(), entries[i + 1].key);
        }
    }

    it->seek(Slice::from(std::string("zzz")));
    EXPECT_FALSE(it->valid());
    EXPECT_EQ(it->status(), Status::Ok);
}

TEST_P(SstTest, ManySmallBlocks) {
    // 64-byte blocks force one entry per block: the index and the two-level
    // iterator carry the whole file.
    const auto entries = sample(200);
    write(entries, {.block_bytes = 64});
    auto reader = open(64);
    ASSERT_NE(reader, nullptr);

    auto it = reader->iterator();
    size_t i = 0;
    for (it->seek_to_first(); it->valid(); it->next(), ++i) {
        EXPECT_EQ(it->key().to_string(), entries[i].key);
    }
    EXPECT_EQ(i, entries.size());
}

TEST_P(SstTest, SingleEntryFile) {
    write({{"only", ValueType::Put, "value"}});
    auto reader = open();
    ASSERT_NE(reader, nullptr);

    auto found = reader->get(Slice::from(std::string("only")));
    ASSERT_TRUE(found.has_value());
    ASSERT_TRUE(found->has_value());
    EXPECT_EQ((*found)->value.to_string(), "value");

    auto it = reader->iterator();
    it->seek_to_first();
    ASSERT_TRUE(it->valid());
    it->next();
    EXPECT_FALSE(it->valid());
}

TEST_P(SstTest, LargeValuesSpanBlocks) {
    std::vector<Entry> entries;
    for (int i = 0; i < 20; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "big:%04d", i);
        entries.push_back({key, ValueType::Put, std::string(100 * 1024, 'v')});
    }
    write(entries);
    auto reader = open();
    ASSERT_NE(reader, nullptr);

    auto found = reader->get(Slice::from(entries[7].key));
    ASSERT_TRUE(found.has_value());
    ASSERT_TRUE(found->has_value());
    EXPECT_EQ((*found)->value.size(), entries[7].value.size());
}

// A pinned value outlives the reader that produced it: the shared_ptr is the pin.
TEST_P(SstTest, PinnedValuesOutliveTheirReader) {
    write(sample(100));
    std::shared_ptr<const Block> pin;
    std::string value;
    {
        auto reader = open();
        ASSERT_NE(reader, nullptr);
        auto found = reader->get(Slice::from(std::string("user:00000001")));
        ASSERT_TRUE(found.has_value());
        ASSERT_TRUE(found->has_value());
        pin = (*found)->block;
        value = (*found)->value.to_string();
    }
    EXPECT_FALSE(value.empty());
    EXPECT_GT(pin->size(), 0u);
}

// --- range tombstones ----------------------------------------------------------

/* A range tombstone rides in its own block and is addressed by seek, not by scanning.
 *
 * **The file's own entries are deliberately unaffected by its own tombstone.** That rule is what
 * makes range deletes implementable without sequence numbers: within one file there is no ordering
 * to appeal to, so a tombstone shadows everything strictly older in `(level, file_number)` order
 * and nothing beside it. `range_deletes` answers only the shadowing question; the caller asks it
 * after finding no point entry here.
 */
TEST_P(SstTest, ARangeTombstoneCoversItsHalfOpenIntervalAndNothingElse) {
    SstOptions options;
    options.compression = GetParam();
    SstWriter writer(options);
    for (const Entry& e : sample(40)) writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
    writer.add_range_tombstone(Slice::from("user:00000010"), Slice::from("user:00000020"));
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(store_.put(kName, Slice::from(built->bytes)).get(), Status::Ok);
    file_size_ = built->bytes.size();

    auto reader = open();
    ASSERT_NE(reader, nullptr);
    ASSERT_TRUE(reader->has_range_tombstones());

    EXPECT_FALSE(*reader->range_deletes(Slice::from("user:00000009")));
    EXPECT_TRUE(*reader->range_deletes(Slice::from("user:00000010"))) << "lower bound is included";
    EXPECT_TRUE(*reader->range_deletes(Slice::from("user:00000015")));
    EXPECT_FALSE(*reader->range_deletes(Slice::from("user:00000020"))) << "upper bound is excluded";
    EXPECT_FALSE(*reader->range_deletes(Slice::from("user:00000021")));

    // The entries themselves are still there: shadowing is a question about older files.
    auto found = reader->get(Slice::from("user:00000015"));
    ASSERT_TRUE(found.has_value());
    EXPECT_TRUE(found->has_value()) << "the file's own entry survives the file's own tombstone";
}

TEST_P(SstTest, SeveralRangeTombstonesAreEachAddressable) {
    SstOptions options;
    options.compression = GetParam();
    SstWriter writer(options);
    for (const Entry& e : sample(40)) writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
    writer.add_range_tombstone(Slice::from("a"), Slice::from("b"));
    writer.add_range_tombstone(Slice::from("m"), Slice::from("n"));
    writer.add_range_tombstone(Slice::from("y"), Slice::from("z"));
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(store_.put(kName, Slice::from(built->bytes)).get(), Status::Ok);
    file_size_ = built->bytes.size();

    auto reader = open();
    ASSERT_NE(reader, nullptr);
    for (const char* covered : {"a", "aa", "m", "mm", "y", "yy"}) {
        EXPECT_TRUE(*reader->range_deletes(Slice::from(covered))) << covered;
    }
    for (const char* clear : {"", "b", "l", "n", "x", "z", "zz"}) {
        EXPECT_FALSE(*reader->range_deletes(Slice::from(clear))) << clear;
    }

    auto listed = reader->range_tombstones();
    ASSERT_TRUE(listed.has_value());
    ASSERT_EQ(listed->size(), 3u);
    EXPECT_EQ((*listed)[0].lower, "a");
    EXPECT_EQ((*listed)[2].upper, "z");
}

/* **A file with no range tombstones stays format v1.** The version is per file precisely so that
 * adding this feature does not reformat a keyspace nobody deleted from — and so that a reader that
 * predates range tombstones keeps reading those files, while refusing exactly the ones whose keys
 * it would otherwise report as present.
 */
TEST_P(SstTest, AFileWithoutRangeTombstonesKeepsTheOlderFormatVersion) {
    const SstBuildResult plain = write(sample(20));
    EXPECT_EQ(plain.num_range_tombstones, 0u);
    auto reader = open();
    ASSERT_NE(reader, nullptr);
    EXPECT_FALSE(reader->has_range_tombstones());
    EXPECT_FALSE(*reader->range_deletes(Slice::from("user:00000005")))
        << "a file with no tombstone block shadows nothing";
}

/* The covered span is reported separately from the data span, because it is not bounded by it: a
 * file can delete a range it holds no keys in, and a read path that consulted only `smallest_key`
 * and `largest_key` would walk past the tombstone that answers its query.
 */
TEST_P(SstTest, TheCoveredSpanIsReportedEvenWhereTheFileHoldsNoSuchKeys) {
    SstOptions options;
    options.compression = GetParam();
    SstWriter writer(options);
    writer.add(Slice::from("user:00000000"), ValueType::Put, Slice::from("v"));
    writer.add_range_tombstone(Slice::from("zzz:aaa"), Slice::from("zzz:bbb"));
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());

    EXPECT_EQ(built->largest_key, "user:00000000");
    EXPECT_EQ(built->smallest_range_key, "zzz:aaa");
    EXPECT_EQ(built->largest_range_key, "zzz:bbb")
        << "the file deletes a range it holds no keys in, and must say so";
}

INSTANTIATE_TEST_SUITE_P(Codecs, SstTest,
                         ::testing::Values(Compression::None, Compression::Lz4,
                                           Compression::Zstd),
                         // Not `info`: gtest's own macro expansion declares a
                         // parameter by that name, and -Wshadow is an error.
                         [](const auto& codec) {
                             switch (codec.param) {
                                 case Compression::None: return "None";
                                 case Compression::Lz4: return "Lz4";
                                 case Compression::Zstd: return "Zstd";
                             }
                             return "Unknown";
                         });

// --- corruption, independent of codec ----------------------------------------

class SstCorruptionTest : public ::testing::Test {
protected:
    static constexpr std::string_view kName = "000000000001.sst";

    std::string build(int entries = 500) {
        SstWriter writer({.compression = Compression::Zstd});
        for (const Entry& e : sample(entries)) {
            writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
        }
        auto built = writer.finish();
        EXPECT_TRUE(built.has_value());
        return std::move(built->bytes);
    }

    Result<std::unique_ptr<SstReader>> open_bytes(const std::string& bytes) {
        EXPECT_EQ(store_.put(kName, Slice::from(bytes)).get(), Status::Ok);
        return SstReader::open(store_, std::string(kName), bytes.size(), {});
    }

    TempDir dir_;
    DiskBlobStore store_{dir_.path()};
};

TEST_F(SstCorruptionTest, TruncatedFileFailsToOpen) {
    std::string bytes = build();
    bytes.resize(bytes.size() / 2);
    auto reader = open_bytes(bytes);
    ASSERT_FALSE(reader.has_value());
    EXPECT_EQ(reader.error(), Status::Corrupt);
}

TEST_F(SstCorruptionTest, DamagedFooterFailsToOpen) {
    std::string bytes = build();
    bytes[bytes.size() - 3] = static_cast<char>(bytes[bytes.size() - 3] ^ 0xFF);
    auto reader = open_bytes(bytes);
    ASSERT_FALSE(reader.has_value());
    EXPECT_EQ(reader.error(), Status::Corrupt);
}

// A damaged data block is detected by CRC on read, never silently returned.
TEST_F(SstCorruptionTest, DamagedDataBlockIsDetectedOnRead) {
    std::string bytes = build();
    bytes[64] = static_cast<char>(bytes[64] ^ 0xFF);

    auto reader = open_bytes(bytes);
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());

    bool saw_corrupt = false;
    for (const Entry& e : sample(500)) {
        auto found = (*reader)->get(Slice::from(e.key));
        if (!found.has_value()) {
            EXPECT_EQ(found.error(), Status::Corrupt);
            saw_corrupt = true;
            break;
        }
    }
    EXPECT_TRUE(saw_corrupt) << "a flipped byte in a data block must surface as Corrupt";
}

TEST_F(SstCorruptionTest, AnEmptyFileIsNotAnSst) {
    auto reader = open_bytes("");
    ASSERT_FALSE(reader.has_value());
    EXPECT_EQ(reader.error(), Status::Corrupt);
}

TEST_F(SstCorruptionTest, AMissingFileIsNotFoundNotCorrupt) {
    auto reader = SstReader::open(store_, "000000000099.sst", 1000, {});
    ASSERT_FALSE(reader.has_value());
    EXPECT_EQ(reader.error(), Status::NotFound);
}

}  // namespace
}  // namespace elysiumkv
