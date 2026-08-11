#include "sst/sst_reader.hpp"
#include "sst/sst_writer.hpp"

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
