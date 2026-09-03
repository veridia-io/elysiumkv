#include "sst/sst_reader.hpp"
#include "sst/footer.hpp"
#include "sst/sst_writer.hpp"

#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <limits>
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


/// The reader's ceiling has to admit every block the writer may emit, and a block is not one entry.
/// `SstWriter::add` appends and only then asks whether the block is full, so a block can hold
/// nearly `block_bytes` of earlier entries *plus* one entry at the write-side limits. A ceiling
/// budgeting only the entry accepts the write and refuses the read.
TEST(SstBoundsTest, ABlockCarryingAMaximalEntryAfterOthersIsStillReadable) {
    TempDir dir;
    DiskBlobStore store(dir.path());

    SstWriter writer({.block_bytes = 4096, .compression = Compression::None});
    for (int i = 0; i < 40; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "aaa:%08d", i);
        writer.add(Slice::from(std::string(key)), ValueType::Put,
                   Slice::from(std::string(64, 'v')));
    }
    // At the limits `DbImpl::put` enforces, so this entry is one the engine accepts.
    const std::string big_key = "zzz" + std::string(kMaxKeyBytes - 3, 'k');
    const std::string big_value(kMaxValueBytes, 'V');
    writer.add(Slice::from(big_key), ValueType::Put, Slice::from(big_value));

    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(store.put("000000000001.sst", Slice::from(built->bytes)).get(), Status::Ok);

    auto reader =
        SstReader::open(store, "000000000001.sst", built->bytes.size(), {.block_bytes = 4096});
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());
    auto found = (*reader)->get(Slice::from(big_key));
    ASSERT_TRUE(found.has_value()) << "the block the writer emitted was refused: "
                                   << status_name(found.error());
    ASSERT_TRUE(found->has_value());
    EXPECT_EQ((*found)->value.size(), big_value.size());
}

/// A filter is sized by the file's key count and its bits per key, so it is unrelated to
/// `block_bytes` and can be larger than any block is allowed to be. Bounding it as though it were
/// a block makes every point lookup on such a file report corruption, while iteration — which
/// never consults it — keeps working.
TEST(SstBoundsTest, AFilterLargerThanAnyBlockIsStillReadable) {
    TempDir dir;
    DiskBlobStore store(dir.path());

    SstWriter writer(
        {.block_bytes = 4096, .bloom_bits_per_key = 2000, .compression = Compression::None});
    std::vector<std::string> keys;
    for (int i = 0; i < 6000; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "user:%08d", i);
        keys.emplace_back(key);
        writer.add(Slice::from(keys.back()), ValueType::Put, Slice::from(std::string(8, 'v')));
    }
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(store.put("000000000001.sst", Slice::from(built->bytes)).get(), Status::Ok);

    auto reader =
        SstReader::open(store, "000000000001.sst", built->bytes.size(), {.block_bytes = 4096});
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());
    // The premise: without this the ceiling under test is never crossed, and the case below would
    // pass against a reader that still bounded the filter as though it were a block. Read from the
    // file rather than recomputed, so it cannot agree with the builder by sharing its arithmetic.
    const Slice tail(reinterpret_cast<const uint8_t*>(built->bytes.data()) + built->bytes.size() -
                         Footer::kMaxFooterLength,
                     Footer::kMaxFooterLength);
    auto footer = Footer::decode(tail);
    ASSERT_TRUE(footer.has_value());
    ASSERT_GT(footer->filter.length, kMaxEntryBlockBytes + 4096u)
        << "the filter is too small to exercise the ceiling";
    auto found = (*reader)->get(Slice::from(keys[1234]));
    ASSERT_TRUE(found.has_value()) << "a point lookup could not read the filter: "
                                   << status_name(found.error());
    EXPECT_TRUE(found->has_value());
}

/// A v1 or v2 footer carries no checksum, so its handles are whatever the bytes say. `offset +
/// length` is therefore corruption-controlled, and the sum wraps: a wrapped sum passes an unsigned
/// comparison against the file size while naming bytes nowhere near the file.
///
/// **This case does not discriminate in an ordinary build, and that was checked rather than
/// assumed.** With the overflow guard removed it still passes: the read lands on unrelated memory,
/// fails its checksum, and is reported as corruption — the right answer reached by undefined
/// behaviour. It is the sanitizer builds that make it a test, where the same read traps. Left here
/// because those run the whole suite in CI, and labelled because a green run of the debug preset is
/// not evidence for the property in the name.
TEST(SstBoundsTest, AFooterHandleThatWrapsIsRefused) {
    TempDir dir;
    DiskBlobStore store(dir.path());

    SstWriter writer({.compression = Compression::None});
    for (const Entry& e : sample(50)) writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());

    // A hand-built v1 footer whose index handle wraps when its two fields are added.
    Footer wrapped;
    wrapped.format_version = Footer::kFormatVersion1;
    wrapped.filter.offset = 0;
    wrapped.filter.length = 0;
    wrapped.index.offset = std::numeric_limits<uint64_t>::max() - 8;
    wrapped.index.length = 16;
    wrapped.num_entries = 1;

    std::string bytes = built->bytes;
    bytes.resize(bytes.size() - static_cast<size_t>(Footer::kFooterLengthV3));
    bytes += wrapped.encode();
    ASSERT_EQ(store.put("000000000002.sst", Slice::from(bytes)).get(), Status::Ok);

    auto reader = SstReader::open(store, "000000000002.sst", bytes.size(), {});
    EXPECT_FALSE(reader.has_value()) << "a handle naming bytes outside the file was accepted";
}

/// Opening a reader must cost one round trip: the footer and the index are adjacent at the end
/// (FORMAT.md §5) and are taken in one speculative tail read, and the bloom filter is loaded
/// lazily because only `get` consults it while a compaction opens a reader per input — an eager
/// load moves about 1.25 MB per million entries and throws it away.
///
/// Asserted as a count rather than a timing, since that is what holds on every machine. Against a
/// remote store each extra read is a round trip, and `reader_cache_bytes` is sized generously
/// because a reader eviction pays them again.
TEST(SstFilterTest, OpeningAReaderCostsOneRoundTrip) {
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
    EXPECT_EQ(at_open - before_open, 1u)
        << "one tail read carrying the footer and the index, and nothing else";

    // And it is still there when something actually asks: lazy, not dropped.
    auto found = (*reader)->get(Slice::from(std::string("user:00000042")));
    ASSERT_TRUE(found.has_value()) << status_name(found.error());
    EXPECT_TRUE(found->has_value()) << "a key that was written must still be found";
    EXPECT_GT(store.call_count(test::FaultInjectingBlobStore::Op::Get), at_open);
}


/// The tombstone list is decoded once per reader: it is asked for once per carrying file per
/// iterator construction and once per input per compaction, and decoding per call fetches the block
/// and rebuilds two strings per tombstone. A file is immutable, so its tombstones are too.
TEST(SstFilterTest, TheRangeTombstoneListIsDecodedOnce) {
    TempDir dir;
    auto disk = std::make_shared<DiskBlobStore>(dir.path());
    test::FaultInjectingBlobStore store(disk);

    SstWriter writer({.bloom_bits_per_key = 10, .compression = Compression::None});
    for (const Entry& e : sample(500)) {
        writer.add(Slice::from(e.key), e.type, Slice::from(e.value));
    }
    for (int i = 0; i < 20; ++i) {
        char lower[32];
        char upper[32];
        std::snprintf(lower, sizeof(lower), "zz:%06d", i * 10);
        std::snprintf(upper, sizeof(upper), "zz:%06d", i * 10 + 5);
        writer.add_range_tombstone(Slice::from(std::string_view(lower)),
                                   Slice::from(std::string_view(upper)));
    }
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());
    ASSERT_EQ(built->num_range_tombstones, 20u);
    ASSERT_EQ(store.put("000000000002.sst", Slice::from(built->bytes)).get(), Status::Ok);

    auto reader = SstReader::open(store, "000000000002.sst", built->bytes.size(), {});
    ASSERT_TRUE(reader.has_value()) << status_name(reader.error());

    auto first = (*reader)->range_tombstones();
    ASSERT_TRUE(first.has_value()) << status_name(first.error());
    EXPECT_EQ(first->size(), 20u);

    const uint64_t after_first = store.call_count(test::FaultInjectingBlobStore::Op::Get);
    auto again = (*reader)->range_tombstones();
    ASSERT_TRUE(again.has_value()) << status_name(again.error());
    ASSERT_EQ(again->size(), first->size()) << "the same reader must answer the same thing";
    for (size_t i = 0; i < first->size(); ++i) {
        EXPECT_EQ((*again)[i].lower, (*first)[i].lower);
        EXPECT_EQ((*again)[i].upper, (*first)[i].upper);
    }
    EXPECT_EQ(store.call_count(test::FaultInjectingBlobStore::Op::Get), after_first)
        << "the second ask re-read the block";
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
 * The file's own entries are deliberately unaffected by its own tombstone. That rule is what
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

/* A file with no range tombstones stays format v1. The version is per file precisely so that
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
