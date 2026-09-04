#include "sst/block_builder.hpp"
#include "sst/bloom.hpp"
#include "sst/compression.hpp"
#include "sst/crc32c.hpp"
#include "sst/footer.hpp"
#include "sst/format.hpp"
#include "sst/sst_writer.hpp"
#include "version/manifest_payload.hpp"
#include "version/version_edit.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>
#include <vector>

/// Pins every layout FORMAT.md declares, against the encoders' actual output rather than
/// against the constants they were written from. A test that asserts `kFooterLengthV1 == 44` and
/// nothing else proves only that a constant was not edited; these assert the bytes.
///
/// Any change here is a format change: bump the version, teach the reader both shapes, and update
/// FORMAT.md. A failing test in this file is not a test to fix.
namespace elysiumkv::test {
namespace {

// --- helpers -----------------------------------------------------------------

uint32_t read_u32(const std::string& bytes, size_t offset) {
    return decode_fixed32(reinterpret_cast<const uint8_t*>(bytes.data()) + offset);
}

uint64_t read_u64(const std::string& bytes, size_t offset) {
    return decode_fixed64(reinterpret_cast<const uint8_t*>(bytes.data()) + offset);
}

/// Reads a varint the way FORMAT.md describes it, independently of the engine's own decoder — so a
/// bug in `get_varint64` cannot make a wrong layout look right.
uint64_t take_varint(const std::string& bytes, size_t& offset) {
    uint64_t value = 0;
    int shift = 0;
    while (true) {
        const auto byte = static_cast<uint8_t>(bytes[offset++]);
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    return value;
}

std::string take_string(const std::string& bytes, size_t& offset) {
    const uint64_t length = take_varint(bytes, offset);
    std::string out = bytes.substr(offset, length);
    offset += length;
    return out;
}

// --- FORMAT.md §5, footer --------------------------------------------------------------

TEST(WireFormat, FooterFieldsSitAtTheDocumentedOffsets) {
    Footer footer;
    footer.filter.offset = 0x1122334455667788ull;
    footer.filter.length = 0x99AABBCCu;
    footer.index.offset = 0x0102030405060708ull;
    footer.index.length = 0x090A0B0Cu;
    footer.num_entries = 0xF0E0D0C0B0A09080ull;

    const std::string encoded = footer.encode();
    ASSERT_EQ(encoded.size(), 44u) << "FORMAT.md declares a 44-byte footer";
    EXPECT_EQ(encoded.size(), static_cast<size_t>(Footer::kFooterLengthV1));

    EXPECT_EQ(read_u64(encoded, 0), footer.filter.offset);
    EXPECT_EQ(read_u32(encoded, 8), footer.filter.length);
    EXPECT_EQ(read_u64(encoded, 12), footer.index.offset);
    EXPECT_EQ(read_u32(encoded, 20), footer.index.length);
    EXPECT_EQ(read_u64(encoded, 24), footer.num_entries);
    EXPECT_EQ(read_u32(encoded, 32), 1u) << "format_version";
    EXPECT_EQ(read_u64(encoded, 36), 0x454C595349554D31ull) << "magic";
}

TEST(WireFormat, TheMagicSpellsElysium1) {
    // Documented as ASCII, so assert the characters rather than repeating the hex constant.
    const std::string expected = "ELYSIUM1";
    std::string actual;
    for (int i = 7; i >= 0; --i) {
        actual.push_back(static_cast<char>((Footer::kMagic >> (8 * i)) & 0xFF));
    }
    EXPECT_EQ(actual, expected);
}

TEST(WireFormat, TheInvariantTrailerIsTheLastTwelveBytesAndIsReadableAlone) {
    EXPECT_EQ(Footer::kTrailerLength, 12);

    const std::string encoded = Footer{}.encode();
    // Exactly the trailer, nothing before it: a reader must be able to learn the footer width from
    // these 12 bytes alone.
    const std::string trailer = encoded.substr(encoded.size() - 12);
    auto width = Footer::footer_length_from_trailer(Slice::from(trailer));
    ASSERT_TRUE(width.has_value()) << "the trailer must be self-sufficient";
    EXPECT_EQ(*width, Footer::kFooterLengthV1);

    // And the version precedes the magic within it.
    EXPECT_EQ(read_u32(trailer, 0), 1u);
    EXPECT_EQ(read_u64(trailer, 4), Footer::kMagic);
}

TEST(WireFormat, CurrentFooterVersionMatchesTheDocument) {
    SstWriter writer({.compression = Compression::None});
    writer.add(Slice::from(std::string("a")), ValueType::Put, Slice::from(std::string("v")));
    auto built = writer.finish();
    ASSERT_TRUE(built.has_value());

    const std::string encoded = built->bytes.substr(built->bytes.size() - 60);
    ASSERT_EQ(encoded.size(), 60u) << "FORMAT.md declares a 60-byte v3 footer";
    EXPECT_EQ(read_u64(encoded, 32), 0u) << "range_del offset";
    EXPECT_EQ(read_u32(encoded, 40), 0u) << "range_del length";
    EXPECT_EQ(read_u32(encoded, 44), crc32c(std::string_view(encoded.data(), 44))) << "footer CRC";
    EXPECT_EQ(read_u32(encoded, 48), Footer::kFormatVersion3);
    EXPECT_EQ(read_u64(encoded, 52), Footer::kMagic);
}

// --- FORMAT.md §2, block framing -------------------------------------------------------

TEST(WireFormat, BlockFramingIsPayloadThenLengthThenCodecThenCrc) {
    const std::string payload = "the-block-payload";
    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(payload), Compression::None, framed), Status::Ok);

    ASSERT_EQ(framed.size(), payload.size() + kBlockTrailerLength);
    EXPECT_EQ(kBlockTrailerLength, 9u);
    EXPECT_EQ(framed.compare(0, payload.size(), payload), 0) << "payload comes first, verbatim";

    EXPECT_EQ(read_u32(framed, payload.size()), payload.size()) << "uncompressed_len";
    EXPECT_EQ(static_cast<uint8_t>(framed[payload.size() + 4]), 0u) << "compression_type = none";

    // The CRC covers payload ‖ len ‖ type — everything except itself.
    const size_t covered = framed.size() - 4;
    const uint32_t expected =
        crc32c(reinterpret_cast<const uint8_t*>(framed.data()), covered);
    EXPECT_EQ(read_u32(framed, covered), expected);
}

TEST(WireFormat, ManifestLevelsAreBoundedBeforeNarrowing) {
    VersionEdit added;
    added.added.push_back(FileMetadata{.level = -1, .file_number = 1});
    VersionEdit deleted;
    deleted.deleted.push_back({-1, 1});
    VersionEdit pointer;
    pointer.compaction_pointers.emplace_back(-1, "key");
    for (const VersionEdit* edit : {&added, &deleted, &pointer}) {
        auto decoded = decode_version_edit(Slice::from(encode_version_edit(*edit)));
        ASSERT_FALSE(decoded.has_value());
        EXPECT_EQ(decoded.error(), Status::Corrupt);
    }

    VersionSnapshot snapshot;
    snapshot.files.push_back(FileMetadata{.level = -1, .file_number = 1});
    auto decoded = decode_version_snapshot(Slice::from(encode_version_snapshot(snapshot)));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error(), Status::Corrupt);

    auto content = unframe_block(Slice::from(encode_version_edit(added)), 1u << 20);
    ASSERT_TRUE(content.has_value());
    const std::array<uint8_t, 5> level_2_31 = {0x80, 0x80, 0x80, 0x80, 0x08};
    content->erase(content->begin() + 3, content->begin() + 13);
    content->insert(content->begin() + 3, level_2_31.begin(), level_2_31.end());
    std::string framed;
    ASSERT_EQ(frame_block(Slice(content->data(), content->size()), Compression::None, framed),
              Status::Ok);
    auto decoded_edit = decode_version_edit(Slice::from(framed));
    ASSERT_FALSE(decoded_edit.has_value());
    EXPECT_EQ(decoded_edit.error(), Status::Corrupt);
}

TEST(WireFormat, ABlockThatCompressionDoesNotShrinkIsStoredRawWithCodecNone) {
    // Incompressible, so zstd cannot win. FORMAT.md says the codec byte is per block, and this is
    // the case that makes that true rather than a technicality.
    std::string payload;
    uint64_t state = 0x9E3779B97F4A7C15ull;
    for (int i = 0; i < 512; ++i) {
        state ^= state >> 30;
        state *= 0xBF58476D1CE4E5B9ull;
        payload.push_back(static_cast<char>(state >> 56));
    }

    std::string framed;
    ASSERT_EQ(frame_block(Slice::from(payload), Compression::Zstd, framed), Status::Ok);
    const size_t covered = framed.size() - 4;
    EXPECT_EQ(static_cast<uint8_t>(framed[covered - 1]), 0u)
        << "a codec that failed to shrink must store the block raw";
    EXPECT_EQ(read_u32(framed, covered - 5), payload.size());
}

// --- FORMAT.md §3, block content -------------------------------------------------------

TEST(WireFormat, EntryLayoutIsSharedUnsharedTypeLengthKeyValue) {
    BlockBuilder builder(16);
    builder.add(Slice::from(std::string("apple")), ValueType::Put, Slice::from(std::string("A")));
    builder.add(Slice::from(std::string("apply")), ValueType::Put, Slice::from(std::string("BB")));
    builder.add(Slice::from(std::string("banana")), ValueType::Delete, Slice());
    const std::string content = builder.finish().to_string();

    size_t at = 0;

    // First entry: nothing to share with an empty previous key.
    EXPECT_EQ(take_varint(content, at), 0u) << "shared_len";
    EXPECT_EQ(take_varint(content, at), 5u) << "unshared_len";
    EXPECT_EQ(static_cast<uint8_t>(content[at++]), 0x01u) << "value_type precedes value_len";
    EXPECT_EQ(take_varint(content, at), 1u) << "value_len";
    EXPECT_EQ(content.substr(at, 5), "apple");
    at += 5;
    EXPECT_EQ(content.substr(at, 1), "A");
    at += 1;

    // Second entry: "appl" is shared with "apple".
    EXPECT_EQ(take_varint(content, at), 4u) << "shared_len";
    EXPECT_EQ(take_varint(content, at), 1u) << "unshared_len";
    EXPECT_EQ(static_cast<uint8_t>(content[at++]), 0x01u);
    EXPECT_EQ(take_varint(content, at), 2u);
    EXPECT_EQ(content.substr(at, 1), "y");
    at += 1;
    EXPECT_EQ(content.substr(at, 2), "BB");
    at += 2;

    // Third entry: a delete carries value_len 0 and no value bytes.
    EXPECT_EQ(take_varint(content, at), 0u) << "no prefix shared with 'apply'";
    EXPECT_EQ(take_varint(content, at), 6u);
    EXPECT_EQ(static_cast<uint8_t>(content[at++]), 0x00u) << "delete";
    EXPECT_EQ(take_varint(content, at), 0u) << "value_len is 0 for a delete";
    EXPECT_EQ(content.substr(at, 6), "banana");
    at += 6;

    // Then the restart array and its count, as fixed32s at the very end.
    const uint32_t restart_count = read_u32(content, content.size() - 4);
    ASSERT_EQ(restart_count, 1u) << "one restart point at this interval";
    EXPECT_EQ(at, content.size() - 4 - restart_count * 4u)
        << "entries must end exactly where the restart array begins";
    EXPECT_EQ(read_u32(content, content.size() - 8), 0u) << "the first restart offset is 0";
}

TEST(WireFormat, ARestartPointStoresTheFullKey) {
    BlockBuilder builder(1);   // restart at every entry after the first
    builder.add(Slice::from(std::string("aaaa1")), ValueType::Put, Slice::from(std::string("x")));
    builder.add(Slice::from(std::string("aaaa2")), ValueType::Put, Slice::from(std::string("y")));
    const std::string content = builder.finish().to_string();

    const uint32_t restart_count = read_u32(content, content.size() - 4);
    ASSERT_EQ(restart_count, 2u);
    const uint32_t second = read_u32(content, content.size() - 8);

    size_t at = second;
    EXPECT_EQ(take_varint(content, at), 0u)
        << "shared_len is 0 at a restart point, which is what makes binary search possible";
    EXPECT_EQ(take_varint(content, at), 5u) << "the whole key follows";
}

// --- FORMAT.md §4, filter block --------------------------------------------------------

TEST(WireFormat, FilterTrailerIsBlockCountThenProbeCount) {
    BloomBuilder builder(10, 6);
    for (int i = 0; i < 200; ++i) builder.add(Slice::from("key:" + std::to_string(i)));
    const std::string filter = builder.finish();

    ASSERT_GT(filter.size(), 5u);
    const size_t bitmap_bytes = filter.size() - 5;
    const uint32_t num_blocks = read_u32(filter, bitmap_bytes);
    const auto num_probes = static_cast<uint8_t>(filter[filter.size() - 1]);

    EXPECT_EQ(num_probes, 6u);
    EXPECT_GT(num_blocks, 0u);
    EXPECT_EQ(bitmap_bytes, static_cast<size_t>(num_blocks) * 64u)
        << "FORMAT.md declares 64-byte (512-bit) bloom blocks";
}

TEST(WireFormat, AMalformedFilterIsTreatedAsMayContain) {
    // Never an authority on absence: a filter the reader cannot trust must fall through.
    EXPECT_TRUE(bloom_may_contain(Slice::from(std::string("")), Slice::from(std::string("k"))));
    std::string zero_blocks(64, '\0');
    put_fixed32(zero_blocks, 0u);            // num_blocks = 0
    zero_blocks.push_back('\x06');
    EXPECT_TRUE(bloom_may_contain(Slice::from(zero_blocks), Slice::from(std::string("k"))));
}

// --- FORMAT.md §6, manifest payloads and records ---------------------------------------

TEST(WireFormat, ManifestPayloadEnvelopeMatchesTheDocument) {
    const ProviderRegistry registry = passthrough_registry();
    const std::string plaintext = "x";
    auto sealed = ManifestPayload::seal(registry, 7, ManifestPayload::snapshot_address(7),
                                        Slice::from(plaintext));
    ASSERT_TRUE(sealed.has_value());
    ASSERT_EQ(sealed->size(), ManifestPayload::kHeaderBytes + plaintext.size());

    EXPECT_EQ(read_u32(*sealed, 0), 0x02564B45u) << "magic: EKV\\x02";
    EXPECT_EQ(static_cast<uint8_t>((*sealed)[4]), 1u) << "header_version low byte";
    EXPECT_EQ(static_cast<uint8_t>((*sealed)[5]), 0u) << "header_version high byte";
    EXPECT_EQ(static_cast<uint8_t>((*sealed)[6]), 0u) << "provider_len low byte";
    EXPECT_EQ(static_cast<uint8_t>((*sealed)[7]), 0u) << "provider_len high byte";
    EXPECT_EQ(read_u32(*sealed, 8), 0u) << "metadata_len";
    EXPECT_EQ(read_u32(*sealed, 12), 0u) << "codec";
    EXPECT_EQ(read_u64(*sealed, 16), plaintext.size()) << "plain_len";
    EXPECT_EQ(read_u64(*sealed, 24), plaintext.size()) << "packed_len";
    EXPECT_EQ(sealed->substr(ManifestPayload::kHeaderBytes), plaintext);
}

TEST(WireFormat, ManifestEditFieldOrderMatchesTheDocument) {
    FileMetadata file;
    file.level = 2;
    file.file_number = 4242;
    file.store_id = "store-1";
    file.smallest_key = "aaa";
    file.largest_key = "zzz";
    file.file_bytes = 123456;
    file.num_entries = 777;
    file.num_tombstones = 3;
    file.num_range_tombstones = 5;
    file.smallest_range_key = "rrr";
    file.largest_range_key = "sss";
    file.compression = Compression::Zstd;
    file.min_write_time_ms = 1'700'000'000'000ull;
    file.max_write_time_ms = 1'700'000'009'999ull;
    file.watermark = {80u, 100u};

    VersionEdit edit;
    edit.next_file_number = 4243;
    edit.added.push_back(file);
    edit.deleted.push_back(FileRef{1, 99});
    edit.compaction_pointers.emplace_back(1, std::string("mmm"));
    edit.truncation_point = "ttt";

    const std::string framed = encode_version_edit(edit);

    // An edit is framed, uncompressed, so the content is the front of the record.
    const size_t covered = framed.size() - 4;
    EXPECT_EQ(static_cast<uint8_t>(framed[covered - 1]), 0u) << "edits are stored uncompressed";
    const uint32_t content_len = read_u32(framed, covered - 5);
    const std::string content = framed.substr(0, content_len);

    size_t at = 0;
    EXPECT_EQ(take_varint(content, at), 6u) << "format_version";
    EXPECT_EQ(take_varint(content, at), 4243u) << "next_file_number";
    ASSERT_EQ(take_varint(content, at), 1u) << "added_count";

    EXPECT_EQ(take_varint(content, at), 2u) << "level";
    EXPECT_EQ(take_varint(content, at), 4242u) << "file_number";
    EXPECT_EQ(take_string(content, at), "store-1");
    EXPECT_EQ(take_string(content, at), "aaa");
    EXPECT_EQ(take_string(content, at), "zzz");
    EXPECT_EQ(take_varint(content, at), 123456u) << "file_bytes";
    EXPECT_EQ(take_varint(content, at), 777u) << "num_entries";
    EXPECT_EQ(take_varint(content, at), 3u) << "num_tombstones";
    EXPECT_EQ(take_varint(content, at), 5u) << "num_range_tombstones";
    EXPECT_EQ(take_string(content, at), "rrr") << "smallest_range_key";
    EXPECT_EQ(take_string(content, at), "sss") << "largest_range_key";
    EXPECT_EQ(take_varint(content, at), 2u) << "compression: zstd";
    EXPECT_EQ(take_varint(content, at), 1'700'000'000'000ull) << "min_write_time_ms";
    EXPECT_EQ(take_varint(content, at), 1'700'000'009'999ull) << "max_write_time_ms";
    EXPECT_EQ(take_varint(content, at), 3u) << "watermark flags: both bounds present";
    EXPECT_EQ(take_varint(content, at), 80u) << "watermark_low";
    EXPECT_EQ(take_varint(content, at), 100u) << "watermark_high";
    EXPECT_EQ(take_string(content, at), "") << "encryption_provider: passthrough";
    EXPECT_EQ(take_string(content, at), "") << "encryption_metadata: passthrough";

    ASSERT_EQ(take_varint(content, at), 1u) << "deleted_count";
    EXPECT_EQ(take_varint(content, at), 1u) << "deleted level";
    EXPECT_EQ(take_varint(content, at), 99u) << "deleted file_number";

    ASSERT_EQ(take_varint(content, at), 1u) << "compaction pointer count";
    EXPECT_EQ(take_varint(content, at), 1u) << "pointer level";
    EXPECT_EQ(take_string(content, at), "mmm");

    EXPECT_EQ(take_string(content, at), "ttt") << "truncation_point";
    EXPECT_EQ(take_varint(content, at), 0u) << "watermark floor state: none recorded";
    EXPECT_EQ(take_varint(content, at), 0u) << "watermark floor position";
    EXPECT_EQ(at, content.size()) << "no unaccounted bytes in the record";
}

/// An edit's instruction about the floor has three meanings, and they are not interchangeable:
/// almost every edit says nothing, a discard installs a floor, and the embedder declaring its
/// replay finished removes one. Collapsing "say nothing" into "clear" would have every ordinary
/// flush quietly discharge a loss.
TEST(WireFormat, TheFloorInstructionsThreeMeaningsRoundTrip) {
    struct Case {
        VersionEdit::FloorUpdate update;
        WatermarkFloor floor;
    };
    const Case cases[] = {
        {VersionEdit::FloorUpdate::Silent, {}},
        {VersionEdit::FloorUpdate::Set, WatermarkFloor{std::optional<uint64_t>(0)}},
        {VersionEdit::FloorUpdate::Set, WatermarkFloor{std::optional<uint64_t>(4242)}},
        {VersionEdit::FloorUpdate::Set, WatermarkFloor{std::nullopt}},
        {VersionEdit::FloorUpdate::Clear, {}},
    };

    for (const Case& c : cases) {
        VersionEdit edit;
        edit.next_file_number = 7;
        edit.floor_update = c.update;
        edit.watermark_floor = c.floor;
        auto decoded = decode_version_edit(Slice::from(encode_version_edit(edit)));
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->floor_update, c.update);
        if (c.update == VersionEdit::FloorUpdate::Set) {
            EXPECT_EQ(decoded->watermark_floor, c.floor);
        }
    }
}

/// The snapshot carries the resulting state, where zero is a valid position — so neither
/// "certifies nothing" nor "no loss recorded" can be inferred from the value.
TEST(WireFormat, TheWatermarkFloorsStatesRoundTripInASnapshot) {
    const std::optional<WatermarkFloor> states[] = {
        std::nullopt,
        WatermarkFloor{std::optional<uint64_t>(0)},
        WatermarkFloor{std::optional<uint64_t>(4242)},
        WatermarkFloor{std::nullopt},
    };
    for (const auto& floor : states) {
        VersionSnapshot snapshot;
        snapshot.next_file_number = 7;
        snapshot.watermark_floor = floor;
        auto back = decode_version_snapshot(Slice::from(encode_version_snapshot(snapshot)));
        ASSERT_TRUE(back.has_value());
        EXPECT_EQ(back->watermark_floor, floor);
    }
}

/// It never rises. A replay's own files are the youngest in the store and sit on the tier that
/// just failed, so crediting them would certify a position the next loss could destroy again. It
/// only ever comes down, and is removed in one step when the replay is declared complete.
TEST(WireFormat, TheWatermarkFloorOnlyEverFalls) {
    WatermarkFloor floor{std::optional<uint64_t>(140)};

    floor.lower_to(200);
    EXPECT_EQ(*floor.position, 140u) << "a loss above the floor tells us nothing new";
    floor.lower_to(80);
    EXPECT_EQ(*floor.position, 80u);
    floor.lower_to(std::nullopt);
    EXPECT_FALSE(floor.position.has_value()) << "nothing certifiable is absorbing";
    floor.lower_to(50);
    EXPECT_FALSE(floor.position.has_value());
}

TEST(WireFormat, ManifestSnapshotIsZstdFramedAndRoundTrips) {
    VersionSnapshot snapshot;
    snapshot.next_file_number = 9;
    snapshot.truncation_point = "cutoff";
    for (int i = 0; i < 40; ++i) {
        FileMetadata file;
        file.level = i % 3;
        file.file_number = static_cast<uint64_t>(i);
        file.store_id = "store-0";
        file.smallest_key = "key:" + std::to_string(i);
        file.largest_key = "key:" + std::to_string(i) + "~";
        file.compression = Compression::None;
        snapshot.files.push_back(file);
    }

    const std::string framed = encode_version_snapshot(snapshot);
    const size_t covered = framed.size() - 4;
    EXPECT_EQ(static_cast<uint8_t>(framed[covered - 1]),
              static_cast<uint8_t>(Compression::Zstd))
        << "a snapshot is read whole, so it is compressed whole";

    auto decoded = decode_version_snapshot(Slice::from(framed));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->next_file_number, 9u);
    EXPECT_EQ(decoded->truncation_point, "cutoff");
    ASSERT_EQ(decoded->files.size(), snapshot.files.size());
    EXPECT_EQ(decoded->files[7].smallest_key, snapshot.files[7].smallest_key);
}

// --- FORMAT.md §8, entry limits --------------------------------------------------------

TEST(WireFormat, DocumentedEntryLimits) {
    EXPECT_EQ(kMaxValueBytes, 1u << 20);
    EXPECT_EQ(kMaxKeyBytes, 64u << 10);
    EXPECT_GE(kMaxEntryBlockBytes, kMaxValueBytes + kMaxKeyBytes)
        << "the reader must accept the largest entry the writer may emit";
}

}  // namespace
}  // namespace elysiumkv::test
