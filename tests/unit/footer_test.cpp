#include "sst/footer.hpp"

#include "sst/format.hpp"

#include <gtest/gtest.h>

#include <string>

namespace elysiumkv {
namespace {

Footer sample_footer() {
    Footer footer;
    footer.filter = {.offset = 1234, .length = 567};
    footer.index = {.offset = 8901, .length = 234};
    footer.num_entries = 4242;
    return footer;
}

// ARCHITECTURE.md "The invariant trailer" — field widths are normative, and the test asserts against the encoder's
// actual output rather than trusting the constant.
TEST(Footer, EncodedLengthIsExactlyTheDeclaredWidth) {
    EXPECT_EQ(sample_footer().encode().size(), static_cast<size_t>(Footer::kFooterLengthV1));
    EXPECT_EQ(Footer::kFooterLengthV1, 44);
    EXPECT_EQ(Footer::kTrailerLength, 12);
}

TEST(Footer, RoundTrips) {
    const Footer original = sample_footer();
    const std::string encoded = original.encode();

    auto decoded = Footer::decode(Slice::from(encoded));
    ASSERT_TRUE(decoded.has_value()) << status_name(decoded.error());
    EXPECT_EQ(decoded->filter.offset, original.filter.offset);
    EXPECT_EQ(decoded->filter.length, original.filter.length);
    EXPECT_EQ(decoded->index.offset, original.index.offset);
    EXPECT_EQ(decoded->index.length, original.index.length);
    EXPECT_EQ(decoded->num_entries, original.num_entries);
    EXPECT_EQ(decoded->format_version, Footer::kFormatVersion1);
}

// The whole point of the invariant trailer: width is learned from the last 12
// bytes, which never move, so a future format version stays parseable.
TEST(Footer, WidthIsLearnedFromTheInvariantTrailerAlone) {
    const std::string encoded = sample_footer().encode();
    const std::string trailer = encoded.substr(encoded.size() - Footer::kTrailerLength);

    auto width = Footer::footer_length_from_trailer(Slice::from(trailer));
    ASSERT_TRUE(width.has_value());
    EXPECT_EQ(*width, Footer::kFooterLengthV1);

    // The magic is the last 8 bytes, the version the 4 before it.
    EXPECT_EQ(decode_fixed64(reinterpret_cast<const uint8_t*>(trailer.data()) + 4),
              Footer::kMagic);
    EXPECT_EQ(decode_fixed32(reinterpret_cast<const uint8_t*>(trailer.data())),
              Footer::kFormatVersion1);
}

TEST(Footer, RejectsABadMagic) {
    std::string encoded = sample_footer().encode();
    encoded[encoded.size() - 1] = static_cast<char>(encoded.back() ^ 0xFF);

    EXPECT_EQ(Footer::decode(Slice::from(encoded)).error(), Status::Corrupt);
    EXPECT_EQ(Footer::footer_length_from_trailer(Slice::from(encoded)).error(), Status::Corrupt);
}

// **`Unsupported`, not `Corrupt`.** The magic is eight bytes and is checked first, so reaching an
// unknown version means the file is intact and written by a newer build. Reporting damage there
// sends an operator to a restore they do not need; the remedy is a different binary.
TEST(Footer, AnUnknownFormatVersionIsUnsupportedNotCorrupt) {
    Footer footer = sample_footer();
    footer.format_version = 99;
    const std::string encoded = footer.encode();

    EXPECT_EQ(Footer::decode(Slice::from(encoded)).error(), Status::Unsupported);
    EXPECT_EQ(Footer::footer_length_from_trailer(Slice::from(encoded)).error(),
              Status::Unsupported);
}

// The CRC is the point of v3: damage in the footer used to produce plausible handles and fail at
// some block that was never the problem.
TEST(Footer, ADamagedFooterBodyIsCaughtByItsChecksum) {
    Footer footer = sample_footer();
    footer.format_version = Footer::kFormatVersion3;
    ASSERT_TRUE(Footer::decode(Slice::from(footer.encode())).has_value());

    // Every byte of the body, one at a time — a checksum that only caught some of them would pass
    // a test that flipped one convenient byte.
    const std::string good = footer.encode();
    for (size_t i = 0; i + Footer::kTrailerLength + 4 < good.size(); ++i) {
        std::string damaged = good;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x01);
        EXPECT_EQ(Footer::decode(Slice::from(damaged)).error(), Status::Corrupt) << "byte " << i;
    }
}

// v1 and v2 files predate the checksum and must still read: the version is per file, and a store
// written by an earlier build is full of them.
TEST(Footer, EarlierVersionsStillDecodeWithoutAChecksum) {
    for (uint32_t version : {Footer::kFormatVersion1, Footer::kFormatVersion2}) {
        Footer footer = sample_footer();
        footer.format_version = version;
        const auto decoded = Footer::decode(Slice::from(footer.encode()));
        ASSERT_TRUE(decoded.has_value()) << version;
        EXPECT_EQ(decoded->format_version, version);
        EXPECT_EQ(decoded->index.offset, footer.index.offset);
    }
}

TEST(Footer, RejectsShortInput) {
    const std::string encoded = sample_footer().encode();
    EXPECT_EQ(Footer::decode(Slice::from(encoded.substr(0, 43))).error(), Status::Corrupt);
    EXPECT_EQ(Footer::footer_length_from_trailer(Slice::from(std::string("short"))).error(),
              Status::Corrupt);
}

}  // namespace
}  // namespace elysiumkv
