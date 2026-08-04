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

TEST(Footer, RejectsAnUnknownFormatVersion) {
    Footer footer = sample_footer();
    footer.format_version = 99;
    const std::string encoded = footer.encode();

    EXPECT_EQ(Footer::decode(Slice::from(encoded)).error(), Status::Corrupt);
    EXPECT_EQ(Footer::footer_length_from_trailer(Slice::from(encoded)).error(), Status::Corrupt);
}

TEST(Footer, RejectsShortInput) {
    const std::string encoded = sample_footer().encode();
    EXPECT_EQ(Footer::decode(Slice::from(encoded.substr(0, 43))).error(), Status::Corrupt);
    EXPECT_EQ(Footer::footer_length_from_trailer(Slice::from(std::string("short"))).error(),
              Status::Corrupt);
}

}  // namespace
}  // namespace elysiumkv
