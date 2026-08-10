#include "version/version.hpp"
#include "version/version_edit.hpp"

#include <gtest/gtest.h>

#include <string>

namespace elysiumkv {
namespace {

FileMetadata file(int level, uint64_t number, std::string smallest, std::string largest,
                  uint64_t bytes = 1000, uint64_t write_time = 100) {
    return FileMetadata{.level = level,
                        .file_number = number,
                        .store_id = "store-a",
                        .smallest_key = std::move(smallest),
                        .largest_key = std::move(largest),
                        .file_bytes = bytes,
                        .num_entries = 10,
                        .min_write_time_ms = write_time};
}

/// The truncation point may only move forward, asserted on `Version::apply` directly.
///
/// `DbImpl::truncate_below` refuses a lower call before it ever builds an edit, so this rule is
/// unreachable through the public API — but it is what makes *manifest replay* safe, where edits
/// arrive in whatever order the log holds them. Testing it where it lives is the only way to see
/// it fail.
TEST(Version, TheTruncationPointOnlyEverMovesForward) {
    Version base({}, 1, {}, "mmm");

    VersionEdit backwards;
    backwards.truncation_point = "aaa";
    EXPECT_EQ(Version::apply(base, backwards)->truncation_point(), "mmm")
            << "an earlier edit replayed later must not resurrect data";

    VersionEdit forwards;
    forwards.truncation_point = "zzz";
    EXPECT_EQ(Version::apply(base, forwards)->truncation_point(), "zzz");

    VersionEdit silent;
    EXPECT_EQ(Version::apply(base, silent)->truncation_point(), "mmm")
            << "an edit that says nothing about truncation leaves it where it was";
}

TEST(VersionEdit, RoundTrips) {
    VersionEdit edit;
    edit.next_file_number = 42;
    edit.added.push_back(file(0, 7, "a", "m"));
    edit.added.push_back(file(2, 8, "n", "z", 5000, 200));
    edit.deleted.push_back({1, 3});
    edit.deleted.push_back({1, 4});
    edit.compaction_pointers.emplace_back(1, "hot-key");

    auto decoded = decode_version_edit(Slice::from(encode_version_edit(edit)));
    ASSERT_TRUE(decoded.has_value()) << status_name(decoded.error());
    EXPECT_EQ(decoded->next_file_number, 42u);
    EXPECT_EQ(decoded->added, edit.added);
    EXPECT_EQ(decoded->deleted, edit.deleted);
    EXPECT_EQ(decoded->compaction_pointers, edit.compaction_pointers);
}

TEST(VersionEdit, EmptyEditRoundTrips) {
    auto decoded = decode_version_edit(Slice::from(encode_version_edit(VersionEdit{})));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->empty());
}

TEST(VersionEdit, KeysWithArbitraryBytesSurvive) {
    VersionEdit edit;
    edit.added.push_back(file(0, 1, std::string("\x00\xFF\x01", 3), std::string("\xFF\xFF", 2)));

    auto decoded = decode_version_edit(Slice::from(encode_version_edit(edit)));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->added[0].smallest_key, std::string("\x00\xFF\x01", 3));
    EXPECT_EQ(decoded->added[0].largest_key, std::string("\xFF\xFF", 2));
}

// A torn manifest write must be *detected*, not misread — that is what lets
// replay stop cleanly at the first unacknowledged edit (ARCHITECTURE.md "Open and recovery").
TEST(VersionEdit, EveryCorruptedByteIsDetected) {
    VersionEdit edit;
    edit.next_file_number = 9;
    edit.added.push_back(file(0, 7, "a", "m"));
    const std::string encoded = encode_version_edit(edit);

    for (size_t i = 0; i < encoded.size(); i += 3) {
        std::string damaged = encoded;
        damaged[i] = static_cast<char>(damaged[i] ^ 0x7F);
        EXPECT_FALSE(decode_version_edit(Slice::from(damaged)).has_value()) << i;
    }
    for (size_t len = 0; len + 1 < encoded.size(); len += 5) {
        EXPECT_FALSE(decode_version_edit(Slice::from(encoded.substr(0, len))).has_value()) << len;
    }
}

TEST(VersionEdit, SnapshotRoundTrips) {
    VersionSnapshot snapshot;
    snapshot.next_file_number = 100;
    for (uint64_t i = 0; i < 2000; ++i) {
        snapshot.files.push_back(file(static_cast<int>(i % 4), i, "key-" + std::to_string(i),
                                      "key-" + std::to_string(i) + "z"));
    }
    snapshot.compaction_pointers.emplace_back(2, "pointer");

    const std::string encoded = encode_version_snapshot(snapshot);
    auto decoded = decode_version_snapshot(Slice::from(encoded));
    ASSERT_TRUE(decoded.has_value()) << status_name(decoded.error());
    EXPECT_EQ(decoded->next_file_number, 100u);
    EXPECT_EQ(decoded->files, snapshot.files);
    EXPECT_EQ(decoded->compaction_pointers, snapshot.compaction_pointers);
}

TEST(Version, AppliesAdditionsAndDeletions) {
    Version base;
    VersionEdit add;
    add.added.push_back(file(0, 1, "a", "z"));
    add.added.push_back(file(1, 2, "a", "m"));
    add.added.push_back(file(1, 3, "n", "z"));
    auto version = Version::apply(base, add);

    EXPECT_EQ(version->file_count(0), 1u);
    EXPECT_EQ(version->file_count(1), 2u);
    EXPECT_EQ(version->total_bytes(1), 2000u);

    VersionEdit remove;
    remove.deleted.push_back({1, 2});
    auto after = Version::apply(*version, remove);
    EXPECT_EQ(after->file_count(1), 1u);
    EXPECT_EQ(after->files_at(1)[0].file_number, 3u);

    // The base version is untouched: that immutability is what an iterator holds.
    EXPECT_EQ(version->file_count(1), 2u);
}

// ARCHITECTURE.md "Positional recency" — within L0 the higher file number wins, and the merging iterator resolves
// it positionally — so the level must be ordered by file number, descending.
TEST(Version, L0IsOrderedByRecency) {
    VersionEdit edit;
    edit.added.push_back(file(0, 5, "a", "z"));
    edit.added.push_back(file(0, 9, "a", "z"));
    edit.added.push_back(file(0, 7, "a", "z"));
    auto version = Version::apply(Version(), edit);

    ASSERT_EQ(version->file_count(0), 3u);
    EXPECT_EQ(version->files_at(0)[0].file_number, 9u);
    EXPECT_EQ(version->files_at(0)[1].file_number, 7u);
    EXPECT_EQ(version->files_at(0)[2].file_number, 5u);
}

TEST(Version, DeeperLevelsAreOrderedByKey) {
    VersionEdit edit;
    edit.added.push_back(file(1, 5, "m", "s"));
    edit.added.push_back(file(1, 9, "a", "c"));
    edit.added.push_back(file(1, 7, "t", "z"));
    auto version = Version::apply(Version(), edit);

    EXPECT_EQ(version->files_at(1)[0].smallest_key, "a");
    EXPECT_EQ(version->files_at(1)[1].smallest_key, "m");
    EXPECT_EQ(version->files_at(1)[2].smallest_key, "t");
}

TEST(Version, FindsOverlappingFiles) {
    VersionEdit edit;
    edit.added.push_back(file(1, 1, "a", "c"));
    edit.added.push_back(file(1, 2, "d", "f"));
    edit.added.push_back(file(1, 3, "g", "i"));
    auto version = Version::apply(Version(), edit);

    auto middle = version->overlapping_half_open(1, Slice::from(std::string("d")), Slice::from(std::string("e")));
    ASSERT_EQ(middle.size(), 1u);
    EXPECT_EQ(middle[0].file_number, 2u);

    auto spanning = version->overlapping_half_open(1, Slice::from(std::string("b")), Slice::from(std::string("h")));
    EXPECT_EQ(spanning.size(), 3u);

    auto unbounded = version->overlapping_half_open(1, Slice::from(std::string("e")), Slice());
    EXPECT_EQ(unbounded.size(), 2u);

    auto none = version->overlapping_half_open(1, Slice::from(std::string("x")), Slice());
    EXPECT_TRUE(none.empty());
}

TEST(Version, TracksTheOldestWriteAtEachLevel) {
    VersionEdit edit;
    edit.added.push_back(file(0, 1, "a", "c", 1000, 500));
    edit.added.push_back(file(0, 2, "d", "f", 1000, 200));
    auto version = Version::apply(Version(), edit);

    EXPECT_EQ(version->oldest_write_time_ms(0), 200u);
    EXPECT_EQ(version->oldest_write_time_ms(1), 0u) << "an empty level has no age";
}

TEST(Version, CompactionPointersCarryForward) {
    VersionEdit first;
    first.compaction_pointers.emplace_back(1, "k1");
    auto version = Version::apply(Version(), first);
    EXPECT_EQ(version->compaction_pointers().at(1), "k1");

    VersionEdit second;
    second.compaction_pointers.emplace_back(2, "k2");
    auto after = Version::apply(*version, second);
    EXPECT_EQ(after->compaction_pointers().at(1), "k1");
    EXPECT_EQ(after->compaction_pointers().at(2), "k2");
}

}  // namespace
}  // namespace elysiumkv
