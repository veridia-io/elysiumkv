#include "version/version.hpp"
#include "version/version_edit.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

namespace elysiumkv {
namespace {

using std::string_view_literals::operator""sv;

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

/* **A file a newer range tombstone covers whole can be unlinked without being read** — one manifest
 * edit, no I/O. It is `files_entirely_truncated` generalised from a floor to a range, and it is what
 * makes evicting a tenant cheap rather than merely expressible: otherwise the bytes come back only
 * when some later compaction happens to rewrite them, which for a bottom-level file may be never.
 *
 * Asserted here rather than through the engine because a compaction usually reaches those files
 * first, so an engine-level test would pass whether this existed or not.
 */
TEST(Version, AFileCoveredWholeByANewerRangeIsReclaimable) {
    FileMetadata data = file(1, 5, "b", "y");
    FileMetadata cover = file(0, 9, "", "");
    cover.num_entries = 0;
    cover.num_range_tombstones = 1;
    cover.smallest_range_key = "a";
    cover.largest_range_key = "z";

    Version version({{cover}, {data}}, 10, {}, "");
    const auto candidates = version.range_drop_candidates();
    ASSERT_EQ(candidates.size(), 1u);
    EXPECT_EQ(candidates[0].file.file_number, 5u);
    EXPECT_TRUE(candidates[0].exact) << "one range, so the recorded span is that range";
}

TEST(Version, APartlyCoveredFileIsNotReclaimable) {
    FileMetadata data = file(1, 5, "b", "y");
    FileMetadata cover = file(0, 9, "", "");
    cover.num_entries = 0;
    cover.num_range_tombstones = 1;
    cover.smallest_range_key = "a";
    cover.largest_range_key = "m";   // stops short of the file's largest key

    Version version({{cover}, {data}}, 10, {}, "");
    EXPECT_TRUE(version.range_drop_candidates().empty())
        << "a file straddling the range keeps its live half and is narrowed by compaction";
}

/// The cover has to be *newer*. An older tombstone says nothing about what was written after it.
TEST(Version, AnOlderRangeDoesNotReclaimANewerFile) {
    FileMetadata data = file(0, 9, "b", "y");
    FileMetadata cover = file(1, 5, "", "");
    cover.num_entries = 0;
    cover.num_range_tombstones = 1;
    cover.smallest_range_key = "a";
    cover.largest_range_key = "z";

    Version version({{data}, {cover}}, 10, {}, "");
    EXPECT_TRUE(version.range_drop_candidates().empty());
}

/// A file carrying tombstones of its own is left alone: dropping it would drop those too, and they
/// shadow files the cover says nothing about.
TEST(Version, AFileWithItsOwnRangesIsNeverReclaimedThisWay) {
    FileMetadata data = file(1, 5, "b", "y");
    data.num_range_tombstones = 1;
    data.smallest_range_key = "aaa";
    data.largest_range_key = "bbb";
    FileMetadata cover = file(0, 9, "", "");
    cover.num_entries = 0;
    cover.num_range_tombstones = 1;
    cover.smallest_range_key = "a";
    cover.largest_range_key = "z";

    Version version({{cover}, {data}}, 10, {}, "");
    EXPECT_TRUE(version.range_drop_candidates().empty());
}

/* **A hull of several ranges shortlists but does not decide.** The manifest records the span of a
 * file's ranges, which for two is a bounding interval with a gap in it — and a hull can show a file
 * is *not* covered, never that it is. So the candidate comes back marked inexact, for a caller with
 * store access to settle by reading the block; a `Version` is a data structure and does no I/O.
 */
TEST(Version, AHullOfSeveralRangesShortlistsWithoutDeciding) {
    FileMetadata data = file(1, 5, "b", "y");
    FileMetadata cover = file(0, 9, "", "");
    cover.num_entries = 0;
    cover.num_range_tombstones = 2;   // "a".."c" and "w".."z", say — the gap is invisible here
    cover.smallest_range_key = "a";
    cover.largest_range_key = "z";

    Version version({{cover}, {data}}, 10, {}, "");
    const auto candidates = version.range_drop_candidates();
    ASSERT_EQ(candidates.size(), 1u) << "the hull admits it, so it is worth a look";
    EXPECT_FALSE(candidates[0].exact)
        << "the hull covers the file, but the tombstones inside it may not";
}

// --- expiry by age -------------------------------------------------------------

/* `Options::ttl` drops a file whose every write has outlived the limit. The predicate is asserted
 * here rather than through the engine because the interesting cases are states a running store
 * reaches rarely and cannot be steered into on demand.
 */
TEST(Version, AFileWhoseNewestWriteHasExpiredIsDropped) {
    FileMetadata old_file = file(1, 5, "a", "m");
    old_file.max_write_time_ms = 100;

    Version version({{}, {old_file}}, 10, {}, "");
    const auto dead = version.files_expired_before(150);
    ASSERT_EQ(dead.size(), 1u);
    EXPECT_EQ(dead[0].file_number, 5u);

    EXPECT_TRUE(version.files_expired_before(99).empty()) << "not yet";
}

/* **The soundness condition, and the reason this is not simply "drop what is old".**
 *
 * A file at a deeper level can hold data *newer* than a file above it — a compaction output takes
 * the max over its inputs, so a deep file that absorbed recent keys carries a recent stamp while an
 * older shallow file above it does not. Dropping the shallow one then does not remove its key; it
 * uncovers the deeper, older version of that key. A resurrection, not an expiry.
 */
TEST(Version, AFileIsNotDroppedWhileSomethingOlderOverlapsIt) {
    FileMetadata shallow = file(1, 9, "b", "d");
    shallow.max_write_time_ms = 100;          // expired
    FileMetadata deeper = file(2, 4, "a", "z");
    deeper.max_write_time_ms = 10'000;        // absorbed recent keys, so not expired

    Version version({{}, {shallow}, {deeper}}, 20, {}, "");
    EXPECT_TRUE(version.files_expired_before(150).empty())
        << "dropping the shallow file would uncover the deeper one's older version of its keys";
}

/// The same at level 0, where files overlap each other and the lower number is the older.
TEST(Version, AnOlderLevelZeroSiblingAlsoBlocksTheDrop) {
    FileMetadata newer = file(0, 9, "b", "d");
    newer.max_write_time_ms = 100;
    FileMetadata older = file(0, 4, "a", "z");
    older.max_write_time_ms = 10'000;

    Version version({{newer, older}}, 20, {}, "");
    EXPECT_TRUE(version.files_expired_before(150).empty());
}

/* **The boundaries, because the answer is now found by binary search rather than a scan.**
 *
 * A file's span is closed at both ends, so a deeper file ending exactly where the candidate begins
 * *does* overlap it. That is the case a half-open search gets wrong, and it gets it wrong silently:
 * the file is dropped and its keys revert to the deeper file's older values.
 */
TEST(Version, ADeeperFileEndingExactlyAtTheCandidatesFirstKeyBlocksTheDrop) {
    FileMetadata candidate = file(1, 9, "m", "p");
    candidate.max_write_time_ms = 100;

    // Sorted and disjoint, as the level invariant requires and the search relies on.
    FileMetadata before = file(2, 1, "a", "c");
    FileMetadata touching = file(2, 2, "e", "m");   // ends exactly at the candidate's first key
    FileMetadata after = file(2, 3, "r", "z");
    for (FileMetadata* f : {&before, &touching, &after}) f->max_write_time_ms = 10000;

    Version version({{}, {candidate}, {before, touching, after}}, 20, {}, "");
    EXPECT_TRUE(version.files_expired_before(150).empty()) << "m is in both files";
}

TEST(Version, ADeeperFileStartingExactlyAtTheCandidatesLastKeyBlocksTheDrop) {
    FileMetadata candidate = file(1, 9, "m", "p");
    candidate.max_write_time_ms = 100;

    FileMetadata before = file(2, 1, "a", "c");
    FileMetadata touching = file(2, 2, "p", "z");   // starts exactly at the candidate's last key
    for (FileMetadata* f : {&before, &touching}) f->max_write_time_ms = 10000;

    Version version({{}, {candidate}, {before, touching}}, 20, {}, "");
    EXPECT_TRUE(version.files_expired_before(150).empty()) << "p is in both files";
}

/// One position further out on each side, which must *not* block — the other half of the boundary,
/// and what a search that reached too far would get wrong.
TEST(Version, DeeperFilesJustOutsideTheCandidatesSpanDoNotBlockTheDrop) {
    FileMetadata candidate = file(1, 9, "m", "p");
    candidate.max_write_time_ms = 100;

    FileMetadata below = file(2, 1, "a", "l");      // ends just before "m"
    FileMetadata above = file(2, 2, "q", "z");      // starts just after "p"
    for (FileMetadata* f : {&below, &above}) f->max_write_time_ms = 10000;

    Version version({{}, {candidate}, {below, above}}, 20, {}, "");
    const auto dead = version.files_expired_before(150);
    ASSERT_EQ(dead.size(), 1u);
    EXPECT_EQ(dead[0].file_number, 9u);
}

/// The blocker in the middle of a long level, which is what distinguishes a search from a peek at
/// the neighbours.
TEST(Version, ABlockerIsFoundWhereverItSitsInTheLevel) {
    FileMetadata candidate = file(1, 999, "key:000500", "key:000501");
    candidate.max_write_time_ms = 100;

    std::vector<FileMetadata> deep;
    for (int i = 0; i < 200; ++i) {
        char lo[32];
        char hi[32];
        std::snprintf(lo, sizeof(lo), "key:%06d", i * 10);
        std::snprintf(hi, sizeof(hi), "key:%06d", i * 10 + 9);
        FileMetadata f = file(2, static_cast<uint64_t>(i + 1), lo, hi);
        f.max_write_time_ms = 10000;
        deep.push_back(f);
    }

    Version version({{}, {candidate}, deep}, 2000, {}, "");
    EXPECT_TRUE(version.files_expired_before(150).empty())
        << "the file covering key:000500 is the 51st of 200, not a neighbour";
}

/// A *newer* file overlapping is not a reason to keep this one — it already shadows it, so dropping
/// the older uncovers nothing.
TEST(Version, ANewerOverlappingFileDoesNotBlockTheDrop) {
    FileMetadata candidate = file(2, 4, "b", "y");
    candidate.max_write_time_ms = 100;
    FileMetadata newer = file(1, 9, "a", "z");      // shallower, so newer
    newer.max_write_time_ms = 10000;

    Version version({{}, {newer}, {candidate}}, 20, {}, "");
    const auto dead = version.files_expired_before(150);
    ASSERT_EQ(dead.size(), 1u);
    EXPECT_EQ(dead[0].file_number, 4u);
}

/// Nothing overlapping beneath it, so it goes.
TEST(Version, AFileWithNothingOlderBeneathItIsDropped) {
    FileMetadata shallow = file(1, 9, "b", "d");
    shallow.max_write_time_ms = 100;
    FileMetadata elsewhere = file(2, 4, "w", "z");   // deeper, but a different key range
    elsewhere.max_write_time_ms = 10'000;

    Version version({{}, {shallow}, {elsewhere}}, 20, {}, "");
    const auto dead = version.files_expired_before(150);
    ASSERT_EQ(dead.size(), 1u);
    EXPECT_EQ(dead[0].file_number, 9u);
}

/// A file carrying range tombstones is spared: they would go with it, and they shadow files the age
/// of this one says nothing about.
TEST(Version, AnExpiredFileCarryingRangeTombstonesIsSpared) {
    FileMetadata carrier = file(1, 5, "a", "m");
    carrier.max_write_time_ms = 100;
    carrier.num_range_tombstones = 1;
    carrier.smallest_range_key = "a";
    carrier.largest_range_key = "c";

    Version version({{}, {carrier}}, 10, {}, "");
    EXPECT_TRUE(version.files_expired_before(150).empty());
}

/// Unknown is not "very old". Nothing should reach the manifest without a stamp, but guessing from
/// an absent one would delete a whole file.
TEST(Version, AFileWithNoRecordedWriteTimeNeverExpires) {
    FileMetadata unstamped = file(1, 5, "a", "m");
    unstamped.max_write_time_ms = 0;

    Version version({{}, {unstamped}}, 10, {}, "");
    EXPECT_TRUE(version.files_expired_before(9'999'999).empty());
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

/* The encryption fields are reserved and written empty (`FORMAT.md` §6), but reserved is not the
 * same as absent: they occupy positions, and everything after them
 * is found by reading through. So the round trip is asserted with them **populated**, which is what
 * the phase that fills them will do — and if that phase forgets to encode or decode one, the fields
 * after it decode as garbage rather than the record failing outright.
 */
TEST(VersionEdit, TheReservedEncryptionFieldsRoundTripWhenPopulated) {
    FileMetadata carrying = file(1, 11, "a", "z");
    carrying.encryption_provider = "kms-gcm-2026";
    // **The length comes from the literal, not from counting it by hand.** The hand-written 15 that
    // was here is 13 bytes of literal, so the string constructor read past the end of it — caught by
    // gcc's -Warray-bounds, and invisible to a round-trip test that would happily carry the extra
    // bytes both ways. `sv` keeps the embedded nulls and cannot disagree with what is written.
    carrying.encryption_metadata = std::string("\x00wrapped\xff\x00key"sv);
    ASSERT_EQ(carrying.encryption_metadata.size(), 13u)
        << "stated, so a literal edited later cannot quietly change what this round-trips";
    ASSERT_EQ(carrying.encryption_metadata[0], '\0') << "leading null";
    ASSERT_EQ(carrying.encryption_metadata[9], '\0') << "interior null";

    VersionEdit edit;
    edit.next_file_number = 12;
    edit.added.push_back(carrying);
    // A second file after it, so a mis-sized read of the first is visible as the second decoding
    // wrongly rather than as a length check catching it by luck.
    edit.added.push_back(file(2, 12, "aa", "zz"));

    auto decoded = decode_version_edit(Slice::from(encode_version_edit(edit)));
    ASSERT_TRUE(decoded.has_value()) << status_name(decoded.error());
    ASSERT_EQ(decoded->added.size(), 2u);
    EXPECT_EQ(decoded->added[0].encryption_provider, "kms-gcm-2026");
    EXPECT_EQ(decoded->added[0].encryption_metadata, carrying.encryption_metadata)
        << "the metadata is opaque bytes, embedded nulls included";
    EXPECT_EQ(decoded->added[1], edit.added[1]) << "the record after it decoded correctly";
}

/// Empty is the reserved id of the passthrough, so today's files and files written with encryption
/// disabled are the same case rather than two.
TEST(VersionEdit, AFileWithNoEncryptionRecordsAnEmptyProvider) {
    VersionEdit edit;
    edit.added.push_back(file(0, 1, "a", "z"));
    auto decoded = decode_version_edit(Slice::from(encode_version_edit(edit)));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->added.size(), 1u);
    EXPECT_TRUE(decoded->added[0].encryption_provider.empty());
    EXPECT_TRUE(decoded->added[0].encryption_metadata.empty());
}

TEST(VersionEdit, EmptyEditRoundTrips) {
    auto decoded = decode_version_edit(Slice::from(encode_version_edit(VersionEdit{})));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->empty());
}

TEST(VersionEdit, KeysWithArbitraryBytesSurvive) {
    VersionEdit edit;
    edit.added.push_back(file(0, 1, std::string("\x00\xFF\x01"sv), std::string("\xFF\xFF"sv)));

    auto decoded = decode_version_edit(Slice::from(encode_version_edit(edit)));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->added[0].smallest_key, std::string("\x00\xFF\x01"sv));
    EXPECT_EQ(decoded->added[0].largest_key, std::string("\xFF\xFF"sv));
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
