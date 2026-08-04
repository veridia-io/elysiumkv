#include "db/db_impl.hpp"
#include "support/test_db.hpp"
#include "version/version.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::string as_text(const std::vector<uint8_t>& bytes) {
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

// Regression tests for a bug that reached the read path: `Version::overlapping`
// was half-open, and `compact_level()` asked it about a file's inclusive
// `smallest_key..largest_key`. An output-level file beginning exactly where the
// input ended was therefore never merged — the compaction wrote its output
// beside it, leaving two files covering that key at a level required to be
// non-overlapping, and reads could then serve the older one. A committed write
// reverted to its previous value.
//
// The picker had a *correct* inclusive rule in a private helper, which is why
// ordinary background compaction was unaffected and only the hand-built
// compactions in db_impl were wrong. Two implementations of one rule, and the
// engine used whichever the call site happened to reach.
//
// Found by the Java differential harness (ARCHITECTURE.md "Dependencies and artifacts") on its first run, because it
// calls compactLevel() — which the C++ differential does not — over short keys
// where boundary coincidences are common.
TEST(OverlapBoundary, CompactLevelKeepsTheNewestValue) {
    TestStore store(1);
    Options options = make_options(store, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    auto opened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(opened->db);
    const std::string key = "k";

    ASSERT_EQ(db->put(Slice::from(key), Slice::from(std::string("old"))), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);   // "old" now sits at L1

    ASSERT_EQ(db->put(Slice::from(key), Slice::from(std::string("new"))), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);            // "new" in a fresh L0 file

    auto before = db->get_copy(Slice::from(key));
    ASSERT_TRUE(before.has_value());
    ASSERT_EQ(as_text(*before), "new") << "reads shadow correctly before the rewrite";

    ASSERT_EQ(db->compact_level(0), Status::Ok);

    const Stats stats = db->stats();
    EXPECT_EQ(stats.levels[1].file_count, 1)
        << "L1 must stay non-overlapping; two files here both contain \"k\"";

    auto after = db->get_copy(Slice::from(key));
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(as_text(*after), "new") << "a committed write reverted to its previous value";
}

// An empty value is a value, not a tombstone. Same shape as the test above, but
// the newer write carries zero bytes — which is how the Java differential
// happened to surface the overlap bug, since its generator produces empty values
// and the C++ one never did.
TEST(OverlapBoundary, CompactLevelKeepsANewerEmptyValue) {
    TestStore store(1);
    Options options = make_options(store, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    auto opened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(opened->db);
    const std::string key = "k";

    ASSERT_EQ(db->put(Slice::from(key), Slice::from(std::string("old"))), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);

    ASSERT_EQ(db->put(Slice::from(key), Slice()), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    auto before = db->get_copy(Slice::from(key));
    ASSERT_TRUE(before.has_value());
    ASSERT_EQ(as_text(*before), "");

    ASSERT_EQ(db->compact_level(0), Status::Ok);
    auto after = db->get_copy(Slice::from(key));
    ASSERT_TRUE(after.has_value()) << "the empty value was dropped as if it were a tombstone";
    EXPECT_EQ(as_text(*after), "");
}

// compact_level() rewrites one file per compaction. At L0 the files are ordered
// newest-first, so the newest lands in the output level first — and the next
// compaction, carrying an *older* L0 file, merges against it. `all_inputs()`
// puts the source level ahead of the output level because the source is normally
// newer, and here it is not: the older L0 file wins and the newer value is lost.
TEST(OverlapBoundary, CompactLevelKeepsTheNewestOfSeveralL0Files) {
    TestStore store(1);
    Options options = make_options(store, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    auto opened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(opened->db);
    const std::string key = "k";

    // Two L0 files, each holding a version of the same key.
    ASSERT_EQ(db->put(Slice::from(key), Slice::from(std::string("older"))), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->put(Slice::from(key), Slice::from(std::string("newer"))), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    auto before = db->get_copy(Slice::from(key));
    ASSERT_TRUE(before.has_value());
    ASSERT_EQ(as_text(*before), "newer") << "L0 recency is positional, and reads honour it";

    ASSERT_EQ(db->compact_level(0), Status::Ok);

    auto after = db->get_copy(Slice::from(key));
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(as_text(*after), "newer") << "a rewrite must not reorder recency";
}

// A delete in an older L0 file must not resurrect over a newer put — the same
// inversion, in the shape that loses data outright rather than reverting it.
TEST(OverlapBoundary, CompactLevelDoesNotLetAnOlderDeleteWin) {
    TestStore store(1);
    Options options = make_options(store, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    auto opened = DbImpl::open(options, /*require_all_durable=*/true);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(opened->db);
    const std::string key = "k";

    ASSERT_EQ(db->remove(Slice::from(key)), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->put(Slice::from(key), Slice()), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_TRUE(db->get_copy(Slice::from(key)).has_value());

    ASSERT_EQ(db->compact_level(0), Status::Ok);
    EXPECT_TRUE(db->get_copy(Slice::from(key)).has_value())
        << "an older tombstone outranked a newer write";
}

// The same defect stated directly against the query, without an engine around
// it: two files, the second starting exactly where the first ends.
TEST(OverlapBoundary, AnInclusiveRangeFindsAFileStartingAtItsUpperBound) {
    std::vector<std::vector<FileMetadata>> levels(2);
    FileMetadata file;
    file.level = 1;
    file.file_number = 7;
    file.smallest_key = "k";
    file.largest_key = "m";
    levels[1].push_back(file);
    const Version version(std::move(levels), /*next_file_number=*/8, {});

    // A compaction input covering exactly "k": the file above starts at "k", so
    // they share that key and must be compacted together.
    EXPECT_EQ(version.overlapping_inclusive(1, Slice::from(std::string("k")),
                                  Slice::from(std::string("k")))
                  .size(),
              1u);
}

}  // namespace
}  // namespace elysiumkv::test
