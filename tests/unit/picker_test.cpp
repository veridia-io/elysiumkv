#include "compact/picker.hpp"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

FileMetadata file(int level, uint64_t number, std::string smallest, std::string largest,
                  uint64_t bytes = 1000, uint64_t tombstones = 0, uint64_t entries = 100) {
    return FileMetadata{.level = level,
                        .file_number = number,
                        .store_id = "store-0",
                        .smallest_key = std::move(smallest),
                        .largest_key = std::move(largest),
                        .file_bytes = bytes,
                        .num_entries = entries,
                        .num_tombstones = tombstones,
                        .compression = Compression::None,
                        .min_write_time_ms = 1000};
}

std::shared_ptr<const Version> version_of(std::vector<FileMetadata> files,
                                          std::map<int, std::string> pointers = {}) {
    VersionEdit edit;
    edit.added = std::move(files);
    for (const auto& [level, key] : pointers) edit.compaction_pointers.emplace_back(level, key);
    return Version::apply(Version(), edit);
}

/// Levels carry no storage decisions now (ARCHITECTURE.md "Compaction"), so a configuration is structure
/// and nothing else — no stores, no durability, no age.
ResolvedLevels config(int max_files = 4, size_t l1_max_bytes = 4096, int level_count = 3) {
    std::map<int, LevelOptions> levels;
    LevelOptions l0;
    l0.max_files = max_files;
    l0.target_file_bytes = 4096;
    levels[0] = l0;

    for (int level = 1; level < level_count; ++level) {
        LevelOptions deeper;
        deeper.target_file_bytes = 4096;
        if (level != level_count - 1) deeper.max_bytes = l1_max_bytes;
        levels[level] = deeper;
    }

    auto resolved = resolve_levels(levels);
    EXPECT_TRUE(resolved.has_value());
    return *resolved;
}

/// `max_compaction_bytes` bounds the primary input set, not only the expansion back into the
/// source level. At L0 the transitive closure is usually the whole level, so a budget consulted
/// only on expansion bounds nothing — and ARCHITECTURE.md makes it the third term of the exposure
/// window.
TEST(PickerTest, AnOverlappingLevelsInputSetStaysInsideTheBudget) {
    // Five mutually overlapping L0 files of 1000 bytes each: the closure is all of them.
    std::vector<FileMetadata> files;
    for (uint64_t n = 1; n <= 5; ++n) files.push_back(file(0, n, "a", "z"));
    auto version = version_of(files);

    auto compaction = pick_compaction(*version, config(/*max_files=*/2), /*budget=*/3500);
    ASSERT_TRUE(compaction.has_value());

    uint64_t bytes = 0;
    for (const FileMetadata& input : compaction->inputs) bytes += input.file_bytes;
    EXPECT_LE(bytes, 3500u) << "the closure was taken whole regardless of the budget";
    EXPECT_LT(compaction->inputs.size(), files.size());
}

/// Oldest first, and the direction is forced. Recency is `(level, file_number)`, so a file left
/// at an overlapping level is newer than the output exactly when its number is larger. Trimming the
/// newest keeps every survivor above the output; trimming the oldest would strand a stale file at
/// L0 shadowing an output built from newer data, and reads would return the stale value.
TEST(PickerTest, TrimmingAnOverlappingLevelKeepsTheOldestFiles) {
    std::vector<FileMetadata> files;
    for (uint64_t n = 1; n <= 5; ++n) files.push_back(file(0, n, "a", "z"));
    auto version = version_of(files);

    auto compaction = pick_compaction(*version, config(/*max_files=*/2), /*budget=*/2500);
    ASSERT_TRUE(compaction.has_value());
    ASSERT_FALSE(compaction->inputs.empty());

    uint64_t highest_taken = 0;
    for (const FileMetadata& input : compaction->inputs) {
        highest_taken = std::max(highest_taken, input.file_number);
    }
    EXPECT_EQ(compaction->inputs.size(), 2u);
    EXPECT_EQ(highest_taken, 2u)
        << "the set must be downward-closed in age; leaving an older file behind inverts recency";
}

/// A budget smaller than a single file must still make progress. Compacting nothing would leave the
/// level over its limit for ever, which is a stall rather than a bound.
TEST(PickerTest, ABudgetSmallerThanOneFileStillCompactsTheOldest) {
    std::vector<FileMetadata> files;
    for (uint64_t n = 1; n <= 5; ++n) files.push_back(file(0, n, "a", "z"));
    auto version = version_of(files);

    auto compaction = pick_compaction(*version, config(/*max_files=*/2), /*budget=*/10);
    ASSERT_TRUE(compaction.has_value());
    ASSERT_EQ(compaction->inputs.size(), 1u);
    EXPECT_EQ(compaction->inputs.front().file_number, 1u) << "the oldest, so recency survives";
}

/// A trim must not reorder what it keeps. The input vector is the merge's child list, and a tie
/// on equal keys is resolved by lowest child index — which is the recency rule only while the
/// children arrive in the level's own order, newest first at L0. Choosing what to keep by sorting
/// the vector by file number inverted that, and the merge then took the oldest value for every
/// duplicated key. `CompactionTest.TrimmingAnL0ClosureKeepsTheNewestOfWhatItMerged` is the same
/// defect as an observable stale read.
TEST(PickerTest, TrimmingAnOverlappingLevelLeavesTheLevelsOwnOrder) {
    std::vector<FileMetadata> files;
    for (uint64_t n = 1; n <= 5; ++n) files.push_back(file(0, n, "a", "z"));
    auto version = version_of(files);

    auto compaction = pick_compaction(*version, config(/*max_files=*/2), /*budget=*/2500);
    ASSERT_TRUE(compaction.has_value());
    ASSERT_EQ(compaction->inputs.size(), 2u);

    // Descending, as `files_at(0)` hands them out: child 0 is the newest of the two kept.
    EXPECT_EQ(compaction->inputs.front().file_number, 2u);
    EXPECT_EQ(compaction->inputs.back().file_number, 1u);
}

/// The one public helper in `LevelOptions`, and the only piece of public API with no caller in the
/// engine itself — a shape an embedder is invited to use has to be known to produce what it
/// describes.
TEST(PickerTest, TheGeometricLayoutIsWhatItDescribes) {
    const auto levels = LevelOptions::geometric(/*base=*/1024, /*multiplier=*/10, /*count=*/4);
    ASSERT_EQ(levels.size(), 4u);

    // L0 is bounded by file count, not bytes: it overlaps, so capacity there is a read-amplification
    // term rather than a size one.
    EXPECT_FALSE(levels.at(0).max_bytes.has_value());
    EXPECT_TRUE(levels.at(0).max_files.has_value());

    EXPECT_EQ(levels.at(1).max_bytes, 1024u);
    EXPECT_EQ(levels.at(2).max_bytes, 10240u);
    // The last level carries no capacity, because it absorbs everything: a bound there would be
    // a limit on the store rather than a compaction trigger.
    EXPECT_FALSE(levels.at(3).max_bytes.has_value());
}

TEST(PickerTest, NothingToDoWhenEveryLevelIsUnderItsLimits) {
    auto version = version_of({file(0, 1, "a", "b"), file(1, 2, "a", "b")});
    EXPECT_FALSE(pick_compaction(*version, config(), 1u << 30).has_value());
}

TEST(PickerTest, ScoreTriggersOnFileCount) {
    auto version = version_of({file(0, 1, "a", "b"), file(0, 2, "c", "d"), file(0, 3, "e", "f"),
                               file(0, 4, "g", "h"), file(0, 5, "i", "j")});
    auto compaction = pick_compaction(*version, config(/*max_files=*/4), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_EQ(compaction->level, 0);
    EXPECT_EQ(compaction->output_level, 1);
    EXPECT_GT(compaction->score, 1.0);
}

TEST(PickerTest, ScoreTriggersOnBytes) {
    auto version = version_of({file(1, 1, "a", "b", 3000), file(1, 2, "c", "d", 3000)});
    auto compaction = pick_compaction(*version, config(4, /*l1_max_bytes=*/4096), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_EQ(compaction->level, 1);
}

// ARCHITECTURE.md "Compaction" — the last level absorbs everything and has nowhere to spill to.
TEST(PickerTest, TheLastLevelNeverTriggers) {
    std::vector<FileMetadata> files;
    for (uint64_t i = 0; i < 100; ++i) {
        files.push_back(file(2, i, "k" + std::to_string(i), "k" + std::to_string(i), 1u << 20));
    }
    EXPECT_FALSE(pick_compaction(*version_of(std::move(files)), config(), 1u << 30).has_value());
}

// ARCHITECTURE.md "Compaction" — score is the only trigger. Age governs tier migration (ARCHITECTURE.md "Migration between tiers"); a level has
// no age at all, however old its files are.
TEST(PickerTest, AgeNeverTriggersACompaction) {
    std::vector<FileMetadata> files = {file(0, 1, "a", "b"), file(0, 2, "c", "d")};
    files[0].min_write_time_ms = 1;  // ancient
    auto version = version_of(std::move(files));
    EXPECT_FALSE(pick_compaction(*version, config(/*max_files=*/4), 1u << 30).has_value())
        << "two files against a limit of four is not over the limit, whatever their age";
}

TEST(PickerTest, TrivialMoveWhenNothingOverlaps) {
    auto version = version_of({file(1, 1, "a", "b", 8192)});
    auto compaction = pick_compaction(*version, config(4, 4096), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_TRUE(compaction->trivial_move);
    EXPECT_TRUE(compaction->overlaps.empty());
}

// ARCHITECTURE.md "Compaction" — a move never changes a file's tier, so there is no store boundary
// to consider.
TEST(PickerTest, TrivialMoveIsNotGatedOnStorage) {
    FileMetadata elsewhere = file(1, 1, "a", "b", 8192);
    elsewhere.store_id = "some-other-store";
    auto version = version_of({elsewhere});

    auto compaction = pick_compaction(*version, config(4, 4096), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_TRUE(compaction->trivial_move) << "the file stays on whatever tier it already occupies";
}

/* The same rule for range tombstones, which need it more: a file can carry them and no point
 * tombstones at all — a flush whose memtable saw only a `delete_range` is exactly that shape — so a
 * check that counted only point tombstones would let the commonest case move straight past the level
 * that reclaims it, with nothing afterwards forcing the rewrite.
 */
TEST(PickerTest, TrivialMoveIsRefusedForRangeTombstonesAtTheBottommost) {
    FileMetadata ranges = file(1, 1, "a", "b", 8192, /*tombstones=*/0);
    ranges.num_range_tombstones = 1;
    ranges.smallest_range_key = "a";
    ranges.largest_range_key = "c";

    auto version = version_of({ranges});
    auto refused = pick_compaction(*version, config(4, 4096), 1u << 30);
    ASSERT_TRUE(refused.has_value());
    EXPECT_TRUE(refused->output_is_bottommost);
    EXPECT_FALSE(refused->trivial_move) << "fall back to a rewrite, which drops them";
}

/* A file whose *only* content is a range tombstone has no data span, and the picker used to seed its
 * search from exactly that — leaving the overlap search matching nothing, not even the seed itself,
 * and a compaction with no inputs that the code below dereferenced. The kill-point fuzzer found it
 * as a segfault.
 */
TEST(PickerTest, ASeedWithNoDataSpanStillProducesInputs) {
    // At level 0, where overlapping files are gathered transitively — that search is what came
    // back empty. A deeper level takes the seed as its own input and never noticed.
    FileMetadata only_ranges = file(0, 2, "", "", 8192, /*tombstones=*/0);
    only_ranges.num_entries = 0;
    only_ranges.num_range_tombstones = 1;
    only_ranges.smallest_range_key = "m";
    only_ranges.largest_range_key = "p";

    // A second L0 file so the level is over its budget and a compaction is offered at all. The
    // tombstone-only file takes the higher number, which makes it the newest and so the seed.
    auto version = version_of({file(0, 1, "a", "b", 8192), only_ranges});
    auto compaction = pick_compaction(*version, config(/*max_files=*/1, 4096), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_FALSE(compaction->inputs.empty()) << "the seed must be among its own inputs";
}

// ARCHITECTURE.md "Compaction" — a move does not rewrite, so moving a tombstone-bearing file to where it is
// bottommost would carry them past the only point that reclaims them.
TEST(PickerTest, TrivialMoveIsRefusedForTombstonesAtTheBottommost) {
    auto with_tombstones = version_of({file(1, 1, "a", "b", 8192, /*tombstones=*/5)});
    auto refused = pick_compaction(*with_tombstones, config(4, 4096), 1u << 30);
    ASSERT_TRUE(refused.has_value());
    EXPECT_TRUE(refused->output_is_bottommost);
    EXPECT_FALSE(refused->trivial_move) << "fall back to a rewrite, which drops them";

    // Without tombstones the same shape moves freely.
    auto clean = version_of({file(1, 1, "a", "b", 8192, /*tombstones=*/0)});
    auto allowed = pick_compaction(*clean, config(4, 4096), 1u << 30);
    ASSERT_TRUE(allowed.has_value());
    EXPECT_TRUE(allowed->trivial_move);
}

// ARCHITECTURE.md "Compaction" — the dynamic bottommost condition. Not the last configured level: a
// store that never grows past L1 leaves the deeper levels empty forever, and a
// static rule would never drop a tombstone.
TEST(PickerTest, BottommostIsAboutTheKeyRangeNotTheLastLevel) {
    const std::vector<FileMetadata> l0 = {file(0, 1, "a", "b"), file(0, 2, "a", "b"),
                                          file(0, 3, "a", "b"), file(0, 4, "a", "b"),
                                          file(0, 5, "a", "b")};

    // Four configured levels, only L0 and L1 populated.
    std::vector<FileMetadata> sparse = l0;
    sparse.push_back(file(1, 9, "a", "b"));
    auto compaction = pick_compaction(*version_of(sparse), config(4, 4096, /*level_count=*/4),
                                      1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_EQ(compaction->output_level, 1);
    EXPECT_TRUE(compaction->output_is_bottommost)
        << "nothing deeper holds this range, so L1 is where tombstones stop mattering";

    // A file in the same range at a deeper level flips the answer.
    std::vector<FileMetadata> deep = sparse;
    deep.push_back(file(3, 20, "a", "b"));
    auto shadowed = pick_compaction(*version_of(deep), config(4, 4096, 4), 1u << 30);
    ASSERT_TRUE(shadowed.has_value());
    EXPECT_FALSE(shadowed->output_is_bottommost);

    // A deeper file in a *different* range does not shadow this one.
    std::vector<FileMetadata> disjoint = sparse;
    disjoint.push_back(file(3, 20, "y", "z"));
    auto unaffected = pick_compaction(*version_of(disjoint), config(4, 4096, 4), 1u << 30);
    ASSERT_TRUE(unaffected.has_value());
    EXPECT_TRUE(unaffected->output_is_bottommost);
}

/// The bottommost decision must cover what the compaction *writes*, not only what it reads from the
/// source level. An output-level file is rewritten whole, so the part of its span the inputs do not
/// reach is still in the output — and a deeper file beneath that part is exactly what makes dropping
/// a tombstone a resurrection rather than a reclamation.
TEST(PickerTest, BottommostCoversTheWholeSpanOfTheFilesItRewrites) {
    std::vector<FileMetadata> files;
    for (uint64_t n = 1; n <= 5; ++n) files.push_back(file(0, n, "a", "c"));
    // Reaches past the inputs, carries tombstones, and is rewritten in full.
    files.push_back(file(1, 9, "b", "e", 1000, /*tombstones=*/1));
    // Sits under the part of that file the inputs do not cover.
    files.push_back(file(2, 20, "d", "d"));

    auto compaction =
        pick_compaction(*version_of(std::move(files)), config(4, 1u << 30, 4), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    ASSERT_EQ(compaction->output_level, 1);
    ASSERT_EQ(compaction->overlaps.size(), 1u) << "the L1 file must be an input for this to matter";

    EXPECT_FALSE(compaction->output_is_bottommost)
        << "L2 holds `d`, which the rewritten L1 file covers: dropping tombstones there uncovers it";
}

/// Recency is positional, so a file left at an overlapping level shadows the output. That is only
/// sound when everything left behind is *newer* than everything moved down. The transitive closure
/// and the budget trim both maintain that; expanding back into the source level afterwards can
/// break it, because the expanded set is chosen by key range alone.
TEST(PickerTest, ExpandingAtAnOverlappingLevelLeavesOnlyNewerFilesBehind) {
    // File numbers ascend with age. 3 overlaps 2 on [f,g], and only 3 falls inside the range the
    // output-level file widens the compaction to.
    std::vector<FileMetadata> files = {file(0, 1, "a", "b"), file(0, 2, "f", "h"),
                                       file(0, 3, "e", "g"), file(1, 9, "a", "e")};
    auto version = version_of(std::move(files));

    auto compaction = pick_compaction(*version, config(/*max_files=*/2, 1u << 30, 3), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    ASSERT_EQ(compaction->level, 0);

    std::set<uint64_t> moved;
    for (const FileMetadata& input : compaction->inputs) moved.insert(input.file_number);
    for (const FileMetadata& kept : version->files_at(0)) {
        if (moved.count(kept.file_number) != 0) continue;
        for (const FileMetadata& input : compaction->inputs) {
            const bool overlaps = kept.effective_smallest() <= input.effective_largest() &&
                                  input.effective_smallest() <= kept.effective_largest();
            if (!overlaps) continue;
            EXPECT_GT(kept.file_number, input.file_number)
                << "file " << kept.file_number << " stayed at L0 overlapping file "
                << input.file_number << ", which moved to L1: the older file now wins the key";
        }
    }
}

TEST(PickerTest, IsBottommostForRangeIsARangeQuestion) {
    auto version = version_of({file(1, 1, "c", "f"), file(3, 2, "a", "b"), file(3, 3, "m", "z")});
    const int last = 3;

    EXPECT_TRUE(is_bottommost_for_range(*version, 1, last, Slice::from(std::string("c")),
                                        Slice::from(std::string("f"))));
    EXPECT_FALSE(is_bottommost_for_range(*version, 1, last, Slice::from(std::string("a")),
                                         Slice::from(std::string("f"))));
    EXPECT_TRUE(is_bottommost_for_range(*version, 3, last, Slice::from(std::string("a")),
                                        Slice::from(std::string("z"))))
        << "nothing is deeper than the last level";
}

TEST(PickerTest, OverlappingOutputFilesBecomeInputs) {
    auto version = version_of({
        file(1, 1, "c", "f", 8192),
        file(2, 10, "a", "d"),
        file(2, 11, "e", "g"),
        file(2, 12, "x", "z"),
    });
    auto compaction = pick_compaction(*version, config(4, 4096), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_FALSE(compaction->trivial_move);
    ASSERT_EQ(compaction->overlaps.size(), 2u);
    EXPECT_EQ(compaction->overlaps[0].file_number, 10u);
    EXPECT_EQ(compaction->overlaps[1].file_number, 11u);
}

TEST(PickerTest, OverlappingLevelsPullInTheirTransitiveClosure) {
    auto version = version_of({
        file(0, 1, "a", "c"),
        file(0, 2, "b", "e"),
        file(0, 3, "d", "g"),
        file(0, 4, "x", "z"),
        file(0, 5, "y", "z"),
    });
    auto compaction = pick_compaction(*version, config(/*max_files=*/4), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_GE(compaction->inputs.size(), 3u);
}

TEST(PickerTest, TheCompactionPointerSweepsAndWraps) {
    std::vector<FileMetadata> files = {file(1, 1, "a", "b", 4096), file(1, 2, "c", "d", 4096),
                                       file(1, 3, "e", "f", 4096)};
    auto cfg = config(4, /*l1_max_bytes=*/4096);

    auto first = pick_compaction(*version_of(files), cfg, 1u << 30);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->inputs.front().file_number, 1u);

    auto second = pick_compaction(*version_of(files, {{1, "b"}}), cfg, 1u << 30);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->inputs.front().file_number, 2u);

    auto wrapped = pick_compaction(*version_of(files, {{1, "zzz"}}), cfg, 1u << 30);
    ASSERT_TRUE(wrapped.has_value());
    EXPECT_EQ(wrapped->inputs.front().file_number, 1u) << "past the end, the sweep wraps";
}

TEST(PickerTest, ExpansionStaysUnderTheCompactionBudget) {
    std::vector<FileMetadata> files;
    for (uint64_t i = 0; i < 10; ++i) {
        const std::string key = "k" + std::to_string(i);
        files.push_back(file(1, i, key, key, 100000));
    }
    files.push_back(file(2, 100, "k0", "k9", 100000));

    auto compaction = pick_compaction(*version_of(std::move(files)), config(4, 4096),
                                      /*max_compaction_bytes=*/1);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_EQ(compaction->inputs.size(), 1u) << "a tight budget forbids expanding back";
}

TEST(PickerTest, GrandparentLimitIsTenTimesTheTargetFileSize) {
    auto cfg = config();
    EXPECT_EQ(max_grandparent_overlap_bytes(cfg.levels[1]), cfg.levels[1].target_file_bytes * 10);
}

TEST(PickerTest, CompactionMetadataDescribesItsInputs) {
    std::vector<FileMetadata> files = {file(1, 1, "c", "f", 8192), file(2, 10, "a", "d", 1000)};
    files[0].min_write_time_ms = 700;
    files[1].min_write_time_ms = 300;

    auto compaction = pick_compaction(*version_of(std::move(files)), config(4, 4096), 1u << 30);
    ASSERT_TRUE(compaction.has_value());
    EXPECT_EQ(compaction->all_inputs().size(), 2u);
    EXPECT_EQ(compaction->input_bytes(), 9192u);
    // The output still holds the oldest write it contains (ARCHITECTURE.md "The manifest is snapshots plus edits") — which is also
    // what places it on a tier (ARCHITECTURE.md "A tier is not a level").
    EXPECT_EQ(compaction->min_write_time_ms(), 300u);
    EXPECT_EQ(compaction->largest_key(), "f");
}


// --- tombstone density -------------------------------------------------------------------
//
// A level within its file and byte budgets never trips the size ratios, so a delete-heavy store
// accumulates tombstones that only a compaction reaching the bottommost level can drop. These
// cases are about the trigger that notices.

/// A configuration deliberately inside every size budget, so nothing but density can fire.
ResolvedLevels roomy() {
    return config(/*max_files=*/64, /*l1_max_bytes=*/1ull << 30, /*level_count=*/3);
}

TEST(PickerTombstoneDensity, ADenseFileTriggersACompactionTheSizeRatiosWouldNotHaveFound) {
    auto version = version_of({file(0, 1, "a", "z", 1000, /*tombstones=*/60, /*entries=*/100)});

    EXPECT_FALSE(pick_compaction(*version, roomy(), 1u << 30).has_value())
            << "no trigger configured: the level is well inside its budgets";

    auto picked = pick_compaction(*version, roomy(), 1u << 30, {/*trigger=*/0.5, /*min=*/10});
    ASSERT_TRUE(picked.has_value());
    EXPECT_EQ(picked->level, 0);
    EXPECT_GT(picked->score, 1.0) << "0.6 density against a 0.5 trigger scores 1.2";
}

TEST(PickerTombstoneDensity, BelowTheTriggerNothingFires) {
    auto version = version_of({file(0, 1, "a", "z", 1000, /*tombstones=*/40, /*entries=*/100)});
    EXPECT_FALSE(pick_compaction(*version, roomy(), 1u << 30, {0.5, 10}).has_value())
            << "0.4 density is under the 0.5 trigger";
}

/// Without a floor, a file holding a handful of entries scores on one tombstone and fires a
/// compaction that rewrites almost nothing — then does it again on the output.
TEST(PickerTombstoneDensity, ATinyFileDoesNotFireHoweverDenseItIs) {
    auto version = version_of({file(0, 1, "a", "z", 1000, /*tombstones=*/5, /*entries=*/5)});
    EXPECT_FALSE(pick_compaction(*version, roomy(), 1u << 30, {0.5, /*min_entries=*/1024}).has_value())
            << "entirely tombstones, but far too small to be worth rewriting";

    EXPECT_TRUE(pick_compaction(*version, roomy(), 1u << 30, {0.5, /*min_entries=*/5}).has_value())
            << "and it does fire once the floor admits it";
}

/// One dense table is enough: averaging it against its clean neighbours would hide the case.
TEST(PickerTombstoneDensity, OneDenseFileAmongCleanOnesIsEnough) {
    auto version = version_of({file(0, 1, "a", "c", 1000, 0, 1000),
                               file(0, 2, "d", "f", 1000, 0, 1000),
                               file(0, 3, "g", "i", 1000, /*tombstones=*/900, /*entries=*/1000),
                               file(0, 4, "j", "z", 1000, 0, 1000)});

    auto picked = pick_compaction(*version, roomy(), 1u << 30, {0.5, 10});
    ASSERT_TRUE(picked.has_value()) << "the level averages 0.225 but one file is at 0.9";
    EXPECT_EQ(picked->level, 0);
}

/// Density is a score on the same scale as the size ratios, not a trigger beside them — so the
/// level that is furthest past *any* of its thresholds is the one picked.
TEST(PickerTombstoneDensity, DensityCompetesWithTheSizeRatiosOnOneScale) {
    // L0 is barely over its file budget; L1 is far past the density trigger.
    std::map<int, LevelOptions> levels;
    LevelOptions l0;
    l0.max_files = 2;
    l0.target_file_bytes = 4096;
    levels[0] = l0;
    LevelOptions l1;
    l1.max_bytes = 1ull << 30;
    l1.target_file_bytes = 4096;
    levels[1] = l1;
    LevelOptions l2;
    l2.target_file_bytes = 4096;
    levels[2] = l2;
    auto resolved = resolve_levels(levels);
    ASSERT_TRUE(resolved.has_value());

    auto version = version_of({file(0, 1, "a", "b", 1000, 0, 1000),
                               file(0, 2, "c", "d", 1000, 0, 1000),
                               file(0, 3, "e", "f", 1000, 0, 1000),
                               file(1, 4, "g", "z", 1000, /*tombstones=*/950, /*entries=*/1000)});

    auto picked = pick_compaction(*version, *resolved, 1u << 30, {0.1, 10});
    ASSERT_TRUE(picked.has_value());
    EXPECT_EQ(picked->level, 1)
            << "L0 scores 1.5 on file count; L1 scores 9.5 on density, so L1 wins";
}

}  // namespace
}  // namespace elysiumkv
