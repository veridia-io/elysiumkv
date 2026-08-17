#include "version/version_set.hpp"

#include "support/temp_dir.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include <memory>
#include <set>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

using test::TempDir;

FileMetadata file(int level, uint64_t number) {
    return FileMetadata{.level = level,
                        .file_number = number,
                        .store_id = "store-a",
                        .smallest_key = "a",
                        .largest_key = "z",
                        .file_bytes = 1000,
                        .num_entries = 10,
                        .min_write_time_ms = 100};
}

class VersionSetTest : public ::testing::Test {
protected:
    /// Records what the version set asks to be deleted, in order — the GC
    /// ordering rule of ARCHITECTURE.md "Open and recovery" is about exactly this sequence.
    VersionSet::DeleteObjects recorder() {
        return [this](const std::vector<FileMetadata>& files) {
            ++delete_calls_;
            for (const FileMetadata& f : files) deleted_.push_back(f.file_number);
            return std::vector<FileMetadata>{};  // nothing failed
        };
    }

    std::unique_ptr<VersionSet> make(int edits_per_generation = 1000) {
        auto set = std::make_unique<VersionSet>(*catalog_, edits_per_generation, recorder());
        return set;
    }

    TempDir dir_;
    std::unique_ptr<DiskManifestCatalog> catalog_ =
        std::make_unique<DiskManifestCatalog>(dir_.path());
    std::vector<uint64_t> deleted_;
    int delete_calls_ = 0;
};

TEST_F(VersionSetTest, AnEmptyStoreHasNoPointer) {
    auto versions = make();
    EXPECT_EQ(versions->recover(), Status::NotFound)
        << "an empty store is not a damaged one";

    ASSERT_EQ(versions->create(), Status::Ok);
    EXPECT_EQ(versions->generation(), 1u);
    EXPECT_TRUE(versions->current()->all_files().empty());
}

/// **A store that never deletes must not accumulate version slots.** Every install appends a
/// `weak_ptr`, and the only place that pruned them sat behind an early return taken whenever
/// nothing is pending — which a flush-only edit always is. A single configured level makes the
/// picker decline outright, so this is a reachable configuration and not a contrived one.
TEST_F(VersionSetTest, InstallsThatDeleteNothingDoNotAccumulateVersionSlots) {
    auto versions = make();
    ASSERT_EQ(versions->create(), Status::Ok);

    for (int i = 0; i < 200; ++i) {
        VersionEdit edit;
        edit.added.push_back(file(0, versions->allocate_file_number()));
        ASSERT_EQ(versions->apply(std::move(edit)), Status::Ok);
    }

    ASSERT_EQ(versions->pending_deletions(), 0u) << "nothing was deleted, which is the premise";
    versions->collect_obsolete();
    // Only the current version is still held; the other 200 expired as each install replaced them.
    EXPECT_LE(versions->tracked_versions(), 2u)
        << "expired version slots were never pruned on a store that deletes nothing";
}

TEST_F(VersionSetTest, EditsSurviveReopen) {
    {
        auto versions = make();
        ASSERT_EQ(versions->create(), Status::Ok);

        VersionEdit edit;
        edit.added.push_back(file(0, versions->allocate_file_number()));
        edit.added.push_back(file(0, versions->allocate_file_number()));
        ASSERT_EQ(versions->apply(std::move(edit)), Status::Ok);

        VersionEdit second;
        second.added.push_back(file(1, versions->allocate_file_number()));
        second.deleted.push_back({0, 1});
        ASSERT_EQ(versions->apply(std::move(second)), Status::Ok);
    }
    {
        auto versions = make();
        ASSERT_EQ(versions->recover(), Status::Ok);

        auto current = versions->current();
        EXPECT_EQ(current->file_count(0), 1u);
        EXPECT_EQ(current->files_at(0)[0].file_number, 2u);
        EXPECT_EQ(current->file_count(1), 1u);
        EXPECT_EQ(versions->next_file_number(), 4u)
            << "the file-number counter must not reuse numbers after a restart";
    }
}

// ARCHITECTURE.md "Open and recovery" — replay stops at the first gap in seq or the first object failing CRC.
// Objects after a gap are ignored even if present.
TEST_F(VersionSetTest, ReplayStopsAtATornEditAndIgnoresWhatFollows) {
    {
        auto versions = make();
        ASSERT_EQ(versions->create(), Status::Ok);
        for (uint64_t i = 1; i <= 4; ++i) {
            VersionEdit edit;
            edit.added.push_back(file(0, versions->allocate_file_number()));
            ASSERT_EQ(versions->apply(std::move(edit)), Status::Ok);
        }
    }

    // Damage edit 3. Edit 4 is intact and still present — and must be ignored.
    const auto path = dir_.path() / "manifest" / "000000000001" / "edit-000000000003";
    ASSERT_TRUE(std::filesystem::exists(path));
    {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(2);
        f.put('\xFF');
    }

    auto versions = make();
    ASSERT_EQ(versions->recover(), Status::Ok);
    EXPECT_EQ(versions->current()->file_count(0), 2u)
        << "only the edits before the damaged one may be applied";
}

TEST_F(VersionSetTest, ReplayStopsAtAMissingSequenceNumber) {
    {
        auto versions = make();
        ASSERT_EQ(versions->create(), Status::Ok);
        for (uint64_t i = 1; i <= 4; ++i) {
            VersionEdit edit;
            edit.added.push_back(file(0, versions->allocate_file_number()));
            ASSERT_EQ(versions->apply(std::move(edit)), Status::Ok);
        }
    }
    std::filesystem::remove(dir_.path() / "manifest" / "000000000001" / "edit-000000000002");

    auto versions = make();
    ASSERT_EQ(versions->recover(), Status::Ok);
    EXPECT_EQ(versions->current()->file_count(0), 1u);
}

TEST_F(VersionSetTest, RollsAGenerationAndCollapsesItsEditsIntoASnapshot) {
    auto versions = make(/*edits_per_generation=*/3);
    ASSERT_EQ(versions->create(), Status::Ok);

    for (int i = 0; i < 10; ++i) {
        VersionEdit edit;
        edit.added.push_back(file(0, versions->allocate_file_number()));
        ASSERT_EQ(versions->apply(std::move(edit)), Status::Ok);
    }
    EXPECT_GT(versions->generation(), 1u);
    const uint64_t generation = versions->generation();
    const size_t files = versions->current()->file_count(0);

    // Superseded generations are deleted; the live one alone must reconstruct
    // the same version.
    auto reopened = make();
    ASSERT_EQ(reopened->recover(), Status::Ok);
    EXPECT_EQ(reopened->generation(), generation);
    EXPECT_EQ(reopened->current()->file_count(0), files);
}

TEST_F(VersionSetTest, DeletedFilesAreCollected) {
    auto versions = make();
    ASSERT_EQ(versions->create(), Status::Ok);

    VersionEdit add;
    add.added.push_back(file(0, versions->allocate_file_number()));
    add.added.push_back(file(0, versions->allocate_file_number()));
    ASSERT_EQ(versions->apply(std::move(add)), Status::Ok);
    EXPECT_TRUE(deleted_.empty());

    VersionEdit remove;
    remove.deleted.push_back({0, 1});
    ASSERT_EQ(versions->apply(std::move(remove)), Status::Ok);
    EXPECT_EQ(deleted_, (std::vector<uint64_t>{1}));
    EXPECT_EQ(versions->pending_deletions(), 0u);
}

// ARCHITECTURE.md "The ABI boundary" — **collection is one call for the whole set, not one per file.** Against
// a remote store the per-file shape was one HTTP round trip each, and a compaction
// routinely obsoletes dozens of objects at once. The count is what this asserts:
// the files being deleted was already true when they were deleted one at a time.
TEST_F(VersionSetTest, ObsoleteFilesAreCollectedInOneBatch) {
    auto versions = make();
    ASSERT_EQ(versions->create(), Status::Ok);

    VersionEdit add;
    for (int i = 0; i < 8; ++i) add.added.push_back(file(0, versions->allocate_file_number()));
    ASSERT_EQ(versions->apply(std::move(add)), Status::Ok);
    ASSERT_EQ(delete_calls_, 0);

    // One compaction taking every input, which is the ordinary shape.
    VersionEdit compaction;
    for (uint64_t number = 1; number <= 8; ++number) compaction.deleted.push_back({0, number});
    compaction.added.push_back(file(1, versions->allocate_file_number()));
    ASSERT_EQ(versions->apply(std::move(compaction)), Status::Ok);

    EXPECT_EQ(deleted_.size(), 8u);
    EXPECT_EQ(delete_calls_, 1)
        << "eight obsolete objects must cost one call, not eight — that is the whole point";
    EXPECT_EQ(versions->pending_deletions(), 0u);
}

// A batch the engine could not delete stays pending in full and is retried, rather
// than being dropped and leaking the objects.
TEST_F(VersionSetTest, FilesThatCouldNotBeDeletedStayPending) {
    std::vector<FileMetadata> refuse;
    bool refusing = true;
    VersionSet versions(*catalog_, 1000,
                        [&](const std::vector<FileMetadata>& files) {
                            ++delete_calls_;
                            for (const FileMetadata& f : files) deleted_.push_back(f.file_number);
                            return refusing ? files : std::vector<FileMetadata>{};
                        });
    ASSERT_EQ(versions.create(), Status::Ok);

    VersionEdit add;
    add.added.push_back(file(0, versions.allocate_file_number()));
    add.added.push_back(file(0, versions.allocate_file_number()));
    ASSERT_EQ(versions.apply(std::move(add)), Status::Ok);

    VersionEdit remove;
    remove.deleted.push_back({0, 1});
    remove.deleted.push_back({0, 2});
    ASSERT_EQ(versions.apply(std::move(remove)), Status::Ok);
    EXPECT_EQ(versions.pending_deletions(), 2u) << "a refused delete is not forgotten";

    refusing = false;
    versions.collect_obsolete();
    EXPECT_EQ(versions.pending_deletions(), 0u);
    EXPECT_EQ(delete_calls_, 2) << "one attempt per pass, not per file";
}

// ARCHITECTURE.md "Versions are immutable snapshots", and the reason the whole component exists: a file is unlinked only when no
// live Version references it — not when compaction finishes. An iterator holding
// a Version is what keeps it alive.
TEST_F(VersionSetTest, AFileHeldByALiveVersionIsNotDeletedUntilItIsReleased) {
    auto versions = make();
    ASSERT_EQ(versions->create(), Status::Ok);

    VersionEdit add;
    add.added.push_back(file(0, versions->allocate_file_number()));
    ASSERT_EQ(versions->apply(std::move(add)), Status::Ok);

    // Stand in for an iterator: hold the version across the compaction.
    std::shared_ptr<const Version> held = versions->current();
    ASSERT_EQ(held->file_count(0), 1u);

    VersionEdit remove;
    remove.deleted.push_back({0, 1});
    ASSERT_EQ(versions->apply(std::move(remove)), Status::Ok);

    EXPECT_TRUE(deleted_.empty()) << "the object is still being read";
    EXPECT_EQ(versions->pending_deletions(), 1u);
    EXPECT_EQ(versions->current()->file_count(0), 0u) << "but new readers no longer see it";

    held.reset();  // the iterator is destroyed
    versions->collect_obsolete();
    EXPECT_EQ(deleted_, (std::vector<uint64_t>{1}));
    EXPECT_EQ(versions->pending_deletions(), 0u);
}

// ARCHITECTURE.md "Open and recovery" — an object may be deleted only once the edit recording its removal is
// durable. A failure to persist the edit leaves both the version and the object
// exactly as they were.
TEST_F(VersionSetTest, AnEditThatCannotBePersistedChangesNothing) {
    auto versions = make();
    ASSERT_EQ(versions->create(), Status::Ok);

    VersionEdit add;
    add.added.push_back(file(0, versions->allocate_file_number()));
    ASSERT_EQ(versions->apply(std::move(add)), Status::Ok);

    // Make the next edit's address unwritable by taking the name first.
    const auto generation_dir = dir_.path() / "manifest" / "000000000001";
    std::ofstream(generation_dir / "edit-000000000002") << "squatter";

    VersionEdit remove;
    remove.deleted.push_back({0, 1});
    EXPECT_NE(versions->apply(std::move(remove)), Status::Ok);

    EXPECT_TRUE(deleted_.empty()) << "nothing may be deleted for an edit that is not durable";
    EXPECT_EQ(versions->current()->file_count(0), 1u)
        << "and the version must still describe what is on disk";
}

// ARCHITECTURE.md "Ownership is one compare-and-set" — **the fence has to fire on the edit path, not only on a generation
// roll.** Two writers sharing a catalog collide on an edit address long before
// either rolls, because `next_seq_` advances from the same replayed state in both.
// That used to surface as `Config`: a fenced writer told it had a configuration
// problem, with `fenced_` still clear, carrying on with a stale view until its next
// roll happened to notice.
//
// An occupied edit address cannot mean anything else. `next_seq_` is engine-owned
// and monotonic, so this instance has never written where it is about to write.
TEST_F(VersionSetTest, AnEditAddressTakenByAnotherWriterFencesTheInstance) {
    auto ours = make(/*edits_per_generation=*/1000);
    ASSERT_EQ(ours->create(), Status::Ok);

    // A second writer on the same catalog, recovering the state ours just created —
    // so both allocate the same next edit sequence.
    DiskManifestCatalog other_catalog(dir_.path());
    VersionSet theirs(other_catalog, 1000, recorder());
    ASSERT_EQ(theirs.recover(), Status::Ok);

    VersionEdit theirs_edit;
    theirs_edit.added.push_back(file(0, theirs.allocate_file_number()));
    ASSERT_EQ(theirs.apply(std::move(theirs_edit)), Status::Ok);

    VersionEdit ours_edit;
    ours_edit.added.push_back(file(0, ours->allocate_file_number()));
    EXPECT_EQ(ours->apply(std::move(ours_edit)), Status::Fenced)
        << "losing the edit address is losing the store, and Config would not say so";
    EXPECT_TRUE(ours->fenced());
    EXPECT_EQ(ours->current()->file_count(0), 0u)
        << "and nothing was swapped: the version still describes what is on disk";

    VersionEdit further;
    further.added.push_back(file(0, 999));
    EXPECT_EQ(ours->apply(std::move(further)), Status::Fenced)
        << "a fenced instance must not write again";
}

// The same rule one step earlier: rolling a generation writes its snapshot before
// the CAS, so a writer that loses the race to roll finds the *snapshot* address
// taken. The generation number is derived from what this instance believes is
// current, so an occupied address there also means someone else installed it.
//
// This is the shape a live two-writer database actually takes — it is what a flush
// through the Java binding hit, where it surfaced as `Config` and told the loser it
// had a configuration problem.
TEST_F(VersionSetTest, LosingTheRaceToRollAGenerationFencesTheInstance) {
    auto ours = make(/*edits_per_generation=*/1);
    ASSERT_EQ(ours->create(), Status::Ok);

    // Another writer rolls generation 2 first, by hand: its snapshot exists and its
    // address is taken.
    DiskManifestCatalog other_catalog(dir_.path());
    ASSERT_EQ(other_catalog.put_snapshot(2, Slice::from(std::string("theirs"))).get(), Status::Ok);

    // One edit takes us past edits_per_generation, so this apply rolls.
    VersionEdit edit;
    edit.added.push_back(file(0, ours->allocate_file_number()));
    EXPECT_EQ(ours->apply(std::move(edit)), Status::Fenced)
        << "losing the roll is losing the store";
    EXPECT_TRUE(ours->fenced());
}

// ARCHITECTURE.md "Ownership is one compare-and-set" — a lost CAS means another process owns the store. Merging is not
// something this engine attempts.
TEST_F(VersionSetTest, LosingTheCompareAndSetFencesTheInstance) {
    auto versions = make(/*edits_per_generation=*/2);
    ASSERT_EQ(versions->create(), Status::Ok);

    // Another writer moves the pointer out from under us.
    DiskManifestCatalog other(dir_.path());
    auto pointer = other.read();
    ASSERT_TRUE(pointer.has_value() && pointer->has_value());
    ASSERT_TRUE(other.compare_and_set(*pointer, 99).has_value());

    Status status = Status::Ok;
    for (int i = 0; i < 5 && status == Status::Ok; ++i) {
        VersionEdit edit;
        edit.added.push_back(file(0, versions->allocate_file_number()));
        status = versions->apply(std::move(edit));
    }
    EXPECT_EQ(status, Status::Fenced);
    EXPECT_TRUE(versions->fenced());

    VersionEdit further;
    further.added.push_back(file(0, 999));
    EXPECT_EQ(versions->apply(std::move(further)), Status::Fenced)
        << "a fenced instance must not write again";
}

TEST_F(VersionSetTest, FileNumbersAreUniqueAndMonotonic) {
    auto versions = make();
    ASSERT_EQ(versions->create(), Status::Ok);

    std::set<uint64_t> numbers;
    for (int i = 0; i < 100; ++i) numbers.insert(versions->allocate_file_number());
    EXPECT_EQ(numbers.size(), 100u);

    VersionEdit edit;
    edit.added.push_back(file(0, *numbers.begin()));
    ASSERT_EQ(versions->apply(std::move(edit)), Status::Ok);

    auto reopened = make();
    ASSERT_EQ(reopened->recover(), Status::Ok);
    EXPECT_GT(reopened->allocate_file_number(), *numbers.rbegin());
}

}  // namespace
}  // namespace elysiumkv
