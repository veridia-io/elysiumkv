#include "contract/manifest_catalog_contract.hpp"

#include <algorithm>
#include <random>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

TEST_P(ManifestCatalogContract, PointerStartsAbsent) {
    auto pointer = catalog_->read();
    ASSERT_TRUE(pointer.has_value()) << status_name(pointer.error());
    EXPECT_FALSE(pointer->has_value()) << "an empty store has no pointer, and that is not an error";
}

TEST_P(ManifestCatalogContract, FirstInstallTakesTheEmptyExpectation) {
    auto installed = catalog_->compare_and_set(std::nullopt, 1);
    ASSERT_TRUE(installed.has_value());
    ASSERT_TRUE(installed->has_value());
    EXPECT_EQ((*installed)->generation, 1u);
    EXPECT_FALSE((*installed)->token.empty()) << "the token is what makes the CAS checkable";

    auto pointer = catalog_->read();
    ASSERT_TRUE(pointer.has_value());
    ASSERT_TRUE(pointer->has_value());
    EXPECT_EQ((*pointer)->generation, 1u);
    EXPECT_EQ((*pointer)->token, (*installed)->token);
}

TEST_P(ManifestCatalogContract, InstallingOverAnExistingPointerNeedsTheCurrentToken) {
    auto first = catalog_->compare_and_set(std::nullopt, 1);
    ASSERT_TRUE(first.has_value() && first->has_value());

    // A second "first install" must lose: the pointer is no longer absent.
    auto stale = catalog_->compare_and_set(std::nullopt, 2);
    ASSERT_TRUE(stale.has_value());
    EXPECT_FALSE(stale->has_value());

    auto next = catalog_->compare_and_set(*first, 2);
    ASSERT_TRUE(next.has_value());
    ASSERT_TRUE(next->has_value());
    EXPECT_EQ((*next)->generation, 2u);
    EXPECT_NE((*next)->token, (*first)->token) << "the token must move with every install";
}

// ARCHITECTURE.md "Ownership is one compare-and-set" — a lost CAS means another process owns the store. The engine turns this
// into Status::Fenced; the catalog's job is only to report it truthfully.
TEST_P(ManifestCatalogContract, TwoInstancesRacingFromTheSameExpectationLeaveOneLoser) {
    auto other = GetParam().create();

    auto installed = catalog_->compare_and_set(std::nullopt, 1);
    ASSERT_TRUE(installed.has_value() && installed->has_value());

    // Both read the same pointer, then both try to move it.
    auto seen_by_other = other->read();
    ASSERT_TRUE(seen_by_other.has_value() && seen_by_other->has_value());

    auto winner = catalog_->compare_and_set(*installed, 2);
    ASSERT_TRUE(winner.has_value());
    ASSERT_TRUE(winner->has_value());

    auto loser = other->compare_and_set(*seen_by_other, 2);
    ASSERT_TRUE(loser.has_value());
    EXPECT_FALSE(loser->has_value()) << "exactly one may win";

    auto pointer = other->read();
    ASSERT_TRUE(pointer.has_value() && pointer->has_value());
    EXPECT_EQ((*pointer)->token, (*winner)->token);
}

TEST_P(ManifestCatalogContract, SnapshotsAndEditsRoundTrip) {
    const std::string snapshot = "snapshot bytes";
    ASSERT_EQ(catalog_->put_snapshot(1, Slice::from(snapshot)).get(), Status::Ok);

    auto read_back = catalog_->get_snapshot(1).get();
    ASSERT_TRUE(read_back.has_value()) << status_name(read_back.error());
    EXPECT_EQ(as_string(*read_back), snapshot);

    for (uint64_t seq = 1; seq <= 5; ++seq) {
        const std::string edit = "edit-" + std::to_string(seq);
        ASSERT_EQ(catalog_->put_edit(1, seq, Slice::from(edit)).get(), Status::Ok);
    }
    for (uint64_t seq = 1; seq <= 5; ++seq) {
        auto edit = catalog_->get_edit(1, seq).get();
        ASSERT_TRUE(edit.has_value()) << seq;
        EXPECT_EQ(as_string(*edit), "edit-" + std::to_string(seq));
    }
}

TEST_P(ManifestCatalogContract, MissingObjectsAreNotFound) {
    auto snapshot = catalog_->get_snapshot(7).get();
    ASSERT_FALSE(snapshot.has_value());
    EXPECT_EQ(snapshot.error(), Status::NotFound);

    auto edit = catalog_->get_edit(7, 3).get();
    ASSERT_FALSE(edit.has_value());
    EXPECT_EQ(edit.error(), Status::NotFound);
}

// ARCHITECTURE.md "Ownership is one compare-and-set" — objects are immutable and write-once. A put at an existing address is a
// programming error, not an overwrite.
// **Status::Config specifically, from every implementation.** This checked only
// "not Ok" before, and under that check the file catalog reported `Unusable` while
// the S3 and DynamoDB ones reported `Config` — a divergence the suite could not
// see. It matters because the engine reacts to this status: an occupied edit
// address is how a fenced writer discovers it lost the store (`VersionSet::apply`),
// and a reaction keyed on the status cannot work when the status depends on which
// catalog happens to be configured.
TEST_P(ManifestCatalogContract, ObjectsAreWriteOnce) {
    ASSERT_EQ(catalog_->put_edit(1, 1, Slice::from(std::string("original"))).get(), Status::Ok);
    EXPECT_EQ(catalog_->put_edit(1, 1, Slice::from(std::string("replacement"))).get(),
              Status::Config);

    auto edit = catalog_->get_edit(1, 1).get();
    ASSERT_TRUE(edit.has_value());
    EXPECT_EQ(as_string(*edit), "original");

    ASSERT_EQ(catalog_->put_snapshot(2, Slice::from(std::string("original"))).get(), Status::Ok);
    EXPECT_EQ(catalog_->put_snapshot(2, Slice::from(std::string("replacement"))).get(),
              Status::Config)
        << "snapshots are write-once on the same terms as edits";
}

TEST_P(ManifestCatalogContract, ListEditsIsSortedAndScopedToItsGeneration) {
    for (uint64_t seq : {3u, 1u, 2u}) {
        ASSERT_EQ(catalog_->put_edit(1, seq, Slice::from(std::string("x"))).get(), Status::Ok);
    }
    ASSERT_EQ(catalog_->put_edit(2, 9, Slice::from(std::string("y"))).get(), Status::Ok);

    auto first = catalog_->list_edits(1).get();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, (std::vector<uint64_t>{1, 2, 3}));

    auto second = catalog_->list_edits(2).get();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, (std::vector<uint64_t>{9}));

    auto empty = catalog_->list_edits(99).get();
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->empty());
}

/// **Optional, and the contract is that it is one of exactly two things.** An implementation that
/// can enumerate cheaply should, because that is what makes generation reclamation complete rather
/// than merely usual; one that cannot says `Unsupported` and the engine probes a bounded window
/// below the pointer instead. What must not happen is a third answer — a partial list, or an empty
/// one from a store that holds generations — because the caller's whole decision is "collect
/// exactly what this returned".
///
/// The disk catalog enumerates. DynamoDB deliberately does not: its sort key varies the generation
/// in the middle, so no prefix query selects snapshots alone. Both are correct, and this is the
/// only test that says so for either — the remote implementations had no coverage of it at all.
TEST_P(ManifestCatalogContract, ListGenerationsEitherEnumeratesOrSaysItCannot) {
    auto empty = catalog_->list_generations().get();
    const bool supported = empty.has_value();
    if (!supported) {
        EXPECT_EQ(empty.error(), Status::Unsupported)
            << "an implementation that cannot enumerate says so with Unsupported, not an I/O error";
    } else {
        EXPECT_TRUE(empty->empty()) << "nothing has been written yet";
    }

    ASSERT_EQ(catalog_->put_snapshot(4, Slice::from(std::string("s4"))).get(), Status::Ok);
    ASSERT_EQ(catalog_->put_snapshot(7, Slice::from(std::string("s7"))).get(), Status::Ok);
    ASSERT_EQ(catalog_->put_edit(7, 1, Slice::from(std::string("e"))).get(), Status::Ok);

    auto listed = catalog_->list_generations().get();
    ASSERT_EQ(listed.has_value(), supported)
        << "the answer must not change with the contents: a caller decides once whether to use it";
    if (!supported) return;

    // **Every generation, once each** — an edit must not add a second entry for the generation its
    // snapshot already named, or the caller would delete the same one twice.
    std::vector<uint64_t> seen = *listed;
    std::sort(seen.begin(), seen.end());
    EXPECT_EQ(seen, (std::vector<uint64_t>{4, 7}));

    // And it tracks deletion, which is the half that makes reclamation terminate.
    ASSERT_EQ(catalog_->delete_generation(4).get(), Status::Ok);
    auto after = catalog_->list_generations().get();
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(*after, (std::vector<uint64_t>{7}));
}

TEST_P(ManifestCatalogContract, DeleteGenerationTakesEverythingOfItsOwnAndNothingElse) {
    ASSERT_EQ(catalog_->put_snapshot(1, Slice::from(std::string("s1"))).get(), Status::Ok);
    ASSERT_EQ(catalog_->put_edit(1, 1, Slice::from(std::string("e1"))).get(), Status::Ok);
    ASSERT_EQ(catalog_->put_snapshot(2, Slice::from(std::string("s2"))).get(), Status::Ok);
    ASSERT_EQ(catalog_->put_edit(2, 1, Slice::from(std::string("e2"))).get(), Status::Ok);

    ASSERT_EQ(catalog_->delete_generation(1).get(), Status::Ok);

    EXPECT_EQ(catalog_->get_snapshot(1).get().error(), Status::NotFound);
    EXPECT_EQ(catalog_->get_edit(1, 1).get().error(), Status::NotFound);
    EXPECT_TRUE(catalog_->get_snapshot(2).get().has_value());
    EXPECT_TRUE(catalog_->get_edit(2, 1).get().has_value());
}

// ARCHITECTURE.md "Ownership is one compare-and-set" — a few thousand file entries is ~450 KB, which is past DynamoDB's item
// limit. Every implementation must carry it whatever it has to do internally.
TEST_P(ManifestCatalogContract, LargeSnapshotsRoundTrip) {
    std::string snapshot;
    snapshot.reserve(2u << 20);
    while (snapshot.size() < (2u << 20)) snapshot += "file-entry-padding-";

    ASSERT_EQ(catalog_->put_snapshot(1, Slice::from(snapshot)).get(), Status::Ok);
    auto read_back = catalog_->get_snapshot(1).get();
    ASSERT_TRUE(read_back.has_value());
    EXPECT_EQ(read_back->size(), snapshot.size());
    EXPECT_EQ(as_string(*read_back), snapshot);
}

// The same for an edit, which is the case that was missing. An edit carries a full record per
// output file, so a compaction with many outputs — or ordinary keys and a small
// `target_file_bytes` — produces one far past any single-item limit a catalog may have.
//
// **Incompressible on purpose.** The padding above compresses to almost nothing, so a 2 MiB
// snapshot of it lands in one chunk and proves the chunking works only in the sense that it did
// not break. This body does not shrink, so it genuinely spans chunks.
TEST_P(ManifestCatalogContract, LargeEditsRoundTrip) {
    std::string edit;
    edit.reserve(2u << 20);
    std::mt19937 rng(20260818);
    while (edit.size() < (2u << 20)) edit.push_back(static_cast<char>(rng() & 0xFF));

    ASSERT_EQ(catalog_->put_edit(1, 7, Slice::from(edit)).get(), Status::Ok);

    auto read_back = catalog_->get_edit(1, 7).get();
    ASSERT_TRUE(read_back.has_value()) << status_name(read_back.error());
    EXPECT_EQ(read_back->size(), edit.size());
    EXPECT_EQ(as_string(*read_back), edit);

    // **One entry, not one per chunk.** Replay asks for each sequence once, so a chunked edit
    // reported several times would replay it several times.
    auto seqs = catalog_->list_edits(1).get();
    ASSERT_TRUE(seqs.has_value());
    EXPECT_EQ(*seqs, (std::vector<uint64_t>{7}));
}

// A sequence number is not a prefix of another one. Chunk addressing appends to the sequence, so
// `edit#7#` must not match `edit#70#` — the failure would be edit 7 silently reading 70's bytes.
TEST_P(ManifestCatalogContract, AnEditSequenceIsNotAPrefixOfAnother) {
    ASSERT_EQ(catalog_->put_edit(1, 7, Slice::from(std::string_view("seven"))).get(), Status::Ok);
    ASSERT_EQ(catalog_->put_edit(1, 70, Slice::from(std::string_view("seventy"))).get(),
              Status::Ok);
    ASSERT_EQ(catalog_->put_edit(1, 700, Slice::from(std::string_view("sevenhundred"))).get(),
              Status::Ok);

    EXPECT_EQ(as_string(*catalog_->get_edit(1, 7).get()), "seven");
    EXPECT_EQ(as_string(*catalog_->get_edit(1, 70).get()), "seventy");
    EXPECT_EQ(as_string(*catalog_->get_edit(1, 700).get()), "sevenhundred");

    auto seqs = catalog_->list_edits(1).get();
    ASSERT_TRUE(seqs.has_value());
    EXPECT_EQ(*seqs, (std::vector<uint64_t>{7, 70, 700}));
}

}  // namespace
}  // namespace elysiumkv::test
