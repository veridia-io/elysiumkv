// ARCHITECTURE.md "Absence is an answer, not an error" — a level below L0 is presented to the merge as one child that
// opens one file at a time, rather than a child per file. These cases are about the seam that
// creates: crossing a file boundary mid-scan, landing in the gap between two files, and turning up
// nothing where the level holds nothing.
//
// A level with only one file exercises none of it, which is what the default test
// configurations produce — so every case here forces several.

#include "db/db_impl.hpp"
#include "sst/concat_iterator.hpp"
#include "sst/sst_reader.hpp"
#include "sst/sst_writer.hpp"

#include "fault/fault_injecting_blob_store.hpp"
#include "support/temp_dir.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::string key_at(int i) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key:%06d", i);
    return buf;
}

class ConcatIteration : public ::testing::Test {
protected:
    static constexpr int kKeys = 2000;

    void SetUp() override {
        Options options;
        options.manifest_catalog = store_.catalog();
        options.background = BackgroundMode::Inline;
        options.memtable_bytes = 8u << 10;

        // Small files, so L1 ends up holding many of them.
        LevelOptions l0;
        l0.max_files = 2;
        l0.target_file_bytes = 8u << 10;
        LevelOptions l1;
        l1.target_file_bytes = 8u << 10;
        options.levels = {{0, l0}, {1, l1}};
        options.tiers = {Tier{.store = store_.store(0), .durability = Durability::Durable}};

        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value());
        db_ = std::move(opened->db);

        for (int i = 0; i < kKeys; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from("v" + std::to_string(i))),
                      Status::Ok);
        }
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_EQ(db_->compact_level(0), Status::Ok);

        // The whole point of the fixture. Without several files at L1 every case below would pass
        // against an implementation that could only ever open one.
        ASSERT_GT(db_->stats().levels[1].file_count, 3)
            << "the concat path is only interesting across file boundaries";
    }

    static std::vector<std::string> keys_of(Iterator& it) {
        std::vector<std::string> keys;
        while (it.next()) {
            keys.emplace_back(reinterpret_cast<const char*>(it.key().data()), it.key().size());
        }
        return keys;
    }

    TestStore store_{1};
    std::unique_ptr<DB> db_;
};

TEST_F(ConcatIteration, AForwardScanCrossesEveryFileBoundary) {
    auto it = db_->iterator();
    const std::vector<std::string> seen = keys_of(*it);

    ASSERT_EQ(seen.size(), static_cast<size_t>(kKeys));
    for (int i = 0; i < kKeys; ++i) EXPECT_EQ(seen[static_cast<size_t>(i)], key_at(i)) << i;
}

TEST_F(ConcatIteration, AReverseScanIsTheForwardScanBackwards) {
    auto it = db_->reverse_iterator();
    const std::vector<std::string> seen = keys_of(*it);

    ASSERT_EQ(seen.size(), static_cast<size_t>(kKeys));
    for (int i = 0; i < kKeys; ++i) {
        EXPECT_EQ(seen[static_cast<size_t>(i)], key_at(kKeys - 1 - i)) << i;
    }
}

TEST_F(ConcatIteration, ASeekLandsInTheFileThatHoldsTheKeyWhereverItIs) {
    // Across the whole keyspace, so some of these land at a file's first key, some at its last,
    // and some in the middle — and the fixture guarantees there are several files to land in.
    for (int start : {0, 1, 499, 500, 501, 1000, 1333, 1999}) {
        auto it = db_->iterator(Slice::from(key_at(start)));
        ASSERT_TRUE(it->next()) << start;
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(it->key().data()), it->key().size()),
                  key_at(start))
            << start;
    }
}

// A key that no file holds sits in the gap between two of them, and a scan from there must begin at
// the next key that exists rather than stop.
TEST_F(ConcatIteration, ASeekIntoAGapContinuesAtTheNextKey) {
    auto it = db_->iterator(Slice::from(std::string("key:000500x")));
    ASSERT_TRUE(it->next());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(it->key().data()), it->key().size()),
              key_at(501));
}

TEST_F(ConcatIteration, ASeekPastEveryFileFindsNothing) {
    auto it = db_->iterator(Slice::from(std::string("zzz")));
    EXPECT_FALSE(it->next());
    EXPECT_EQ(it->status(), Status::Ok) << "exhaustion is not a failure";
}

TEST_F(ConcatIteration, ABoundedScanStopsAtTheUpperBoundNotAtAFileBoundary) {
    auto it = db_->iterator(Slice::from(key_at(400)), Slice::from(key_at(1200)));
    const std::vector<std::string> seen = keys_of(*it);

    ASSERT_EQ(seen.size(), 800u);
    EXPECT_EQ(seen.front(), key_at(400));
    EXPECT_EQ(seen.back(), key_at(1199));
}

// A reverse scan bounded above starts at the last key below the bound, which is a file the search
// has to find by its *smallest* key rather than its largest.
TEST_F(ConcatIteration, AReverseBoundedScanStartsBelowTheBound) {
    auto it = db_->reverse_iterator(Slice::from(key_at(400)), Slice::from(key_at(1200)));
    const std::vector<std::string> seen = keys_of(*it);

    ASSERT_EQ(seen.size(), 800u);
    EXPECT_EQ(seen.front(), key_at(1199));
    EXPECT_EQ(seen.back(), key_at(400));
}

// A range delete spanning many files of a level still reads correctly. This does not test the
// gate: a level carrying range tombstones is presented file by file rather than collapsed, and
// removing that check leaves this case passing — because the compaction that wrote the tombstone
// also dropped the entries it covered, so nothing was left needing to be shadowed at read time. The
// gate guards the case where it was not, which no test here can construct.
TEST_F(ConcatIteration, ARangeDeleteAcrossManyFilesStillReadsCorrectly) {
    ASSERT_EQ(db_->delete_range(Slice::from(key_at(300)), Slice::from(key_at(1500))), Status::Ok);
    ASSERT_EQ(db_->flush(), Status::Ok);
    ASSERT_EQ(db_->compact_level(0), Status::Ok);

    auto it = db_->iterator();
    const std::vector<std::string> seen = keys_of(*it);

    ASSERT_EQ(seen.size(), static_cast<size_t>(kKeys - 1200));
    for (const std::string& key : seen) {
        EXPECT_TRUE(key < key_at(300) || !(key < key_at(1500))) << key << " is inside the range";
    }
}

/// A file that cannot be read is not a file that holds nothing.
///
/// Positioning walks forward past a file whose seek left the child invalid, which is right for a
/// file a truncating compaction emptied and wrong for one the store could not answer for. The two
/// are distinguishable only by the child's status, and both `enter` and `leave` overwrite the
/// child — so a status not taken at the point of failure is gone. Reported as absence, an
/// unreachable file becomes missing keys with an Ok scan, which is the one confusion
/// ARCHITECTURE.md "Immutable named objects" forbids everywhere.
TEST(ConcatIteratorFault, AnUnreadableFileIsNotReportedAsAnEmptyOne) {
    TempDir dir;
    auto disk = std::make_shared<DiskBlobStore>(dir.path());
    auto store = std::make_shared<FaultInjectingBlobStore>(disk);

    // Three disjoint, sorted files, as a level below L0 always is.
    std::vector<FileMetadata> files;
    std::vector<std::shared_ptr<SstReader>> readers;
    for (int f = 0; f < 3; ++f) {
        char name[32];
        std::snprintf(name, sizeof(name), "%012d.sst", f + 1);
        SstWriter writer({.compression = Compression::None});
        for (int i = f * 100; i < (f + 1) * 100; ++i) {
            writer.add(Slice::from(key_at(i)), ValueType::Put, Slice::from(std::string("v")));
        }
        auto built = writer.finish();
        ASSERT_TRUE(built.has_value());
        ASSERT_EQ(store->put(name, Slice::from(built->bytes)).get(), Status::Ok);

        files.push_back(FileMetadata{.level = 1,
                                     .file_number = static_cast<uint64_t>(f + 1),
                                     .store_id = store->id(),
                                     .smallest_key = built->smallest_key,
                                     .largest_key = built->largest_key,
                                     .file_bytes = built->bytes.size(),
                                     .num_entries = built->num_entries,
                                     .compression = Compression::None,
                                     .min_write_time_ms = 1000});
        // Opened before the fault is armed, so the failure below lands on reading a block rather
        // than on opening the file — the path where a status has somewhere to be lost.
        auto reader = SstReader::open(*store, name, built->bytes.size(), {});
        ASSERT_TRUE(reader.has_value()) << status_name(reader.error());
        readers.emplace_back(std::move(*reader));
    }

    VersionEdit edit;
    edit.added = files;
    auto version = Version::apply(Version(), edit);

    // Every read of the middle file fails from here on.
    store->add_rule({.op = FaultInjectingBlobStore::Op::Get,
                     .name_contains = "000000000002.sst",
                     .first_match = 0,
                     .match_count = 0,
                     .status = Status::Io});

    auto it = make_concat_iterator(version, 1, 0, 3,
                                   [&](const FileMetadata& file) -> Result<std::shared_ptr<SstReader>> {
                                       return readers[file.file_number - 1];
                                   });

    // A key inside the unreadable file. The answer may not be a key from the file after it.
    it->seek(Slice::from(key_at(150)));

    EXPECT_NE(it->status(), Status::Ok)
        << "the unreadable file was walked past as though it held nothing";
    if (it->valid()) {
        const std::string landed(reinterpret_cast<const char*>(it->key().data()), it->key().size());
        EXPECT_LT(landed, key_at(200))
            << "positioning skipped the unreadable file and answered from the next one";
    }
}

}  // namespace
}  // namespace elysiumkv::test
