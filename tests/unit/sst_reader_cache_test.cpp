// The open-`SstReader` cache: the one cache in the engine that had neither a bound
// nor a number. Two properties carry everything else here — the bound is a bound, and
// evicting a reader someone is using cannot break them, because every user holds
// its own `shared_ptr`. The second is what makes bounding this cheap at all.

#include "sst/sst_reader_cache.hpp"

#include "elysiumkv/memory_budget.hpp"
#include "sst/sst_writer.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/disk_blob_store.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

using test::TempDir;

class SstReaderCacheTest : public ::testing::Test {
protected:
    void SetUp() override { store_ = std::make_shared<DiskBlobStore>(dir_.path()); }

    /// One SST per file number, all the same shape, so the per-reader cost is uniform
    /// and the arithmetic in these tests is about the cache rather than the files.
    std::shared_ptr<SstReader> open_reader(uint64_t file_number) {
        char name[32];
        std::snprintf(name, sizeof(name), "%012llu.sst",
                      static_cast<unsigned long long>(file_number));

        if (!written_.count(file_number)) {
            std::vector<std::pair<std::string, std::string>> entries;
            for (int i = 0; i < 200; ++i) {
                char key[32];
                std::snprintf(key, sizeof(key), "key:%06d", i);
                entries.emplace_back(key, std::string(64, 'v'));
            }
            VectorSource source(entries);
            SstOptions options;
            options.block_bytes = 512;
            options.bloom_bits_per_key = 10;
            auto built = build_sst(source, options);
            EXPECT_TRUE(built.has_value());
            EXPECT_EQ(store_->put(name, Slice::from(built->bytes)).get(), Status::Ok);
            sizes_[file_number] = built->bytes.size();
            written_.insert(file_number);
        }

        SstReaderOptions reader_options;
        reader_options.block_bytes = 512;
        reader_options.file_number = file_number;
        auto reader = SstReader::open(*store_, name, sizes_[file_number], reader_options);
        EXPECT_TRUE(reader.has_value());
        return reader.has_value() ? std::move(*reader) : nullptr;
    }

    /// A source over an in-memory vector; `build_sst` wants an `InternalIterator`.
    class VectorSource final : public InternalIterator {
    public:
        explicit VectorSource(std::vector<std::pair<std::string, std::string>> entries)
            : entries_(std::move(entries)) {}
        void seek_to_first() override { index_ = 0; }
        void seek(Slice) override { index_ = 0; }
        bool valid() const override { return index_ < entries_.size(); }
        void next() override { ++index_; }
        // build_sst only scans forward; these exist because the interface requires them.
        void seek_to_last() override { index_ = entries_.empty() ? 0 : entries_.size() - 1; }
        void seek_for_prev(Slice) override { seek_to_last(); }
        void prev() override { index_ = index_ == 0 ? entries_.size() : index_ - 1; }
        Slice key() const override { return Slice::from(entries_[index_].first); }
        Slice value() const override { return Slice::from(entries_[index_].second); }
        ValueType type() const override { return ValueType::Put; }
        Status status() const override { return Status::Ok; }

    private:
        std::vector<std::pair<std::string, std::string>> entries_;
        size_t index_ = 0;
    };

    TempDir dir_;
    std::shared_ptr<DiskBlobStore> store_;
    std::set<uint64_t> written_;
    std::map<uint64_t, uint64_t> sizes_;
};

TEST_F(SstReaderCacheTest, AResidentReaderIsReturnedAndCountedAsAHit) {
    SstReaderCache cache(64u << 20, nullptr);
    EXPECT_EQ(cache.get(1), nullptr);
    EXPECT_EQ(cache.misses(), 1u);

    auto reader = cache.insert(1, open_reader(1));
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(cache.count(), 1u);
    EXPECT_GT(cache.bytes(), 0u) << "a reader holds an index block and a filter";

    EXPECT_EQ(cache.get(1), reader);
    EXPECT_EQ(cache.hits(), 1u);
}

/// The property that makes a bound safe. A reader evicted while someone is reading
/// through it must keep working: the cache's reference is not the only one.
TEST_F(SstReaderCacheTest, EvictingAReaderInUseDoesNotBreakItsUser) {
    SstReaderCache cache(64u << 20, nullptr);
    auto held = cache.insert(1, open_reader(1));
    ASSERT_NE(held, nullptr);

    cache.forget(1);
    EXPECT_EQ(cache.count(), 0u);
    EXPECT_EQ(cache.get(1), nullptr);

    // The evicted reader is still perfectly usable through the reference the caller
    // already had — which is how an iterator survives compaction dropping its files.
    auto found = held->get(Slice::from(std::string("key:000005")));
    ASSERT_TRUE(found.has_value());
    ASSERT_TRUE(found->has_value());
}

TEST_F(SstReaderCacheTest, TheBoundIsABoundAndEvictionIsLeastRecentlyUsed) {
    // Room for two readers, not three.
    auto probe = open_reader(1);
    const size_t per_reader = probe->memory_bytes();
    probe.reset();
    SstReaderCache cache(per_reader * 2 + per_reader / 2, nullptr);

    cache.insert(1, open_reader(1));
    cache.insert(2, open_reader(2));
    ASSERT_EQ(cache.count(), 2u);

    // Touch 1, so 2 is the least recently used.
    ASSERT_NE(cache.get(1), nullptr);
    cache.insert(3, open_reader(3));

    EXPECT_LE(cache.bytes(), per_reader * 2 + per_reader / 2) << "the bound is a bound";
    EXPECT_EQ(cache.count(), 2u);
    EXPECT_NE(cache.get(1), nullptr) << "1 was touched and must have survived";
    EXPECT_NE(cache.get(3), nullptr) << "3 was just inserted";
    EXPECT_EQ(cache.get(2), nullptr) << "2 was the least recently used";
}

TEST_F(SstReaderCacheTest, AReaderLargerThanTheWholeCacheIsServedButNotCached) {
    auto probe = open_reader(1);
    const size_t per_reader = probe->memory_bytes();
    probe.reset();

    SstReaderCache cache(per_reader / 2, nullptr);
    auto reader = cache.insert(1, open_reader(1));
    EXPECT_NE(reader, nullptr) << "the reader is still handed back — it is a valid reader";
    EXPECT_EQ(cache.count(), 0u) << "but evicting everything for one entry is not worth it";
    EXPECT_EQ(cache.bytes(), 0u);
}

TEST_F(SstReaderCacheTest, ZeroMeansUnbounded) {
    SstReaderCache cache(0, nullptr);
    for (uint64_t i = 1; i <= 20; ++i) cache.insert(i, open_reader(i));
    EXPECT_EQ(cache.count(), 20u) << "zero is the old behaviour, kept reachable on purpose";
}

/// ARCHITECTURE.md "A process-wide memory budget" — this cache was the one that reported nothing, which is what made its size
/// invisible where sizing decisions are made.
TEST_F(SstReaderCacheTest, ResidentBytesAreReportedToTheSharedBudgetAndReleased) {
    auto probe = open_reader(1);
    const size_t per_reader = probe->memory_bytes();
    probe.reset();

    MemoryBudget budget(per_reader * 2 + per_reader / 2);
    {
        SstReaderCache cache(64u << 20, &budget);
        cache.insert(1, open_reader(1));
        EXPECT_EQ(budget.used(), per_reader);

        cache.insert(2, open_reader(2));
        EXPECT_EQ(budget.used(), per_reader * 2);

        // The budget is full, and a refusal means "do not become resident" — never
        // "fail the read".
        auto third = cache.insert(3, open_reader(3));
        EXPECT_NE(third, nullptr);
        EXPECT_EQ(cache.count(), 2u);
        EXPECT_EQ(budget.used(), per_reader * 2);

        cache.forget(1);
        EXPECT_EQ(budget.used(), per_reader) << "eviction gives the budget back";
    }
    EXPECT_EQ(budget.used(), 0u)
        << "and so does destruction, or a process that opens and closes instances leaks "
           "the whole budget one cache at a time";
}

TEST_F(SstReaderCacheTest, InsertingAResidentFileNumberKeepsTheFirstReader) {
    SstReaderCache cache(64u << 20, nullptr);
    auto first = cache.insert(1, open_reader(1));
    auto second = cache.insert(1, open_reader(1));

    EXPECT_EQ(cache.count(), 1u) << "the same file must not be charged twice";
    EXPECT_EQ(cache.bytes(), first->memory_bytes());
    EXPECT_NE(second, nullptr)
        << "and the loser of the race gets its own reader back rather than nothing";
    EXPECT_EQ(cache.get(1), first);
}

}  // namespace
}  // namespace elysiumkv
