#ifndef ELYSIUMKV_TESTS_SUPPORT_TEST_DB_HPP
#define ELYSIUMKV_TESTS_SUPPORT_TEST_DB_HPP

#include "support/temp_dir.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/file_manifest_catalog.hpp"
#include "elysiumkv/disk_cache_blob_store.hpp"
#include "elysiumkv/local_file_blob_store.hpp"
#include "elysiumkv/memory_cache_blob_store.hpp"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv::test {

/// A store on disk that survives close-and-reopen: the directory, the blob
/// stores and the catalog outlive any DB opened over them.
class TestStore {
public:
    explicit TestStore(int num_stores = 1) {
        for (int i = 0; i < num_stores; ++i) {
            const std::filesystem::path path = dir_.path() / ("store-" + std::to_string(i));
            std::filesystem::create_directories(path);
            auto store = std::make_shared<LocalFileBlobStore>(path, "store-" + std::to_string(i));
            store->set_sync_writes(false);  // tests are not measuring fsync
            stores_.push_back(std::move(store));
        }
        catalog_ = std::make_shared<FileManifestCatalog>(dir_.path());
    }

    const std::shared_ptr<LocalFileBlobStore>& store(size_t i = 0) const { return stores_[i]; }
    const std::shared_ptr<ManifestCatalog>& catalog() const { return catalog_; }
    const std::filesystem::path& path() const { return dir_.path(); }
    size_t num_stores() const { return stores_.size(); }

private:
    TempDir dir_;
    std::vector<std::shared_ptr<LocalFileBlobStore>> stores_;
    std::shared_ptr<ManifestCatalog> catalog_;
};

/// The LSM half, shared by every configuration below. Levels carry no storage
/// decisions at all now (ARCHITECTURE.md "Compaction") — only structure.
inline std::map<int, LevelOptions> make_levels(Compression codec) {
    LevelOptions l0;
    l0.compression = Compression::None;
    l0.max_files = 4;
    l0.slowdown_at = 8;
    l0.stop_at = 12;
    l0.target_file_bytes = 1u << 20;

    LevelOptions l1;
    l1.compression = codec;
    l1.max_bytes = 4u << 20;
    l1.target_file_bytes = 1u << 20;

    LevelOptions l2;  // last level: no capacity, absorbs everything
    l2.compression = codec;
    l2.target_file_bytes = 2u << 20;

    return {{0, l0}, {1, l1}, {2, l2}};
}

/// The simplest correct configuration: one durable tier, lag = 0 (ARCHITECTURE.md "A tier is not a level").
inline Options make_options(const TestStore& store, Compression codec = Compression::None,
                            size_t memtable_bytes = 1u << 20) {
    Options options;
    options.manifest_catalog = store.catalog();
    options.memtable_bytes = memtable_bytes;
    options.block_bytes = 1024;
    options.levels = make_levels(codec);
    options.tiers = {Tier{.store = store.store(0), .durability = Durability::Durable}};
    return options;
}

/// Two durable tiers: files older than `max_age` migrate from the hot store to
/// the cold one. Storage cost, not durability — nothing is at risk either way.
inline Options make_tiered_options(const TestStore& store, Duration max_age,
                                   Compression codec = Compression::None,
                                   size_t memtable_bytes = 64u << 10) {
    Options options = make_options(store, codec, memtable_bytes);
    options.tiers = {
        Tier{.store = store.store(0), .durability = Durability::Durable, .max_age = max_age},
        Tier{.store = store.store(1), .durability = Durability::Durable},
    };
    return options;
}

/// ARCHITECTURE.md "A tier is not a level" — the second shape: a transient hot tier over a durable one. Files on tier 0
/// are the embedder's responsibility if that store is lost, and `max_age` bounds
/// how far back that reaches.
inline Options make_transient_options(const TestStore& store, Duration max_age,
                                      Duration stall_age, size_t memtable_bytes = 64u << 10) {
    Options options = make_options(store, Compression::None, memtable_bytes);
    options.tiers = {
        Tier{.store = store.store(0),
             .durability = Durability::Transient,
             .max_age = max_age,
             .stall_age = stall_age},
        Tier{.store = store.store(1), .durability = Durability::Durable},
    };
    return options;
}

/// ARCHITECTURE.md "Caches chain" — the example chain, minus the remote store: memory over disk over whatever is
/// already there. Returns the outermost layer and **keeps every layer alive through
/// it**, since each holds a `shared_ptr` to the one below.
///
/// The budgets are deliberately small. A cache large enough to hold a test's whole
/// working set exercises insertion and nothing else; these force eviction constantly,
/// which is where a range-keyed cache goes wrong.
inline std::shared_ptr<BlobStore> wrap_in_cache_chain(std::shared_ptr<BlobStore> below,
                                                      const std::filesystem::path& cache_root,
                                                      const std::string& label,
                                                      size_t fetch_granularity = 0) {
    auto disk = std::make_shared<DiskCacheBlobStore>(std::move(below), cache_root / (label + "-disk"),
                                                     1u << 20, /*cache_on_write=*/true,
                                                     fetch_granularity);
    return std::make_shared<MemoryCacheBlobStore>(disk, nullptr, 64u << 10,
                                                  /*cache_on_write=*/true, fetch_granularity);
}

/// Wraps every tier's store in a chain, in place. The engine sees `BlobStore`s and
/// nothing else, which is exactly the property under test.
inline void cache_every_tier(Options& options, const std::filesystem::path& cache_root,
                             size_t fetch_granularity = 0) {
    std::filesystem::create_directories(cache_root);
    for (size_t i = 0; i < options.tiers.size(); ++i) {
        options.tiers[i].store = wrap_in_cache_chain(options.tiers[i].store, cache_root,
                                                     "tier-" + std::to_string(i),
                                                     fetch_granularity);
    }
}

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_TEST_DB_HPP
