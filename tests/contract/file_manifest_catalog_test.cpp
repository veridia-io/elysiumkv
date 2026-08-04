#include "contract/manifest_catalog_contract.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/file_manifest_catalog.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include <memory>

namespace elysiumkv::test {
namespace {

/// One temp directory per test, shared by every catalog the factory hands out —
/// which is what lets the contract's fencing case race two instances against the
/// same store.
ManifestCatalogFactory local_factory() {
    auto dir = std::make_shared<TempDir>();
    return {"FileManifestCatalog", [dir] {
                auto* catalog = new FileManifestCatalog(dir->path());
                return std::shared_ptr<ManifestCatalog>(
                    catalog, [dir](ManifestCatalog* p) { delete p; });
            }};
}

INSTANTIATE_TEST_SUITE_P(FileCatalog, ManifestCatalogContract,
                         ::testing::Values(local_factory()), ManifestCatalogFactoryName());

TEST(FileManifestCatalog, PointerSurvivesReopen) {
    TempDir dir;
    ManifestCatalog::Entry installed;
    {
        FileManifestCatalog catalog(dir.path());
        auto entry = catalog.compare_and_set(std::nullopt, 1);
        ASSERT_TRUE(entry.has_value() && entry->has_value());
        installed = **entry;
    }
    {
        FileManifestCatalog reopened(dir.path());
        auto pointer = reopened.read();
        ASSERT_TRUE(pointer.has_value() && pointer->has_value());
        EXPECT_EQ((*pointer)->generation, installed.generation);
        EXPECT_EQ((*pointer)->token, installed.token);
    }
}

TEST(FileManifestCatalog, ADamagedPointerIsCorruptNotAbsent) {
    TempDir dir;
    FileManifestCatalog catalog(dir.path());
    ASSERT_TRUE(catalog.compare_and_set(std::nullopt, 1).has_value());

    std::ofstream(dir.path() / "manifest" / "CURRENT", std::ios::trunc) << "not a pointer";
    auto pointer = catalog.read();
    ASSERT_FALSE(pointer.has_value());
    EXPECT_EQ(pointer.error(), Status::Corrupt)
        << "an unreadable pointer must not look like an empty store";
}

}  // namespace
}  // namespace elysiumkv::test
