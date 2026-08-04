#ifndef ELYSIUMKV_TESTS_CONTRACT_MANIFEST_CATALOG_CONTRACT_HPP
#define ELYSIUMKV_TESTS_CONTRACT_MANIFEST_CATALOG_CONTRACT_HPP

#include "elysiumkv/manifest_catalog.hpp"

#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>

namespace elysiumkv::test {

/// ARCHITECTURE.md "Contract suites" — the contract suite every `ManifestCatalog` runs. `create` must return
/// catalogs that address the *same* store on repeated calls, so a case can open
/// a second instance and race it against the first.
struct ManifestCatalogFactory {
    std::string name;
    std::function<std::shared_ptr<ManifestCatalog>()> create;
};

inline void PrintTo(const ManifestCatalogFactory& factory, std::ostream* os) {
    *os << factory.name;
}

class ManifestCatalogContract : public ::testing::TestWithParam<ManifestCatalogFactory> {
protected:
    /// A factory may decline to construct — the remote instantiations need
    /// ELYSIUMKV_S3_ENDPOINT and there is nothing to test without it. Skipping is
    /// visible in the ctest report; silently passing an empty suite would not be.
    void SetUp() override {
        catalog_ = GetParam().create();
        if (catalog_ == nullptr) {
            GTEST_SKIP() << GetParam().name << " could not be constructed here (set "
                         << "ELYSIUMKV_S3_ENDPOINT for the remote instantiations)";
        }
    }

    static std::string as_string(const Buffer& b) {
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }

    std::shared_ptr<ManifestCatalog> catalog_;
};

struct ManifestCatalogFactoryName {
    std::string operator()(
        const ::testing::TestParamInfo<ManifestCatalogFactory>& info) const {
        return info.param.name;
    }
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_CONTRACT_MANIFEST_CATALOG_CONTRACT_HPP
