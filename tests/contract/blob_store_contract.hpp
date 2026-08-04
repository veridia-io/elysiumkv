#ifndef ELYSIUMKV_TESTS_CONTRACT_BLOB_STORE_CONTRACT_HPP
#define ELYSIUMKV_TESTS_CONTRACT_BLOB_STORE_CONTRACT_HPP

#include "elysiumkv/blob_store.hpp"

#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace elysiumkv::test {

/// ARCHITECTURE.md "Contract suites" — the abstract contract suite every `BlobStore` implementation runs,
/// local and remote alike. An implementation earns its place by passing this
/// unchanged; step 11 instantiates it for S3 without editing a case.
struct BlobStoreFactory {
    std::string name;
    /// Returns a fresh, empty, ready-to-use store.
    std::function<std::shared_ptr<BlobStore>()> create;
};

/// Keeps ctest's parameter comment readable instead of a byte dump.
inline void PrintTo(const BlobStoreFactory& factory, std::ostream* os) { *os << factory.name; }

class BlobStoreContract : public ::testing::TestWithParam<BlobStoreFactory> {
protected:
    /// A factory may decline to construct — the remote instantiations need
    /// ELYSIUMKV_S3_ENDPOINT and there is nothing to test without it. Skipping is
    /// visible in the ctest report; silently passing an empty suite would not be.
    void SetUp() override {
        store_ = GetParam().create();
        if (store_ == nullptr) {
            GTEST_SKIP() << GetParam().name << " could not be constructed here (set "
                         << "ELYSIUMKV_S3_ENDPOINT for the remote instantiations)";
        }
    }

    Status put(std::string_view name, std::string_view bytes) {
        return store_->put(name, Slice::from(bytes)).get();
    }
    GetResult get(std::string_view name, uint64_t offset = 0,
                  size_t len = BlobStore::kReadToEnd) {
        return store_->get(name, offset, len).get();
    }
    ListResult list(std::string_view prefix = "") { return store_->list(prefix).get(); }
    Status remove(std::string_view name) { return store_->remove(name).get(); }

    static std::string as_string(const Buffer& b) {
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }

    std::shared_ptr<BlobStore> store_;
};

struct BlobStoreFactoryName {
    std::string operator()(const ::testing::TestParamInfo<BlobStoreFactory>& info) const {
        return info.param.name;
    }
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_CONTRACT_BLOB_STORE_CONTRACT_HPP
