// ARCHITECTURE.md "Contract suites" — the remote implementations run the same contract suites as the local
// ones, unchanged. `blob_store_contract.hpp` said "step 11 instantiates it for S3
// without editing a case"; this is that, and it is the only place the remote code
// runs under ASan, UBSan and TSan.
//
// It was nearly not written. The plan had the contract cases ported to Java
// instead, and that would have missed a real defect the very first run caught:
// `S3BlobStore` reported a write-once collision as `Config` while every other store
// reports `Unusable`. The suite pins that status; the implementation simply was not
// being run through it.
//
// Requires `ELYSIUMKV_S3_ENDPOINT` (LocalStack 4.4.0 — earlier images silently ignore
// `If-Match`). Without it every case skips, visibly, rather than passing.

#include "contract/blob_store_contract.hpp"
#include "contract/manifest_catalog_contract.hpp"
#include "elysiumkv/dynamo_manifest_catalog.hpp"
#include "elysiumkv/s3_blob_store.hpp"
#include "elysiumkv/s3_manifest_catalog.hpp"

#include <gtest/gtest.h>

#include <unistd.h>

#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

const char* endpoint() { return std::getenv("ELYSIUMKV_S3_ENDPOINT"); }

constexpr const char* kBucket = "elysiumkv";
constexpr const char* kTable = "elysiumkv-manifest";

/// Every case needs a *fresh* namespace, because these stores are write-once and a
/// second test reusing an address would fail on the address rather than on what it
/// is testing. But repeated `create()` calls within one case must address the
/// same store, or the catalog contract's racing case would race two unrelated
/// stores and never contend.
///
/// Keying on the running test's name gives both, and does so without depending on
/// when gtest happens to construct the factory object.
std::string namespace_for_current_test() {
    static std::string current_test;
    static std::string current_namespace;
    static int counter = 0;

    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string name = info == nullptr ? std::string("unknown") : info->name();
    if (name != current_test) {
        current_test = name;
        current_namespace =
            "contract-" + std::to_string(::getpid()) + "-" + std::to_string(++counter);
    }
    return current_namespace;
}

S3Options s3_options(const std::string& prefix) {
    S3Options options;
    options.bucket = kBucket;
    options.prefix = prefix;
    options.endpoint = endpoint();
    options.access_key = "test";
    options.secret_key = "test";
    // Low enough that the contract's 5 MiB case (`LargeObjectRoundTripsAndRangeReadsIntoIt`)
    // goes through the multipart path — which is how multipart gets contract coverage and,
    // with it, a run under ASan and UBSan.
    options.multipart_threshold_bytes = 4u << 20;
    options.multipart_part_bytes = 5u << 20;
    return options;
}

// --- BlobStore ----------------------------------------------------------------

std::shared_ptr<BlobStore> make_s3_store() {
    if (endpoint() == nullptr) return nullptr;
    auto store = S3BlobStore::open(s3_options(namespace_for_current_test() + "/store"));
    return store.has_value() ? *store : nullptr;
}

INSTANTIATE_TEST_SUITE_P(S3, BlobStoreContract,
                         ::testing::Values(BlobStoreFactory{"S3BlobStore", make_s3_store}),
                         BlobStoreFactoryName());

// --- ManifestCatalog ----------------------------------------------------------

std::shared_ptr<ManifestCatalog> make_s3_catalog() {
    if (endpoint() == nullptr) return nullptr;
    auto catalog = S3ManifestCatalog::open(s3_options(namespace_for_current_test() + "/manifest"));
    return catalog.has_value() ? *catalog : nullptr;
}

std::shared_ptr<ManifestCatalog> make_dynamo_catalog() {
    if (endpoint() == nullptr) return nullptr;
    DynamoOptions options;
    options.table = kTable;
    options.store_id = namespace_for_current_test();
    options.endpoint = endpoint();
    options.access_key = "test";
    options.secret_key = "test";
    // The table is shared by every case; store_id is what keeps them apart.
    options.create_table_if_missing = true;
    auto catalog = DynamoManifestCatalog::open(options);
    return catalog.has_value() ? *catalog : nullptr;
}

INSTANTIATE_TEST_SUITE_P(
    Remote, ManifestCatalogContract,
    ::testing::Values(ManifestCatalogFactory{"S3ManifestCatalog", make_s3_catalog},
                      ManifestCatalogFactory{"DynamoManifestCatalog", make_dynamo_catalog}),
    ManifestCatalogFactoryName());

}  // namespace
}  // namespace elysiumkv::test
