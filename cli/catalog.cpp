#include "catalog.hpp"

#include "elysiumkv/disk_manifest_catalog.hpp"

#ifdef ELYSIUMKV_WITH_AWS
#include "elysiumkv/dynamo_manifest_catalog.hpp"
#include "elysiumkv/s3_manifest_catalog.hpp"
#endif

namespace elysiumkv::cli {

void add_catalog_flags(CLI::App& command, CatalogOptions& options) {
    command.add_option("--catalog", options.backend, "where the manifest lives")
        ->required()
        ->check(CLI::IsMember({"disk", "dynamo", "s3"}));
    command.add_option("--dir", options.dir, "disk: the catalog directory");
    command.add_option("--table", options.table, "dynamo: the DynamoDB table");
    command.add_option("--store", options.store, "dynamo: the partition key, one database");
    command.add_option("--bucket", options.bucket, "s3: the bucket");
    command.add_option("--prefix", options.prefix, "s3: the key prefix");
    command.add_option("--region", options.region, "AWS region")->capture_default_str();
    command.add_option("--endpoint", options.endpoint, "override, e.g. LocalStack");
}

std::shared_ptr<ManifestCatalog> open_catalog(const CatalogOptions& options) {
    if (options.backend == "disk") {
        if (options.dir.empty()) throw CLI::ValidationError("--catalog disk needs --dir");
        return std::make_shared<DiskManifestCatalog>(options.dir);
    }
#ifdef ELYSIUMKV_WITH_AWS
    if (options.backend == "dynamo") {
        if (options.table.empty() || options.store.empty()) {
            throw CLI::ValidationError("--catalog dynamo needs --table and --store");
        }
        DynamoOptions dynamo;
        dynamo.table = options.table;
        dynamo.store_id = options.store;
        dynamo.region = options.region;
        dynamo.endpoint = options.endpoint;
        auto catalog = DynamoManifestCatalog::open(std::move(dynamo));
        if (!catalog) throw CLI::ValidationError("could not open the DynamoDB catalog");
        return *catalog;
    }
    if (options.backend == "s3") {
        if (options.bucket.empty()) throw CLI::ValidationError("--catalog s3 needs --bucket");
        S3Options s3;
        s3.bucket = options.bucket;
        s3.prefix = options.prefix;
        s3.region = options.region;
        s3.endpoint = options.endpoint;
        auto catalog = S3ManifestCatalog::open(std::move(s3));
        if (!catalog) throw CLI::ValidationError("could not open the S3 catalog");
        return *catalog;
    }
    throw CLI::ValidationError("unreachable: --catalog was validated already");
#else
    throw CLI::ValidationError(
        "this build has no AWS support, so only --catalog disk works "
        "(build with -DELYSIUMKV_BUILD_AWS=ON)");
#endif
}

}  // namespace elysiumkv::cli
