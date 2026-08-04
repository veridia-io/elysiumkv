#ifndef ELYSIUMKV_DYNAMO_MANIFEST_CATALOG_HPP
#define ELYSIUMKV_DYNAMO_MANIFEST_CATALOG_HPP

#include "elysiumkv/manifest_catalog.hpp"

#include <chrono>
#include <memory>
#include <string>

namespace elysiumkv {

struct DynamoOptions {
    std::string table;
    /// Partition key value — one store's manifest state, kept apart from any
    /// other sharing the table.
    std::string store_id;
    std::string region = "us-east-1";

    /// Non-empty points at LocalStack or DynamoDB Local instead of real DynamoDB.
    std::string endpoint;
    std::string access_key;
    std::string secret_key;

    /// The pointer install sits on every flush and compaction, so this is a
    /// commit-latency budget rather than a bulk one.
    std::chrono::milliseconds timeout{3'000};

    /// Off by default: a production table belongs to whatever provisions
    /// infrastructure, and creating one silently would hide a misconfigured name
    /// behind a working store. Tests turn it on.
    bool create_table_if_missing = false;
};

/// ARCHITECTURE.md "Ownership is one compare-and-set" — a `ManifestCatalog` on DynamoDB, and **the better fit for this
/// workload** than object storage: small items, conditional writes giving CAS
/// without an ETag round trip, and single-digit-millisecond commits against
/// roughly 50 ms for S3. Commit latency lands on every flush and every
/// compaction, so that difference is not academic.
///
/// Layout:
/// ```
/// PK = store_id
/// SK = "POINTER"
/// SK = "gen#{generation:012d}#snap#{chunk:04d}"
/// SK = "gen#{generation:012d}#edit#{seq:012d}"
/// ```
class DynamoManifestCatalog final : public ManifestCatalog {
public:
    static Result<std::shared_ptr<DynamoManifestCatalog>> open(DynamoOptions options);

    Result<std::optional<Entry>> read() override;
    Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                 uint64_t generation) override;

    std::future<Status> put_snapshot(uint64_t generation, Slice bytes) override;
    std::future<GetResult> get_snapshot(uint64_t generation) override;
    std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) override;
    std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) override;
    std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) override;
    std::future<Status> delete_generation(uint64_t generation) override;

    ~DynamoManifestCatalog() override;

    DynamoManifestCatalog(const DynamoManifestCatalog&) = delete;
    DynamoManifestCatalog& operator=(const DynamoManifestCatalog&) = delete;

    struct Impl;

private:
    explicit DynamoManifestCatalog(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DYNAMO_MANIFEST_CATALOG_HPP
