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

    // **`list_generations` is deliberately not overridden here.** The sort key is
    // `gen#{generation}#edit#{seq}`, so the generation varies in the middle and no prefix query
    // selects snapshots alone — enumerating would read every edit item, up to
    // `manifest_edits_per_generation` of them per generation, and DynamoDB charges for items read
    // rather than returned. The engine's fallback probes a short window below the live pointer
    // with one `GetItem` each, which finds what a crash during a roll leaves and costs almost
    // nothing. A leak from further back stays.

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
