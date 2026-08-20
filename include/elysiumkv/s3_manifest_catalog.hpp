#ifndef ELYSIUMKV_S3_MANIFEST_CATALOG_HPP
#define ELYSIUMKV_S3_MANIFEST_CATALOG_HPP

#include "elysiumkv/manifest_catalog.hpp"
#include "elysiumkv/s3_blob_store.hpp"

#include <memory>
#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "Ownership is one compare-and-set" — a `ManifestCatalog` on S3. Generation objects live under a prefix; the
/// pointer is a small object whose ETag is the token.
///
/// Using the ETag is what makes the CAS free: `compare_and_set` becomes a single
/// conditional `PutObject`, with no read-modify-write and no round trip to
/// discover the current version.
///
/// Commit latency lands on every flush and every compaction, and S3 pays
/// roughly 50 ms for it against single-digit milliseconds for DynamoDB. If commit
/// latency matters more than having one fewer service, prefer
/// `DynamoManifestCatalog`; this exists so an S3-only deployment is possible at
/// all.
class S3ManifestCatalog final : public ManifestCatalog {
public:
    /// Reuses `S3Options` — same bucket, prefix, endpoint and credential handling
    /// as the blob store, because there is no reason for two ways to say it. The
    /// prefix should differ from any blob store's, or manifest objects and SSTs
    /// share a namespace.
    static Result<std::shared_ptr<S3ManifestCatalog>> open(S3Options options);

    Result<std::optional<Entry>> read() override;
    Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                 uint64_t generation) override;

    std::future<Status> put_snapshot(uint64_t generation, Slice bytes) override;
    std::future<GetResult> get_snapshot(uint64_t generation) override;
    std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) override;
    std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) override;
    std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) override;
    std::future<Status> delete_generation(uint64_t generation) override;
    /// Listed with a delimiter, so it costs one page of common prefixes rather than a page per
    /// object — the generations, not their contents.
    std::future<Result<std::vector<uint64_t>>> list_generations() override;

    ~S3ManifestCatalog() override;

    S3ManifestCatalog(const S3ManifestCatalog&) = delete;
    S3ManifestCatalog& operator=(const S3ManifestCatalog&) = delete;

    struct Impl;

private:
    explicit S3ManifestCatalog(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_S3_MANIFEST_CATALOG_HPP
