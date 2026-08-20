#ifndef ELYSIUMKV_S3_BLOB_STORE_HPP
#define ELYSIUMKV_S3_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <chrono>
#include <memory>
#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "The ABI boundary" — a `BlobStore` over an S3 bucket.
///
/// The blob store moves opaque bytes and nothing else. In particular it applies
/// no compression: blocks arrive already compressed (ARCHITECTURE.md "Inside an SST"), and
/// whole-object compression would make `get(name, offset, len)` meaningless —
/// every block read would have to fetch and inflate the entire file.
struct S3Options {
    std::string bucket;
    /// Key prefix within the bucket, so several stores can share one. Joined
    /// with a single `/`; an empty prefix puts objects at the bucket root.
    std::string prefix;
    std::string region = "us-east-1";

    /// Non-empty points the client at something other than real S3 — LocalStack
    /// or MinIO. Forces path-style addressing, since a test endpoint has no
    /// per-bucket DNS.
    std::string endpoint;

    /// Left empty, the SDK's default chain is used (environment, profile,
    /// instance metadata). Set both for a fixed pair; this is what the tests do.
    std::string access_key;
    std::string secret_key;

    /// ARCHITECTURE.md "The ABI boundary" — two profiles, because one cannot serve both. A timeout
    /// generous enough for a 16 MB compaction read makes a point lookup's tail
    /// latency terrible; one tight enough for a point lookup invents failures on
    /// bulk reads. `bulk_view()` is the seam that lets the engine pick.
    std::chrono::milliseconds point_timeout{3'000};
    std::chrono::milliseconds bulk_timeout{60'000};

    /// At or above this, `put` uses a multipart upload on the bulk client. Retry
    /// granularity is worth having on a large object — a failed part is one part
    /// re-sent rather than the whole object — and below it a single PUT is cheaper.
    ///
    /// Write-once still holds: the completion carries `If-None-Match: *`, so a multipart put at a
    /// taken name is refused exactly as a single PUT is and reports the same `Unusable`.
    ///
    /// A failed upload is aborted rather than abandoned. S3 charges storage for the parts
    /// of an incomplete upload until it is aborted or a lifecycle rule expires it, so
    /// leaving one behind is a bill, not just untidiness.
    size_t multipart_threshold_bytes = 16ull << 20;

    /// Part size. S3 requires at least 5 MiB for every part except the last, which this
    /// clamps to. Exposed mainly so a test can force several parts without moving tens of
    /// megabytes: a threshold below one part size simply means a one-part upload, which is
    /// legal and is what makes the path reachable at a sane size.
    size_t multipart_part_bytes = 8ull << 20;

    /// `id()` is recorded per file in the manifest (ARCHITECTURE.md "The manifest is snapshots plus edits") and must be stable
    /// across restarts. Empty derives `s3://{bucket}/{prefix}`, which is stable
    /// as long as the configuration is; set it explicitly to rename a bucket
    /// without rewriting history.
    std::string id;
};

/// Objects are write-once: `put` uses `If-None-Match: *`, so a second write to
/// the same name fails rather than overwriting. File numbers are monotonic under
/// a single writer, so a collision means a zombie process is reusing them — and
/// the conditional costs nothing while converting a silent overwrite into an
/// error the engine can act on.
class S3BlobStore final : public BlobStore {
public:
    /// Fails only on unusable configuration; it does not talk to S3. A bucket
    /// that does not exist surfaces on first use as `Status::Io` — never as
    /// absence, because "the bucket is missing" and "the bucket name is wrong"
    /// are indistinguishable and neither is evidence of data loss (ARCHITECTURE.md "Immutable named objects").
    static Result<std::shared_ptr<S3BlobStore>> open(S3Options options);

    std::string id() const override;

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    std::future<Status> remove_many(const std::vector<std::string>& names) override;
    std::future<ListResult> list(std::string_view prefix) override;

    /// A sibling configured for large sequential reads. Same bucket, same
    /// prefix, same credentials — only the timeout profile differs.
    BlobStore& bulk_view() override;

    ~S3BlobStore() override;

    S3BlobStore(const S3BlobStore&) = delete;
    S3BlobStore& operator=(const S3BlobStore&) = delete;

    /// Public only as an incomplete type: the bulk view is a separate object in
    /// the implementation file and has to name this. Nothing outside can do
    /// anything with it, and it keeps the AWS headers out of this one.
    struct Impl;

private:
    /// Above `multipart_threshold_bytes`. Separate because the shapes share nothing but
    /// the name: three round trips minimum, a part list to assemble, and an abort to
    /// remember on every failing path.
    Status multipart_put(std::string_view name, Slice bytes);

public:

private:
    explicit S3BlobStore(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_S3_BLOB_STORE_HPP
