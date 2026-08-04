#include "elysiumkv/s3_blob_store.hpp"

// One definition of what names a store accepts, shared with the local store —
// this file had its own, looser copy, and the two disagreed about a leading dot.
#include "blob/object_name.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <algorithm>
#include <mutex>
#include <optional>
#include <sstream>
#include <utility>

namespace elysiumkv {
namespace {

/// The SDK needs process-wide init and shutdown, and a process may hold several
/// stores. Refcounting it here rather than asking the embedder to remember:
/// getting it wrong surfaces as a crash during exit, long after the mistake.
class SdkGuard {
public:
    SdkGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (refs_++ == 0) {
            options_ = new Aws::SDKOptions();
            Aws::InitAPI(*options_);
        }
    }
    ~SdkGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (--refs_ == 0) {
            Aws::ShutdownAPI(*options_);
            delete options_;
            options_ = nullptr;
        }
    }
    SdkGuard(const SdkGuard&) = delete;
    SdkGuard& operator=(const SdkGuard&) = delete;

private:
    static std::mutex mutex_;
    static int refs_;
    static Aws::SDKOptions* options_;
};

std::mutex SdkGuard::mutex_;
int SdkGuard::refs_ = 0;
Aws::SDKOptions* SdkGuard::options_ = nullptr;

/// **The discard path rests on this function.** `Status::NotFound` is positive
/// evidence that an object is absent, and a successful `list` omitting a
/// referenced file is the only loss signal the engine accepts — so anything that
/// merely means "could not determine" has to be `Io`:
///
///   * `403` is not absence. A permissions failure says nothing about whether the
///     object exists, and reporting it as absence lets a misconfigured role look
///     exactly like a wiped bucket.
///   * `NoSuchBucket` is not absence either. Buckets do not spontaneously vanish,
///     so it always means misconfiguration — and a deleted bucket is anyway
///     indistinguishable from a mistyped one.
///
/// Only a genuine per-object 404 earns `NotFound`.
template <typename Error>
Status classify(const Error& error) {
    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
        error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
        return Status::NotFound;
    }
    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_BUCKET) return Status::Io;
    // A 404 the SDK did not map to a typed error is still a genuine absence.
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return Status::NotFound;
    }
    return Status::Io;
}

}  // namespace

struct S3BlobStore::Impl {
    SdkGuard sdk;
    S3Options options;
    std::shared_ptr<Aws::S3::S3Client> point_client;
    std::shared_ptr<Aws::S3::S3Client> bulk_client;
    std::string id;
    std::unique_ptr<BlobStore> bulk_facade;

    std::string key_for(std::string_view name) const {
        if (options.prefix.empty()) return std::string(name);
        return options.prefix + "/" + std::string(name);
    }

    /// Names are relative to the store, so the prefix comes back off. A key that
    /// does not carry it belongs to something else sharing the bucket and is
    /// skipped rather than truncated into a name we would then fail to read.
    std::optional<std::string> name_for(const std::string& key) const {
        if (options.prefix.empty()) return key;
        const std::string expected = options.prefix + "/";
        if (key.size() <= expected.size() || key.compare(0, expected.size(), expected) != 0) {
            return std::nullopt;
        }
        return key.substr(expected.size());
    }
};

namespace {

GetResult do_get(S3BlobStore::Impl& impl, Aws::S3::S3Client& client, std::string_view name,
                 uint64_t offset, size_t len) {
    if (!is_valid_object_name(name)) return std::unexpected(Status::Config);

    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(impl.options.bucket);
    request.SetKey(impl.key_for(name));

    // kReadToEnd sends no Range at all — a whole-object GET is exactly what the
    // bulk path wants, and an open-ended `bytes=N-` is needed only when N is not
    // zero.
    if (len != BlobStore::kReadToEnd) {
        if (len == 0) return Buffer{};
        std::ostringstream range;
        range << "bytes=" << offset << "-" << (offset + len - 1);
        request.SetRange(range.str());
    } else if (offset != 0) {
        std::ostringstream range;
        range << "bytes=" << offset << "-";
        request.SetRange(range.str());
    }

    auto outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        // A range starting at or past the end is an empty read, not an error. The
        // interface says so and the local store behaves that way, so the remote
        // one must too or the same call means different things per tier.
        if (outcome.GetError().GetResponseCode() ==
            Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE) {
            return Buffer{};
        }
        return std::unexpected(classify(outcome.GetError()));
    }

    auto& stream = outcome.GetResult().GetBody();
    Buffer out;
    const int64_t length = outcome.GetResult().GetContentLength();
    if (length > 0) out.reserve(static_cast<size_t>(length));
    char chunk[64 * 1024];
    while (stream.read(chunk, sizeof(chunk)) || stream.gcount() > 0) {
        out.insert(out.end(), chunk, chunk + stream.gcount());
        if (stream.eof()) break;
    }
    return out;
}

/// `bulk_view()` must return a `BlobStore&`, so the bulk profile needs an object.
/// It forwards to the owner with the bulk client selected — no duplicated request
/// logic, and no second set of retry and addressing settings to keep in step.
class BulkFacade final : public BlobStore {
public:
    BulkFacade(S3BlobStore& owner, S3BlobStore::Impl& impl) : owner_(owner), impl_(impl) {}

    std::string id() const override { return impl_.id; }
    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
        return make_ready_future(do_get(impl_, *impl_.bulk_client, name, offset, len));
    }
    std::future<Status> put(std::string_view name, Slice bytes) override {
        return owner_.put(name, bytes);
    }
    std::future<Status> remove(std::string_view name) override { return owner_.remove(name); }
    std::future<Status> remove_many(const std::vector<std::string>& names) override {
        return owner_.remove_many(names);
    }
    std::future<ListResult> list(std::string_view prefix) override { return owner_.list(prefix); }
    /// Already the bulk view; returning `*this` keeps a chain from forming.
    BlobStore& bulk_view() override { return *this; }

private:
    S3BlobStore& owner_;
    S3BlobStore::Impl& impl_;
};

}  // namespace

S3BlobStore::S3BlobStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
S3BlobStore::~S3BlobStore() = default;

Result<std::shared_ptr<S3BlobStore>> S3BlobStore::open(S3Options options) {
    if (options.bucket.empty()) return std::unexpected(Status::Config);
    // A trailing slash would produce `prefix//name`, which S3 accepts as a
    // distinct key and which nothing in the engine would ever generate again.
    while (!options.prefix.empty() && options.prefix.back() == '/') options.prefix.pop_back();

    auto impl = std::make_unique<Impl>();
    impl->options = options;
    impl->id = options.id.empty()
                   ? "s3://" + options.bucket + (options.prefix.empty() ? "" : "/" + options.prefix)
                   : options.id;

    const auto make_client = [&options](std::chrono::milliseconds timeout) {
        Aws::Client::ClientConfiguration config;
        config.region = options.region;
        config.requestTimeoutMs = static_cast<long>(timeout.count());
        config.connectTimeoutMs = static_cast<long>(std::min<long long>(timeout.count(), 5'000));

        const bool overridden = !options.endpoint.empty();
        if (overridden) {
            config.endpointOverride = options.endpoint;
            config.scheme = options.endpoint.rfind("https://", 0) == 0 ? Aws::Http::Scheme::HTTPS
                                                                      : Aws::Http::Scheme::HTTP;
        }
        // Virtual-host addressing needs per-bucket DNS, which a test endpoint
        // does not have; path style is the only thing that resolves there.
        const bool virtual_host = !overridden;

        if (options.access_key.empty()) {
            return std::make_shared<Aws::S3::S3Client>(
                config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, virtual_host);
        }
        return std::make_shared<Aws::S3::S3Client>(
            Aws::Auth::AWSCredentials(options.access_key, options.secret_key), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, virtual_host);
    };

    impl->point_client = make_client(options.point_timeout);
    impl->bulk_client = make_client(options.bulk_timeout);

    Impl* raw = impl.get();
    std::shared_ptr<S3BlobStore> store(new S3BlobStore(std::move(impl)));
    raw->bulk_facade = std::make_unique<BulkFacade>(*store, *raw);
    return store;
}

std::string S3BlobStore::id() const { return impl_->id; }

BlobStore& S3BlobStore::bulk_view() { return *impl_->bulk_facade; }

std::future<GetResult> S3BlobStore::get(std::string_view name, uint64_t offset, size_t len) {
    return make_ready_future(do_get(*impl_, *impl_->point_client, name, offset, len));
}

namespace {

/// S3's own floor: every part except the last must be at least 5 MiB.
constexpr size_t kMinPartBytes = 5ull << 20;

/// A 412 means the name is already taken, which under the no-reuse rule means a zombie
/// writer reusing file numbers — terminal, and the status the blob-store contract pins.
Status classify_conditional_put(Aws::Http::HttpResponseCode code) {
    return code == Aws::Http::HttpResponseCode::PRECONDITION_FAILED ? Status::Unusable : Status::Io;
}

}  // namespace

/// A multipart upload, on the bulk client: an object over the threshold is by definition a
/// bulk write, and the point client's timeout would invent failures on it.
Status S3BlobStore::multipart_put(std::string_view name, Slice bytes) {
    const std::string key = impl_->key_for(name);
    const size_t part_bytes = std::max(impl_->options.multipart_part_bytes, kMinPartBytes);

    Aws::S3::Model::CreateMultipartUploadRequest create;
    create.SetBucket(impl_->options.bucket);
    create.SetKey(key);
    auto created = impl_->bulk_client->CreateMultipartUpload(create);
    if (!created.IsSuccess()) return Status::Io;
    const Aws::String upload_id = created.GetResult().GetUploadId();

    // **Aborted on every failing path below.** S3 charges for the parts of an incomplete
    // upload until something removes them, so an early return that skipped this would be a
    // recurring bill for an operation that already failed.
    const auto abort = [&] {
        Aws::S3::Model::AbortMultipartUploadRequest request;
        request.SetBucket(impl_->options.bucket);
        request.SetKey(key);
        request.SetUploadId(upload_id);
        (void)impl_->bulk_client->AbortMultipartUpload(request);
    };

    Aws::S3::Model::CompletedMultipartUpload completed;
    int part_number = 1;
    for (size_t offset = 0; offset < bytes.size(); offset += part_bytes, ++part_number) {
        const size_t length = std::min(part_bytes, bytes.size() - offset);

        Aws::S3::Model::UploadPartRequest part;
        part.SetBucket(impl_->options.bucket);
        part.SetKey(key);
        part.SetUploadId(upload_id);
        part.SetPartNumber(part_number);
        auto body = Aws::MakeShared<Aws::StringStream>("elysiumkv");
        body->write(reinterpret_cast<const char*>(bytes.data()) + offset,
                    static_cast<std::streamsize>(length));
        part.SetBody(body);
        part.SetContentLength(static_cast<long long>(length));

        auto uploaded = impl_->bulk_client->UploadPart(part);
        if (!uploaded.IsSuccess()) {
            abort();
            return Status::Io;
        }
        completed.AddParts(Aws::S3::Model::CompletedPart()
                               .WithETag(uploaded.GetResult().GetETag())
                               .WithPartNumber(part_number));
    }

    Aws::S3::Model::CompleteMultipartUploadRequest complete;
    complete.SetBucket(impl_->options.bucket);
    complete.SetKey(key);
    complete.SetUploadId(upload_id);
    complete.SetMultipartUpload(completed);
    // Write-once, on the same terms as the single-PUT path. Verified against the endpoint
    // before this was relied on.
    complete.SetIfNoneMatch("*");

    auto finished = impl_->bulk_client->CompleteMultipartUpload(complete);
    if (finished.IsSuccess()) return Status::Ok;
    // Including the write-once rejection: the upload exists and its parts are billable
    // until aborted, and "the name was taken" is no reason to leave them.
    abort();
    return classify_conditional_put(finished.GetError().GetResponseCode());
}

std::future<Status> S3BlobStore::put(std::string_view name, Slice bytes) {
    if (!is_valid_object_name(name)) return make_ready_future(Status::Config);

    if (bytes.size() >= impl_->options.multipart_threshold_bytes) {
        return make_ready_future(multipart_put(name, bytes));
    }

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(impl_->options.bucket);
    request.SetKey(impl_->key_for(name));

    // Write-once enforced by the store rather than trusted of the caller. File
    // numbers are monotonic under a single writer, so a collision means a zombie
    // process is reusing them; the conditional costs nothing and converts a
    // silent overwrite into an error the engine can act on.
    request.SetIfNoneMatch("*");

    auto body = Aws::MakeShared<Aws::StringStream>("elysiumkv");
    body->write(reinterpret_cast<const char*>(bytes.data()),
                static_cast<std::streamsize>(bytes.size()));
    request.SetBody(body);
    request.SetContentLength(static_cast<long long>(bytes.size()));

    auto outcome = impl_->point_client->PutObject(request);
    if (outcome.IsSuccess()) return make_ready_future(Status::Ok);
    // A rejected conditional means the name is already taken, which under the
    // no-reuse rule means a zombie writer reusing file numbers. Terminal, not
    // transient, so it must not look retryable.
    //
    // **`Unusable`, which is what the blob-store contract already requires** — this
    // returned `Config` while `LocalFileBlobStore` returned `Unusable` for the
    // identical condition. The contract suite pins it (`PutAtAnExistingName-
    // NeverOverwrites`), and this implementation was not being run through it,
    // which is exactly how the divergence survived being written down.
    return make_ready_future(classify_conditional_put(outcome.GetError().GetResponseCode()));
}

std::future<Status> S3BlobStore::remove(std::string_view name) {
    if (!is_valid_object_name(name)) return make_ready_future(Status::Config);
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(impl_->options.bucket);
    request.SetKey(impl_->key_for(name));
    auto outcome = impl_->point_client->DeleteObject(request);
    if (outcome.IsSuccess()) return make_ready_future(Status::Ok);
    // Removing something absent is success — S3 says so, and the interface
    // requires remove to be idempotent.
    const Status status = classify(outcome.GetError());
    return make_ready_future(status == Status::NotFound ? Status::Ok : status);
}

std::future<Status> S3BlobStore::remove_many(const std::vector<std::string>& names) {
    if (names.empty()) return make_ready_future(Status::Ok);

    // DeleteObjects caps at 1000 keys, and exceeding it is a 400 rather than a
    // partial delete — a hard boundary, not a tuning choice.
    constexpr size_t kBatch = 1000;
    for (size_t start = 0; start < names.size(); start += kBatch) {
        Aws::S3::Model::Delete payload;
        for (size_t i = start; i < std::min(start + kBatch, names.size()); ++i) {
            if (!is_valid_object_name(names[i])) return make_ready_future(Status::Config);
            Aws::S3::Model::ObjectIdentifier id;
            id.SetKey(impl_->key_for(names[i]));
            payload.AddObjects(std::move(id));
        }
        Aws::S3::Model::DeleteObjectsRequest request;
        request.SetBucket(impl_->options.bucket);
        request.SetDelete(std::move(payload));

        auto outcome = impl_->point_client->DeleteObjects(request);
        if (!outcome.IsSuccess()) return make_ready_future(Status::Io);
        // Per-key failures arrive *inside* a 200. Ignoring them would report
        // success for objects still present, and the caller would stop trying.
        if (!outcome.GetResult().GetErrors().empty()) return make_ready_future(Status::Io);
    }
    return make_ready_future(Status::Ok);
}

std::future<ListResult> S3BlobStore::list(std::string_view prefix) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(impl_->options.bucket);
    request.SetPrefix(prefix.empty() && impl_->options.prefix.empty() ? std::string()
                                                                     : impl_->key_for(prefix));

    std::vector<std::string> names;
    std::string token;
    do {
        if (!token.empty()) request.SetContinuationToken(token);
        auto outcome = impl_->point_client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            // **Never absence.** An empty list in an existing bucket is a
            // meaningful empty result; a failure to look is not, and conflating
            // them is how a store loses data that was never lost.
            return make_ready_future(ListResult(std::unexpected(Status::Io)));
        }
        for (const auto& object : outcome.GetResult().GetContents()) {
            if (auto name = impl_->name_for(object.GetKey())) names.push_back(*name);
        }
        // Pagination is not optional: S3 returns at most 1000 keys per page, and
        // a mature store has thousands of SSTs. Stopping at the first page would
        // omit the rest — which the engine reads as loss.
        token = outcome.GetResult().GetIsTruncated()
                    ? outcome.GetResult().GetNextContinuationToken()
                    : std::string();
    } while (!token.empty());

    std::sort(names.begin(), names.end());
    return make_ready_future(ListResult(std::move(names)));
}

}  // namespace elysiumkv
