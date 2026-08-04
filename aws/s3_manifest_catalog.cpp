#include "elysiumkv/s3_manifest_catalog.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <zstd.h>

#include <algorithm>
#include <charconv>
#include <cstdio>
#include <mutex>
#include <utility>

namespace elysiumkv {
namespace {

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

std::string generation_prefix(uint64_t generation) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "gen-%012llu", static_cast<unsigned long long>(generation));
    return buf;
}

std::string edit_suffix(uint64_t seq) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "edit-%012llu", static_cast<unsigned long long>(seq));
    return buf;
}

/// Snapshots are read whole, so **whole-object compression is correct here** —
/// the opposite of an SST, where it would make a ranged block read fetch and
/// inflate the entire file. Edits are small and left alone.
///
/// The uncompressed length is prefixed rather than relying on
/// `ZSTD_getFrameContentSize`, which is not guaranteed to be present in a frame.
Status compress(Slice content, std::string& out) {
    const size_t bound = ZSTD_compressBound(content.size());
    out.resize(8 + bound);
    const uint64_t original = content.size();
    for (int i = 0; i < 8; ++i) out[static_cast<size_t>(i)] = static_cast<char>(original >> (8 * i));
    const size_t written =
        ZSTD_compress(out.data() + 8, bound, content.data(), content.size(), 3);
    if (ZSTD_isError(written) != 0) return Status::Io;
    out.resize(8 + written);
    return Status::Ok;
}

GetResult decompress(const Buffer& raw) {
    if (raw.size() < 8) return std::unexpected(Status::Corrupt);
    uint64_t original = 0;
    for (int i = 0; i < 8; ++i) original |= static_cast<uint64_t>(raw[static_cast<size_t>(i)])
                                            << (8 * i);
    // A corrupted length must not become an allocation request. Snapshots are
    // large but bounded; a gigabyte here means the bytes are wrong.
    if (original > (1ull << 30)) return std::unexpected(Status::Corrupt);

    Buffer out(original);
    if (original > 0) {
        const size_t produced =
            ZSTD_decompress(out.data(), original, raw.data() + 8, raw.size() - 8);
        if (ZSTD_isError(produced) != 0 || produced != original) {
            return std::unexpected(Status::Corrupt);
        }
    }
    return out;
}

template <typename Error>
Status classify(const Error& error) {
    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
        error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
        return Status::NotFound;
    }
    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_BUCKET) return Status::Io;
    if (error.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) return Status::NotFound;
    return Status::Io;
}

/// S3 returns the ETag quoted. The quotes are part of the header value and must
/// go back out verbatim on `If-Match`, so the token is stored exactly as
/// received — normalising it here and re-adding quotes there would be two places
/// to get the same thing wrong.
std::string etag_of(const std::string& etag) { return etag; }

}  // namespace

struct S3ManifestCatalog::Impl {
    SdkGuard sdk;
    S3Options options;
    std::shared_ptr<Aws::S3::S3Client> client;

    std::string key(const std::string& suffix) const {
        return options.prefix.empty() ? suffix : options.prefix + "/" + suffix;
    }
    std::string pointer_key() const { return key("CURRENT"); }
    std::string snapshot_key(uint64_t generation) const {
        return key(generation_prefix(generation) + "/snapshot");
    }
    std::string edit_key(uint64_t generation, uint64_t seq) const {
        return key(generation_prefix(generation) + "/" + edit_suffix(seq));
    }

    GetResult fetch(const std::string& object_key) {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(options.bucket);
        request.SetKey(object_key);
        auto outcome = client->GetObject(request);
        if (!outcome.IsSuccess()) return std::unexpected(classify(outcome.GetError()));

        auto& stream = outcome.GetResult().GetBody();
        Buffer out;
        char chunk[64 * 1024];
        while (stream.read(chunk, sizeof(chunk)) || stream.gcount() > 0) {
            out.insert(out.end(), chunk, chunk + stream.gcount());
            if (stream.eof()) break;
        }
        return out;
    }

    /// Write-once, structurally: `If-None-Match: *` means a second put at the
    /// same address fails rather than overwriting. ARCHITECTURE.md "Ownership is one compare-and-set" calls that a programming
    /// error, and this is what makes it one in fact rather than by convention.
    Status put_once(const std::string& object_key, const char* data, size_t size) {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(options.bucket);
        request.SetKey(object_key);
        request.SetIfNoneMatch("*");
        auto body = Aws::MakeShared<Aws::StringStream>("elysiumkv");
        body->write(data, static_cast<std::streamsize>(size));
        request.SetBody(body);
        request.SetContentLength(static_cast<long long>(size));

        auto outcome = client->PutObject(request);
        if (outcome.IsSuccess()) return Status::Ok;
        if (outcome.GetError().GetResponseCode() ==
            Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
            return Status::Config;
        }
        return Status::Io;
    }
};

S3ManifestCatalog::S3ManifestCatalog(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
S3ManifestCatalog::~S3ManifestCatalog() = default;

Result<std::shared_ptr<S3ManifestCatalog>> S3ManifestCatalog::open(S3Options options) {
    if (options.bucket.empty()) return std::unexpected(Status::Config);
    while (!options.prefix.empty() && options.prefix.back() == '/') options.prefix.pop_back();

    auto impl = std::make_unique<Impl>();
    impl->options = options;

    Aws::Client::ClientConfiguration config;
    config.region = options.region;
    // The pointer install sits on the commit path, so the point profile is the
    // right one here; there is no bulk traffic in a catalog.
    config.requestTimeoutMs = static_cast<long>(options.point_timeout.count());
    config.connectTimeoutMs =
        static_cast<long>(std::min<long long>(options.point_timeout.count(), 5'000));
    const bool overridden = !options.endpoint.empty();
    if (overridden) {
        config.endpointOverride = options.endpoint;
        config.scheme = options.endpoint.rfind("https://", 0) == 0 ? Aws::Http::Scheme::HTTPS
                                                                  : Aws::Http::Scheme::HTTP;
    }
    if (options.access_key.empty()) {
        impl->client = std::make_shared<Aws::S3::S3Client>(
            config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !overridden);
    } else {
        impl->client = std::make_shared<Aws::S3::S3Client>(
            Aws::Auth::AWSCredentials(options.access_key, options.secret_key), config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, !overridden);
    }

    return std::shared_ptr<S3ManifestCatalog>(new S3ManifestCatalog(std::move(impl)));
}

Result<std::optional<ManifestCatalog::Entry>> S3ManifestCatalog::read() {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(impl_->options.bucket);
    request.SetKey(impl_->pointer_key());
    auto outcome = impl_->client->GetObject(request);
    if (!outcome.IsSuccess()) {
        // No pointer means an empty store, not a damaged one — the caller
        // installs generation 1 against an empty expectation.
        if (classify(outcome.GetError()) == Status::NotFound) {
            return Result<std::optional<Entry>>(std::optional<Entry>{});
        }
        return std::unexpected(Status::Io);
    }

    auto& stream = outcome.GetResult().GetBody();
    std::string body((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    uint64_t generation = 0;
    const auto parsed =
        std::from_chars(body.data(), body.data() + body.size(), generation);
    if (parsed.ec != std::errc()) return std::unexpected(Status::Corrupt);

    return Result<std::optional<Entry>>(
        std::optional<Entry>(Entry{generation, etag_of(outcome.GetResult().GetETag())}));
}

Result<std::optional<ManifestCatalog::Entry>> S3ManifestCatalog::compare_and_set(
    std::optional<Entry> expected, uint64_t generation) {
    const std::string body = std::to_string(generation) + "\n";

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(impl_->options.bucket);
    request.SetKey(impl_->pointer_key());
    if (expected.has_value()) {
        request.SetIfMatch(expected->token);
    } else {
        // No pointer expected: the write must fail if one appeared, or two
        // processes could both believe they installed the first generation.
        request.SetIfNoneMatch("*");
    }
    auto stream = Aws::MakeShared<Aws::StringStream>("elysiumkv");
    stream->write(body.data(), static_cast<std::streamsize>(body.size()));
    request.SetBody(stream);
    request.SetContentLength(static_cast<long long>(body.size()));

    auto outcome = impl_->client->PutObject(request);
    if (outcome.IsSuccess()) {
        return Result<std::optional<Entry>>(
            std::optional<Entry>(Entry{generation, etag_of(outcome.GetResult().GetETag())}));
    }

    const auto code = outcome.GetError().GetResponseCode();

    // **412 is a lost CAS, not an error.** Another writer installed first, so
    // this process is fenced: nullopt, which the engine turns into Status::Fenced
    // and an instance that must be reopened. Retrying would be wrong — its
    // Version is stale.
    if (code == Aws::Http::HttpResponseCode::PRECONDITION_FAILED) {
        return Result<std::optional<Entry>>(std::optional<Entry>{});
    }

    // **409 is not a lost CAS.** S3 returns it when a conditional write races a
    // multipart upload, and it *is* retryable. Folding it into 412 would fence a
    // writer that never lost; folding 412 into it would retry a writer that did,
    // and the second is how two processes end up both believing they own the
    // store.
    if (code == Aws::Http::HttpResponseCode::CONFLICT) return std::unexpected(Status::Io);

    return std::unexpected(Status::Io);
}

std::future<Status> S3ManifestCatalog::put_snapshot(uint64_t generation, Slice bytes) {
    std::string packed;
    if (const Status status = compress(bytes, packed); status != Status::Ok) {
        return make_ready_future(status);
    }
    return make_ready_future(
        impl_->put_once(impl_->snapshot_key(generation), packed.data(), packed.size()));
}

std::future<GetResult> S3ManifestCatalog::get_snapshot(uint64_t generation) {
    auto raw = impl_->fetch(impl_->snapshot_key(generation));
    if (!raw) return make_ready_future(GetResult(std::unexpected(raw.error())));
    return make_ready_future(decompress(*raw));
}

std::future<Status> S3ManifestCatalog::put_edit(uint64_t generation, uint64_t seq, Slice bytes) {
    // Edits are small and appended one at a time; compressing each would cost a
    // frame header per record for nothing.
    return make_ready_future(impl_->put_once(impl_->edit_key(generation, seq),
                                             reinterpret_cast<const char*>(bytes.data()),
                                             bytes.size()));
}

std::future<GetResult> S3ManifestCatalog::get_edit(uint64_t generation, uint64_t seq) {
    return make_ready_future(impl_->fetch(impl_->edit_key(generation, seq)));
}

std::future<Result<std::vector<uint64_t>>> S3ManifestCatalog::list_edits(uint64_t generation) {
    const std::string prefix = impl_->key(generation_prefix(generation) + "/edit-");

    std::vector<uint64_t> seqs;
    std::string token;
    do {
        Aws::S3::Model::ListObjectsV2Request request;
        request.SetBucket(impl_->options.bucket);
        request.SetPrefix(prefix);
        if (!token.empty()) request.SetContinuationToken(token);

        auto outcome = impl_->client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            return make_ready_future(Result<std::vector<uint64_t>>(std::unexpected(Status::Io)));
        }
        for (const auto& object : outcome.GetResult().GetContents()) {
            const std::string& object_key = object.GetKey();
            const size_t dash = object_key.rfind('-');
            if (dash == std::string::npos) continue;
            uint64_t seq = 0;
            const char* begin = object_key.data() + dash + 1;
            if (std::from_chars(begin, object_key.data() + object_key.size(), seq).ec ==
                std::errc()) {
                seqs.push_back(seq);
            }
        }
        // A generation with more edits than one page is entirely ordinary — the
        // rolling policy is the engine's, not this layer's, so no page count can
        // be assumed.
        token = outcome.GetResult().GetIsTruncated()
                    ? outcome.GetResult().GetNextContinuationToken()
                    : std::string();
    } while (!token.empty());

    std::sort(seqs.begin(), seqs.end());
    return make_ready_future(Result<std::vector<uint64_t>>(std::move(seqs)));
}

std::future<Status> S3ManifestCatalog::delete_generation(uint64_t generation) {
    // Scoped by the generation's own prefix, so it cannot reach another
    // generation's objects however the naming evolves.
    const std::string prefix = impl_->key(generation_prefix(generation) + "/");

    std::string token;
    do {
        Aws::S3::Model::ListObjectsV2Request list;
        list.SetBucket(impl_->options.bucket);
        list.SetPrefix(prefix);
        if (!token.empty()) list.SetContinuationToken(token);

        auto listed = impl_->client->ListObjectsV2(list);
        if (!listed.IsSuccess()) return make_ready_future(Status::Io);

        Aws::S3::Model::Delete payload;
        int count = 0;
        for (const auto& object : listed.GetResult().GetContents()) {
            Aws::S3::Model::ObjectIdentifier id;
            id.SetKey(object.GetKey());
            payload.AddObjects(std::move(id));
            ++count;
        }
        if (count > 0) {
            Aws::S3::Model::DeleteObjectsRequest remove;
            remove.SetBucket(impl_->options.bucket);
            remove.SetDelete(std::move(payload));
            auto outcome = impl_->client->DeleteObjects(remove);
            if (!outcome.IsSuccess()) return make_ready_future(Status::Io);
            // Per-key failures arrive inside a 200; treating the batch as
            // successful would leave objects behind and stop the caller retrying.
            if (!outcome.GetResult().GetErrors().empty()) return make_ready_future(Status::Io);
        }
        token = listed.GetResult().GetIsTruncated()
                    ? listed.GetResult().GetNextContinuationToken()
                    : std::string();
    } while (!token.empty());

    return make_ready_future(Status::Ok);
}

}  // namespace elysiumkv
