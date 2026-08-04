#include "elysiumkv/s3_blob_store.hpp"

#include "probe_support.hpp"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListMultipartUploadsRequest.h>

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <string>
using namespace elysiumkv;
static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("  %-52s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++failures;
}
int main() {
    const char* endpoint = std::getenv("ELYSIUMKV_S3_ENDPOINT");
    if (endpoint == nullptr) {
        std::printf("ELYSIUMKV_S3_ENDPOINT unset; skipping (start LocalStack 4.4.0 to run)\n");
        return 77;  // ctest SKIP_RETURN_CODE — never a silent pass
    }
    S3Options o;
    o.bucket = "elysiumkv-probe";
    // Write-once means a fixed prefix passes only against a pristine bucket: the
    // second run legitimately fails on `put`. The catalog smoke already learned
    // this; a per-run namespace is what makes the suite rerunnable.
    o.prefix = "store-" + std::to_string(::getpid());
    o.endpoint = endpoint;
    o.access_key = "test"; o.secret_key = "test";
    auto opened = S3BlobStore::open(o);
    if (!opened) { std::printf("open failed\n"); return 1; }
    auto& s = **opened;

    elysiumkv_probe::ensure_bucket(endpoint, o.bucket);

    check(s.id() == "s3://elysiumkv-probe/" + o.prefix, "id is stable and derived");
    check(s.put("000000000001.sst", Slice::from(std::string("hello world"))) .get() == Status::Ok, "put");
    auto got = s.get("000000000001.sst", 0, BlobStore::kReadToEnd).get();
    check(got.has_value() && std::string(got->begin(), got->end()) == "hello world", "get whole object");
    auto ranged = s.get("000000000001.sst", 6, 5).get();
    check(ranged.has_value() && std::string(ranged->begin(), ranged->end()) == "world", "ranged get returns exactly the range");
    // Unusable, not Config: a taken name means a zombie writer reusing file
    // numbers, and the blob-store contract pins that status for every store.
    check(s.put("000000000001.sst", Slice::from(std::string("again"))).get() == Status::Unusable, "put never overwrites (If-None-Match)");
    auto missing = s.get("000000000099.sst", 0, BlobStore::kReadToEnd).get();
    check(!missing.has_value() && missing.error() == Status::NotFound, "missing object is NotFound, not Io");
    auto past = s.get("000000000001.sst", 500, 10).get();
    check(past.has_value() && past->empty(), "read past the end is empty, not an error");

    check(s.put("000000000002.sst", Slice::from(std::string("two"))).get() == Status::Ok, "second put");
    auto listed = s.list("").get();
    check(listed.has_value() && listed->size() == 2 && (*listed)[0] == "000000000001.sst",
          "list strips the prefix and sorts");
    check(s.remove_many({"000000000001.sst", "000000000002.sst"}).get() == Status::Ok, "remove_many");
    auto after = s.list("").get();
    check(after.has_value() && after->empty(), "empty list succeeds and means empty");
    check(s.remove("000000000001.sst").get() == Status::Ok, "remove is idempotent");
    check(&s.bulk_view() != static_cast<BlobStore*>(&s), "bulk_view is a distinct view");
    check(s.bulk_view().id() == s.id(), "bulk_view names the same location");
    // --- multipart (ARCHITECTURE.md "The ABI boundary") ---------------------------------------------------
    //
    // Part size clamped to S3's 5 MiB floor, so 12 MiB is three parts: the smallest object
    // that exercises the multi-part path at all. The threshold is lowered to match, which is
    // what `multipart_part_bytes` exists for — otherwise this test would have to move tens of
    // megabytes to reach the code.
    {
        S3Options big = o;
        big.prefix = o.prefix + "-mp";
        big.multipart_threshold_bytes = 5u << 20;
        big.multipart_part_bytes = 5u << 20;
        auto opened_big = S3BlobStore::open(big);
        check(opened_big.has_value(), "open a store with a low multipart threshold");
        if (opened_big.has_value()) {
            auto& m = **opened_big;

            // Incompressible-ish and position-dependent, so a part written in the wrong
            // order or a range read off by a part boundary shows up as wrong bytes rather
            // than as a plausible-looking buffer.
            std::string huge(12u << 20, '\0');
            for (size_t i = 0; i < huge.size(); ++i) {
                huge[i] = static_cast<char>((i * 31u + (i >> 11)) & 0xFF);
            }

            check(m.put("000000000100.sst", Slice::from(huge)).get() == Status::Ok,
                  "multipart put (12 MiB, 3 parts)");
            auto whole = m.get("000000000100.sst", 0, BlobStore::kReadToEnd).get();
            check(whole.has_value() && whole->size() == huge.size(),
                  "multipart object round-trips whole");
            check(whole.has_value() && std::string(whole->begin(), whole->end()) == huge,
                  "and byte-identically");

            // Across the second part boundary, which is where a mis-ordered part shows.
            auto across = m.get("000000000100.sst", (10u << 20) - 8, 16).get();
            check(across.has_value() &&
                      std::string(across->begin(), across->end()) == huge.substr((10u << 20) - 8, 16),
                  "a range spanning a part boundary is exact");

            // **Write-once holds for multipart too**, which was verified of the endpoint
            // before the path was written: a multipart upload that silently overwrote would
            // break the central guarantee in the one case nobody tests by hand.
            check(m.put("000000000100.sst", Slice::from(huge)).get() == Status::Unusable,
                  "a multipart put at a taken name is refused, as a single PUT is");

            // And the refused upload was aborted rather than abandoned: S3 bills for the
            // parts of an incomplete upload until something removes them.
            Aws::Client::ClientConfiguration config;
            config.region = "us-east-1";
            config.endpointOverride = endpoint;
            config.scheme = Aws::Http::Scheme::HTTP;
            Aws::S3::S3Client raw(Aws::Auth::AWSCredentials("test", "test"), config,
                                  Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
            Aws::S3::Model::ListMultipartUploadsRequest listing;
            listing.SetBucket(big.bucket);
            listing.SetPrefix(big.prefix);
            auto pending = raw.ListMultipartUploads(listing);
            check(pending.IsSuccess() && pending.GetResult().GetUploads().empty(),
                  "no incomplete multipart upload is left behind");
        }
    }

    std::printf("%s\n", failures ? "FAILURES" : "all probes passed");
    return failures;
}
