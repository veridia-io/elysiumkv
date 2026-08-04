#ifndef ELYSIUMKV_AWS_PROBE_SUPPORT_HPP
#define ELYSIUMKV_AWS_PROBE_SUPPORT_HPP

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>

#include <string>

namespace elysiumkv_probe {

/// `S3BlobStore` and `S3ManifestCatalog` deliberately have no create-bucket
/// option — a bucket is provisioned infrastructure, and creating one silently
/// would hide a misconfigured name behind a working store. The probes still have
/// to run against a fresh LocalStack, so they create their own bucket here with a
/// raw client. Each probe holds the SDK initialised through the object it opened,
/// so this needs no guard of its own; call it after opening.
inline void ensure_bucket(const char* endpoint, const std::string& bucket) {
    Aws::Client::ClientConfiguration config;
    config.region = "us-east-1";
    config.endpointOverride = endpoint;
    config.scheme = Aws::Http::Scheme::HTTP;
    // Virtual-host addressing needs per-bucket DNS, which a test endpoint does
    // not have.
    Aws::S3::S3Client client(Aws::Auth::AWSCredentials("test", "test"), config,
                             Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                             /*useVirtualAddressing=*/false);
    Aws::S3::Model::CreateBucketRequest create;
    create.SetBucket(bucket);
    (void)client.CreateBucket(create);  // already-exists is fine
}

}  // namespace elysiumkv_probe

#endif  // ELYSIUMKV_AWS_PROBE_SUPPORT_HPP
