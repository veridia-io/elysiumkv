#include "encryption.hpp"

#include "elysiumkv/aes256_gcm_encryption_provider.hpp"
#include "elysiumkv/static_encryption_key_manager.hpp"

#ifdef ELYSIUMKV_WITH_AWS
#include "elysiumkv/aws_kms_encryption_key_manager.hpp"
#endif

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string_view>

namespace elysiumkv::cli {
namespace {

std::string trimmed(std::string_view text) {
    const auto first = text.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) return {};
    return std::string(text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1));
}

std::string read_key_file(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw CLI::ValidationError("cannot read the key file '" + path + "'");
    std::ostringstream all;
    all << in.rdbuf();
    return trimmed(all.str());
}

std::shared_ptr<EncryptionKeyManager> keys_from_hex(std::string_view hex, const std::string& spec) {
    auto keys = StaticEncryptionKeyManager::from_hex(hex);
    if (!keys) {
        // Never the key itself, and never its length: both narrow a search for it.
        throw CLI::ValidationError("'" + spec +
                                   "' is not a usable master key (it must be 32 bytes, hex)");
    }
    return *keys;
}

std::shared_ptr<EncryptionKeyManager> open_keys(const std::string& spec,
                                                const EncryptionOptions& options) {
    const auto colon = spec.find(':');
    if (colon == std::string::npos) {
        throw CLI::ValidationError(
            "'" + spec + "' must name where the key comes from: hex:, file:, env: or kms:");
    }
    const std::string kind = spec.substr(0, colon);
    const std::string value = spec.substr(colon + 1);

    if (kind == "hex") return keys_from_hex(value, spec);
    if (kind == "file") return keys_from_hex(read_key_file(value), spec);
    if (kind == "env") {
        const char* hex = std::getenv(value.c_str());
        if (hex == nullptr) throw CLI::ValidationError("$" + value + " is not set");
        return keys_from_hex(trimmed(hex), spec);
    }
    if (kind == "kms") {
#ifdef ELYSIUMKV_WITH_AWS
        KmsOptions kms;
        kms.key_id = value;
        if (!options.kms_region.empty()) kms.region = options.kms_region;
        kms.endpoint = options.kms_endpoint;
        auto keys = AwsKmsEncryptionKeyManager::open(std::move(kms));
        if (!keys) throw CLI::ValidationError("could not open KMS for key '" + value + "'");
        return *keys;
#else
        (void)options;
        throw CLI::ValidationError(
            "this build has no AWS support, so kms: keys are unavailable "
            "(build with -DELYSIUMKV_BUILD_AWS=ON)");
#endif
    }
    throw CLI::ValidationError("'" + kind + "' is not a key source: use hex:, file:, env: or kms:");
}

}  // namespace

void add_encryption_flags(CLI::App& command, EncryptionOptions& options) {
    command
        .add_option("--encryption-provider", options.providers,
                    "an encrypted store's provider id and its key, as "
                    "`<id> hex:<hex>|file:<path>|env:<VAR>|kms:<key-id>`; repeatable")
        ->type_size(2)
        ->type_name("ID KEY");
    command.add_option("--encryption-kms-region", options.kms_region, "region for a kms: key");
    command.add_option("--encryption-kms-endpoint", options.kms_endpoint,
                       "override, e.g. LocalStack");
}

ProviderRegistry open_registry(const EncryptionOptions& options) {
    // The passthrough under its reserved id, and primary left as it is: a command never writes, so
    // the primary is only ever the id a payload records.
    ProviderRegistry registry = passthrough_registry();

    for (size_t i = 0; i + 1 < options.providers.size(); i += 2) {
        const std::string& id = options.providers[i];
        if (id.empty()) {
            throw CLI::ValidationError("the empty provider id is reserved for the passthrough");
        }
        auto provider = Aes256GcmEncryptionProvider::open(open_keys(options.providers[i + 1],
                                                                    options));
        if (!provider) throw CLI::ValidationError("could not open a provider for '" + id + "'");
        registry.providers[id] = *provider;
    }
    return registry;
}

}  // namespace elysiumkv::cli
