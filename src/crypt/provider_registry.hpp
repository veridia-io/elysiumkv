#ifndef ELYSIUMKV_CRYPT_PROVIDER_REGISTRY_HPP
#define ELYSIUMKV_CRYPT_PROVIDER_REGISTRY_HPP

#include "elysiumkv/encryption.hpp"
#include "elysiumkv/no_encryption_provider.hpp"

#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace elysiumkv {

/// The registered providers and which one writes.
///
/// One resolved copy, shared by everything that encrypts. SST objects and manifest payloads
/// route on the same map, so a file and the edit that records it can never disagree about what a
/// provider id means. Built once at open, after the validation that guarantees `primary` names a
/// registered provider and that none of them is null.
struct ProviderRegistry {
    std::map<std::string, std::shared_ptr<EncryptionProvider>> providers;
    std::string primary;
    bool accept_plaintext = false;

    EncryptionProvider* find(const std::string& id) const {
        const auto found = providers.find(id);
        return found == providers.end() ? nullptr : found->second.get();
    }

    /// Never null once `open` has validated the configuration.
    EncryptionProvider* primary_provider() const { return find(primary); }
};

/// The registry an unencrypted store has: the passthrough alone, primary. Not a null registry
/// — there is no configuration in which nothing is registered, so nothing downstream needs a branch
/// for one.
inline ProviderRegistry passthrough_registry() {
    ProviderRegistry registry;
    registry.providers.emplace(std::string(kNoEncryptionProviderId),
                               std::make_shared<NoEncryptionProvider>());
    return registry;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_CRYPT_PROVIDER_REGISTRY_HPP
