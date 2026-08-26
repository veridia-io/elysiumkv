#ifndef ELYSIUMKV_CLI_VERSION_LOAD_HPP
#define ELYSIUMKV_CLI_VERSION_LOAD_HPP

#include "elysiumkv/manifest_catalog.hpp"

#include "crypt/provider_registry.hpp"
#include "version/version.hpp"

#include <cstddef>
#include <memory>

namespace elysiumkv::cli {

/// The Version a reopen would load: snapshot, then every edit above it, as recovery does.
/// Catalog only — no blob store is touched, so it works wherever the manifest is reachable.
///
/// `encryption` must route whatever provider ids the payloads record; `open_registry` builds one.
std::shared_ptr<const Version> load_version(ManifestCatalog& catalog,
                                            const ProviderRegistry& encryption, uint64_t generation,
                                            size_t& edits_replayed);

/// The generation the pointer names.
uint64_t current_generation(ManifestCatalog& catalog);

}  // namespace elysiumkv::cli

#endif
