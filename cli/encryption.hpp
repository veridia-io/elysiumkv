#ifndef ELYSIUMKV_CLI_ENCRYPTION_HPP
#define ELYSIUMKV_CLI_ENCRYPTION_HPP

#include "crypt/provider_registry.hpp"

#include <CLI/CLI.hpp>

#include <string>
#include <vector>

namespace elysiumkv::cli {

/// How a command reconstructs the providers a store's payloads were written under.
///
/// Routing is by the id recorded in each payload, so the id given here has to be the one the
/// writer registered — there is nothing in the bytes to guess it from. A generation may hold
/// payloads under two ids at once, because a rotation changes the primary without starting a new
/// generation, which is why more than one can be named.
struct EncryptionOptions {
    /// `--encryption-provider <id> <key>` pairs, flattened: id, key, id, key.
    std::vector<std::string> providers;

    std::string kms_region, kms_endpoint;
};

/// Adds the encryption flags to a subcommand.
void add_encryption_flags(CLI::App& command, EncryptionOptions& options);

/// The registry to read with. Always holds the passthrough, so a store written with encryption off
/// needs no flags at all. Throws `CLI::ValidationError` on a key this process cannot construct.
ProviderRegistry open_registry(const EncryptionOptions& options);

}  // namespace elysiumkv::cli

#endif
