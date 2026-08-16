#ifndef ELYSIUMKV_CLI_CATALOG_HPP
#define ELYSIUMKV_CLI_CATALOG_HPP

#include "elysiumkv/manifest_catalog.hpp"

#include <CLI/CLI.hpp>

#include <memory>
#include <string>

namespace elysiumkv::cli {

/// Which manifest catalog a command should talk to, and how to reach it.
///
/// Shared rather than per-command because every command that names a store answers the same
/// question — where does its manifest live — and an operator should not have to learn a different
/// spelling of `--table` for each one.
struct CatalogOptions {
    std::string backend;  ///< disk | dynamo | s3
    std::string dir, table, store, bucket, prefix, region = "eu-central-1", endpoint;
};

/// Adds the catalog flags to a subcommand. Kept as one call so a new command cannot accidentally
/// offer a subset of them.
void add_catalog_flags(CLI::App& command, CatalogOptions& options);

/// Opens it, or throws `CLI::ValidationError` with something an operator can act on.
std::shared_ptr<ManifestCatalog> open_catalog(const CatalogOptions& options);

}  // namespace elysiumkv::cli

#endif
