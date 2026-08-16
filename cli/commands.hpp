#ifndef ELYSIUMKV_CLI_COMMANDS_HPP
#define ELYSIUMKV_CLI_COMMANDS_HPP

#include <CLI/CLI.hpp>

#include <iostream>
#include <string>

namespace elysiumkv::cli {

/// Reports and exits.
///
/// CLI11's `RuntimeError` carries a message that `App::exit` deliberately does **not** print — it
/// assumes whoever threw it has already said something. Throwing one directly therefore produces a
/// bare non-zero exit and no explanation, which is the worst possible failure for a tool reached
/// for when something is already wrong. This is the only way a command should fail.
[[noreturn]] inline void fail(const std::string& message, int code = 1) {
    std::cerr << "error: " << message << "\n";
    throw CLI::RuntimeError(message, code);
}

/// Flags that belong to the tool rather than to any one command.
///
/// `--json` lives here because "how do I want this answered" is not a question about *which*
/// question — every command that prints anything should accept it, and an operator should not have
/// to remember which ones do. With `fallthrough()` on the root app it may be written on either side
/// of the subcommand.
struct GlobalOptions {
    bool json = false;
};

/// Each command registers itself into the root app: one function per subcommand, declared here and
/// defined in its own translation unit. Adding a command is a new file plus one line in main.
void add_manifest_command(CLI::App& root, const GlobalOptions& globals);
void add_stats_command(CLI::App& root, const GlobalOptions& globals);

}  // namespace elysiumkv::cli

#endif
