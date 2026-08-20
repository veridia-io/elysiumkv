/* `elysiumkv` — the operator CLI.
 *
 * One binary, a command per question, in the shape of the cloud CLIs an operator already has in
 * their fingers. Commands live one per translation unit and register themselves here; adding one
 * is a new file plus a line below, and no command knows anything about the others.
 */

#include "commands.hpp"

#include "elysiumkv/elysiumkv.h"

#include <CLI/CLI.hpp>

int main(int argc, char** argv) {
    CLI::App app{"elysiumkv — inspect and operate an ElysiumKV store", "elysiumkv"};
    app.set_version_flag("--version", []() {
        // The library's own answer, not a constant compiled in here: the point of a version is to
        // say what is actually running.
        return std::string(elysiumkv_version());
    });
    // A bare invocation should teach rather than fail silently.
    app.require_subcommand(1);

    // fallthrough is what lets a global be written where it reads naturally: an operator types
    // `elysiumkv manifest --json`, not `elysiumkv --json manifest`, and without this the subcommand
    // would reject a flag it does not define. Both orders now work.
    app.fallthrough();

    elysiumkv::cli::GlobalOptions globals;
    app.add_flag("--json", globals.json, "machine-readable output");

    elysiumkv::cli::add_manifest_command(app, globals);
    elysiumkv::cli::add_stats_command(app, globals);

    CLI11_PARSE(app, argc, argv);
    return 0;
}
