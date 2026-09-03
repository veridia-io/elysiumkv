#include "version_load.hpp"

#include "commands.hpp"

#include "version/manifest_payload.hpp"
#include "version/version_edit.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace elysiumkv::cli {
namespace {

std::string named(Status status) { return std::string(status_name(status)); }

/// `ManifestPayload::open` fills `why` only for the failures configuration can fix — a provider
/// this process does not hold, a key that is not the one the store was written with. The rest are
/// bytes that are not what was written, and have only a status to report.
std::string payload_error(const char* what, Status status, const std::string& why) {
    if (!why.empty()) return why;
    return std::string("the ") + what + " payload did not open (" + named(status) + ")";
}

}  // namespace

/// Snapshot, then every edit above it, exactly as recovery does — including stopping at the first
/// gap, because an edit above one was never acknowledged and a reopen would not apply it either.
std::shared_ptr<const Version> load_version(ManifestCatalog& catalog,
                                            const ProviderRegistry& encryption, uint64_t generation,
                                            size_t& edits_replayed) {
    auto snapshot_bytes = catalog.get_snapshot(generation).get();
    if (!snapshot_bytes) {
        // A collected generation is a different instruction to an operator than an unreachable or
        // forbidden store, and one sentence for both sent them looking at their data for a defect
        // in this tool.
        if (snapshot_bytes.error() == Status::NotFound) fail("that generation has no snapshot");
        fail("could not fetch the snapshot (" + named(snapshot_bytes.error()) + ")");
    }

    // Every manifest payload is framed, whether or not the store is encrypted, so this is on the
    // way to every decode rather than a branch for encrypted stores.
    std::string why;
    auto snapshot_plain = ManifestPayload::open(encryption, generation,
                                                ManifestPayload::snapshot_address(generation),
                                                Slice::from(*snapshot_bytes), why);
    if (!snapshot_plain) fail(payload_error("snapshot", snapshot_plain.error(), why));

    auto snapshot = decode_version_snapshot(Slice::from(*snapshot_plain));
    if (!snapshot) {
        // Unsupported rather than Corrupt is the interesting case: the bytes are fine and this
        // binary is the wrong one, which is a different instruction to the operator.
        fail("the snapshot did not decode (" + named(snapshot.error()) +
             ") — a newer format, or damage");
    }

    std::map<int, std::string> pointers;
    for (const auto& [level, key] : snapshot->compaction_pointers) pointers[level] = key;

    VersionEdit initial;
    initial.added = std::move(snapshot->files);
    auto version = Version::apply(Version({}, snapshot->next_file_number, std::move(pointers),
                                          snapshot->truncation_point, snapshot->watermark_floor),
                                  initial);

    auto seqs = catalog.list_edits(generation).get();
    if (!seqs) fail("could not list the edits (" + named(seqs.error()) + ")");
    std::sort(seqs->begin(), seqs->end());

    uint64_t expected_seq = 1;
    for (uint64_t seq : *seqs) {
        if (seq != expected_seq) break;
        const bool tail = seq == seqs->back();
        auto bytes = catalog.get_edit(generation, seq).get();
        if (!bytes) {
            if (bytes.error() == Status::NotFound && tail) break;
            if (bytes.error() == Status::NotFound) {
                fail("manifest generation " + std::to_string(generation) + " edit " +
                     std::to_string(seq) + " is missing below the listed tail");
            }
            fail("could not fetch edit " + std::to_string(seq) + " (" + named(bytes.error()) + ")");
        }

        std::string edit_why;
        auto plain = ManifestPayload::open(encryption, generation,
                                           ManifestPayload::edit_address(generation, seq),
                                           Slice::from(*bytes), edit_why);
        // A payload this process cannot route is not a torn write. Stopping there would report a
        // history that ends where the operator's configuration ends, which reads as data loss.
        if (!plain && (plain.error() == Status::Config || plain.error() == Status::Unsupported)) {
            fail(payload_error("edit", plain.error(), edit_why));
        }
        if (!plain) {
            if (tail && plain.error() == Status::Corrupt) break;
            fail("manifest generation " + std::to_string(generation) + " edit " +
                 std::to_string(seq) + " is corrupt below the listed tail");
        }

        auto edit = decode_version_edit(Slice::from(*plain));
        if (!edit) {
            if (tail && edit.error() == Status::Corrupt) break;
            fail("manifest generation " + std::to_string(generation) + " edit " +
                 std::to_string(seq) + " did not decode (" + named(edit.error()) + ")");
        }
        version = Version::apply(*version, *edit);
        ++expected_seq;
        ++edits_replayed;
    }
    return version;
}

uint64_t current_generation(ManifestCatalog& catalog) {
    auto pointer = catalog.read();
    if (!pointer) fail("could not read the manifest pointer (" + named(pointer.error()) + ")");
    if (!pointer->has_value()) fail("no manifest here: this store has never been written");
    return (*pointer)->generation;
}

}  // namespace elysiumkv::cli
