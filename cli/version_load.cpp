#include "version_load.hpp"

#include "commands.hpp"

#include "version/version_edit.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace elysiumkv::cli {

/// Snapshot, then every edit above it, exactly as recovery does.
std::shared_ptr<const Version> load_version(ManifestCatalog& catalog, uint64_t generation,
                                    size_t& edits_replayed) {
    auto snapshot_bytes = catalog.get_snapshot(generation).get();
    if (!snapshot_bytes) fail("that generation has no snapshot");
    auto snapshot = decode_version_snapshot(Slice(snapshot_bytes->data(), snapshot_bytes->size()));
    if (!snapshot) {
        // Unsupported rather than Corrupt is the interesting case: the bytes are fine and this
        // binary is the wrong one, which is a different instruction to the operator.
        fail("the snapshot did not decode (status " +
             std::to_string(static_cast<int>(snapshot.error())) + ") — a newer format, or damage");
    }

    std::vector<std::vector<FileMetadata>> levels;
    for (const FileMetadata& file : snapshot->files) {
        const auto level = static_cast<size_t>(file.level);
        if (levels.size() <= level) levels.resize(level + 1);
        levels[level].push_back(file);
    }
    std::map<int, std::string> pointers(snapshot->compaction_pointers.begin(),
                                        snapshot->compaction_pointers.end());
    auto version = std::make_shared<const Version>(std::move(levels), snapshot->next_file_number,
                                                   std::move(pointers), snapshot->truncation_point);

    auto seqs = catalog.list_edits(generation).get();
    if (!seqs) fail("could not list the edits");
    std::sort(seqs->begin(), seqs->end());
    for (uint64_t seq : *seqs) {
        auto bytes = catalog.get_edit(generation, seq).get();
        if (!bytes) break;   // replay stops at the first unreadable record, as recovery does
        auto edit = decode_version_edit(Slice(bytes->data(), bytes->size()));
        if (!edit) break;
        version = Version::apply(*version, *edit);
        ++edits_replayed;
    }
    return version;
}

uint64_t current_generation(ManifestCatalog& catalog) {
    auto pointer = catalog.read();
    if (!pointer) fail("could not read the manifest pointer");
    if (!pointer->has_value()) fail("no manifest here: this store has never been written");
    return (*pointer)->generation;
}

}  // namespace elysiumkv::cli
