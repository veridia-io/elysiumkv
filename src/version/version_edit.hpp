#ifndef ELYSIUMKV_VERSION_VERSION_EDIT_HPP
#define ELYSIUMKV_VERSION_VERSION_EDIT_HPP

#include "elysiumkv/options.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "The manifest is snapshots plus edits". Note what is *not* here: no sequence-number range, no smallest/largest
/// internal key — SST keys are raw user bytes (ARCHITECTURE.md "Positional recency").
struct FileMetadata {
    int level = 0;
    uint64_t file_number = 0;
    /// Which authoritative store physically holds the file (ARCHITECTURE.md "A tier is not a level"). Recorded
    /// rather than derived, so changing the level→store map cannot strand
    /// existing files.
    std::string store_id;
    std::string smallest_key;
    std::string largest_key;
    uint64_t file_bytes = 0;
    uint64_t num_entries = 0;
    /// How many entries are tombstones. Not in the list: added so a trivial
    /// move into the bottommost level can be allowed exactly when it drops
    /// nothing (ARCHITECTURE.md "Compaction"), instead of either accumulating garbage at the level holding
    /// most of the bytes or giving the optimisation up.
    uint64_t num_tombstones = 0;
    /// The codec the file was *written under* — the level's configured
    /// compression at the time. Not in the list, but the
    /// `files_stale_codec` cannot be computed without it: the per-block type
    /// byte makes a file self-describing, not cheaply queryable, and reading
    /// every file is not a stats call. Records intent, so a block that fell back
    /// to `None` for being incompressible does not count as stale.
    Compression compression = Compression::None;
    /// Wall clock of the oldest write in the file: a flushed L0 file inherits
    /// its memtable's creation time, a compaction output takes the min() over
    /// its inputs. Sole input to the recovery horizon (ARCHITECTURE.md "Migration between tiers"), and it cannot be
    /// recomputed after a restart — hence persisted.
    uint64_t min_write_time_ms = 0;

    bool operator==(const FileMetadata&) const = default;
};

struct FileRef {
    int level = 0;
    uint64_t file_number = 0;

    bool operator==(const FileRef&) const = default;
};

struct VersionEdit {
    uint64_t next_file_number = 0;
    std::vector<FileMetadata> added;
    std::vector<FileRef> deleted;
    /// ARCHITECTURE.md "Compaction" — the per-level compaction pointer — the largest key of the previous
    /// compaction at that level — must survive a restart, or the sweep restarts
    /// from the beginning of the keyspace and rewrites the same hot region.
    std::vector<std::pair<int, std::string>> compaction_pointers;

    bool empty() const {
        return added.empty() && deleted.empty() && compaction_pointers.empty() &&
               next_file_number == 0;
    }
};

/// CRC32C-framed, like an SST block (ARCHITECTURE.md "The manifest is snapshots plus edits"). Replay stops at the first record
/// that fails to decode, so the framing is what makes a torn write survivable.
std::string encode_version_edit(const VersionEdit&);
Result<VersionEdit> decode_version_edit(Slice bytes);

/// A snapshot is the complete state of one Version: every file, plus the
/// counters needed to continue.
struct VersionSnapshot {
    uint64_t next_file_number = 1;
    std::vector<FileMetadata> files;
    std::vector<std::pair<int, std::string>> compaction_pointers;
};

std::string encode_version_snapshot(const VersionSnapshot&);
Result<VersionSnapshot> decode_version_snapshot(Slice bytes);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_VERSION_EDIT_HPP
