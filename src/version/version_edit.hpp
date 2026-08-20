#ifndef ELYSIUMKV_VERSION_VERSION_EDIT_HPP
#define ELYSIUMKV_VERSION_VERSION_EDIT_HPP

#include "elysiumkv/options.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"
#include "version/watermark.hpp"

#include <optional>

#include <cstdint>
#include <algorithm>
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
    /// How many range tombstones the file carries, and the span they cover.
    ///
    /// The covered span is not bounded by `smallest_key` and `largest_key`. A file can delete a
    /// range it holds no keys in at all — a flush whose memtable saw only a `delete_range` produces
    /// exactly that — so a read path that consulted the data span alone would walk straight past
    /// the tombstone that answers its query. Recorded here so the decision to open the file's
    /// tombstone block costs no I/O.
    uint64_t num_range_tombstones = 0;
    std::string smallest_range_key;
    std::string largest_range_key;

    /// ARCHITECTURE.md "A range delete is a record, not a rewrite" — the span this file
    /// affects, data and deletions together.
    ///
    /// Not the data span, and the difference is a correctness matter rather than an efficiency
    /// one. A file can delete a range it holds no keys in — a flush whose memtable saw only a
    /// `delete_range` produces exactly that, with an empty data span. Choosing compaction inputs by
    /// the data span alone moves such a file past the very files it exists to shadow and drops it
    /// into a level where they sit *beside* it, and between two files at one level there is no
    /// recency to appeal to. The deleted keys come back.
    ///
    /// `largest_range_key` is a tombstone's exclusive upper bound used here as an inclusive one,
    /// which over-selects by at most one key. Over-selecting costs a file in a compaction; the
    /// other direction costs data.
    std::string effective_smallest() const {
        if (num_range_tombstones == 0) return smallest_key;
        if (num_entries == 0) return smallest_range_key;
        return std::min(smallest_key, smallest_range_key);
    }
    std::string effective_largest() const {
        if (num_range_tombstones == 0) return largest_key;
        if (num_entries == 0) return largest_range_key;
        return std::max(largest_key, largest_range_key);
    }

    /// Whether one of this file's range tombstones could cover `key`. Cheap: manifest data only.
    bool range_may_cover(Slice key) const {
        return num_range_tombstones != 0 && Slice::from(smallest_range_key) <= key &&
               key < Slice::from(largest_range_key);
    }
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

    /// Wall clock of the newest write in the file: a flushed L0 file takes its memtable's seal
    /// time, a compaction output the max over its inputs, a migration carries it unchanged.
    ///
    /// Separate from `min_write_time_ms` because the two answer opposite questions. Placement asks
    /// how old the file's *oldest* data is, so that a file is moved to cold storage as soon as any
    /// of it qualifies. Expiry asks how young its *newest* data is, because a file may only be
    /// dropped when everything in it has outlived the limit — using the oldest would delete data
    /// still inside it. Zero means unknown, and unknown never expires.
    uint64_t max_write_time_ms = 0;

    /// The embedder-supplied watermark interval this file's data lies within — see
    /// `WatermarkInterval` for why it is an interval and not a scalar. A flushed L0 file takes
    /// its source memtable's; a compaction output takes `min` of the lows and `max` of the highs;
    /// a migration carries it unchanged, since a byte copy does not change what the file holds.
    ///
    /// Persisted, and it cannot be recomputed after a restart — the same reason
    /// `min_write_time_ms` is persisted, and the only reason either is in the manifest.
    WatermarkInterval watermark;

    /// Which registered `EncryptionProvider` wrote this file's bytes, and whatever that provider
    /// needs to reopen them — a wrapped data key, a nonce basis, its own suite identifier.
    ///
    /// Reserved ahead of the feature that fills them. Both are written empty today. Adding
    /// fields to the manifest is free only while its format version is unreleased, and format 6 is
    /// exactly that until 0.7.0 ships; afterwards it would cost a bump and a rebuild of every store
    /// from its changelog. Reserving now is a few lines and no behaviour change.
    ///
    /// An empty provider is the reserved id of the passthrough, so a file written before encryption
    /// existed and one written with encryption disabled are the same case rather than two.
    std::string encryption_provider;
    std::string encryption_metadata;

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
    /// Everything below this key has been truncated away. Empty means "no change" — which is also
    /// the correct reading of "truncate below the empty key", since no key sorts under it.
    ///
    /// Monotone by construction: `Version::apply` takes the max, so replaying the manifest is
    /// idempotent and an out-of-order edit cannot resurrect data. That is the property that lets
    /// truncation be a single field instead of a log of ranges.
    std::string truncation_point;

    /// What this edit says about the watermark floor — see `Version::watermark_floor`.
    ///
    /// Three states rather than two, because "say nothing" and "there is no longer a floor" are
    /// different instructions: almost every edit is silent, a discard installs one, and the
    /// embedder declaring its replay complete removes one.
    enum class FloorUpdate : uint8_t { Silent, Set, Clear };
    FloorUpdate floor_update = FloorUpdate::Silent;
    /// Meaningful only when `floor_update == Set`.
    WatermarkFloor watermark_floor;

    bool empty() const {
        return added.empty() && deleted.empty() && compaction_pointers.empty() &&
               truncation_point.empty() && floor_update == FloorUpdate::Silent &&
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
    std::string truncation_point;
    std::optional<WatermarkFloor> watermark_floor;
};

std::string encode_version_snapshot(const VersionSnapshot&);
Result<VersionSnapshot> decode_version_snapshot(Slice bytes);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_VERSION_EDIT_HPP
