#ifndef ELYSIUMKV_TESTS_DIFF_OP_STREAM_HPP
#define ELYSIUMKV_TESTS_DIFF_OP_STREAM_HPP

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace elysiumkv::test {

/// ARCHITECTURE.md "The differential oracle" — the op stream is a **flat list**, deliberately. Shrinking is
/// straightforward while it is one, and awkward once it has structure, which is
/// why the shrinker exists from the start rather than being added later.
struct DiffOp {
    enum class Kind {
        Put,
        Remove,
        Get,
        Batch,
        ScanAll,
        ScanRange,
        ScanPrefix,
        /// Drops every key below `key`. Durable the moment it returns, so a kill does not undo
        /// it — which is why the replay applies it to the post-crash oracle as well.
        TruncateBelow,
        /// Deletes `[key, upper)`. **Unlike a truncation this is not durable when it returns**: it
        /// is a record in the memtable, so a kill before the next flush loses it and the keys come
        /// back. The replay therefore applies it to the live oracle and *not* to the post-crash
        /// one, which is the distinction between the two operations stated as a test.
        DeleteRange,
        /// The same three scans descending. Compared against the oracle's answer reversed, so the
        /// bar is not merely "descending" but "the same set, in the opposite order" — a reverse
        /// scan that dropped or duplicated an entry passes an ordering check and fails this one.
        ReverseScanAll,
        ReverseScanRange,
        ReverseScanPrefix,
        Flush,
        /// Force every compaction the picker offers.
        Compact,
        /// Iterate half the keyspace, force a flush, finish the scan. A flush
        /// changes where data lives, never what it says.
        IterAcrossFlush,
        /// ARCHITECTURE.md "The differential oracle" and "Versions are immutable snapshots" — iterate half the keyspace, force a compaction, finish. The
        /// case VersionSet exists to protect: the iterator holds a Version, so
        /// files compaction unlinked must stay readable until it is released.
        IterAcrossCompaction,
        /// Clean close and reopen: flush first, so everything must come back.
        Reopen,
        /// The same, descending. A reverse scan pins its Version exactly as a forward one does, but
        /// it is `prev()` that runs after the files moved — and nothing else calls `prev()` while a
        /// compaction is unlinking underneath it.
        ReverseIterAcrossCompaction,
        /// ARCHITECTURE.md "Fault injection" — the process stops existing here. Everything since the last
        /// successful flush is lost, and nothing else is. Expressing kill points
        /// as positions in the op stream is what makes them reproduce from a
        /// seed like any other operation.
        Kill,
    };

    Kind kind = Kind::Get;
    std::string key;
    std::string value;
    std::string upper;  ///< ScanRange only
    /// Batch only: (is_delete, key, value).
    std::vector<std::tuple<bool, std::string, std::string>> batch;

    std::string describe() const;
};

struct GeneratorOptions {
    int distinct_keys = 2000;
    /// Kill points are only generated for the fault suite; the differential
    /// suite reopens cleanly instead.
    bool allow_kills = false;
};

std::vector<DiffOp> generate_ops(uint64_t seed, int count, GeneratorOptions options = {});

/// Renders an op list as something a person can read and re-enter by hand — the
/// point of shrinking is a sequence you can look at.
std::string describe_ops(const std::vector<DiffOp>& ops);

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_DIFF_OP_STREAM_HPP
