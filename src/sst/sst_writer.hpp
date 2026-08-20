#ifndef ELYSIUMKV_SST_SST_WRITER_HPP
#define ELYSIUMKV_SST_SST_WRITER_HPP

#include "sst/block_builder.hpp"
#include "sst/bloom.hpp"
#include "sst/footer.hpp"
#include "sst/internal_iterator.hpp"
#include "elysiumkv/options.hpp"
#include "elysiumkv/status.hpp"

#include <cstdint>
#include <string>

namespace elysiumkv {

struct SstOptions {
    size_t block_bytes = 4096;
    int restart_interval = 16;
    int bloom_bits_per_key = 10;
    int bloom_probes = 6;
    Compression compression = Compression::None;
};

struct SstBuildResult {
    std::string bytes;
    uint64_t num_entries = 0;
    /// How many of those entries are tombstones. Known exactly at write time,
    /// and the only thing that makes a trivial move into the bottommost level
    /// safe: a file with no tombstones has nothing to drop (ARCHITECTURE.md "Compaction").
    uint64_t num_tombstones = 0;
    std::string smallest_key;
    std::string largest_key;
    uint64_t num_range_tombstones = 0;
    /// The span the file's range tombstones cover, which is not bounded by `smallest_key` and
    /// `largest_key`: a file can delete a range it holds no keys in at all, and a reader that
    /// consulted only the data span would walk straight past the tombstone that answers its query.
    std::string smallest_range_key;
    std::string largest_range_key;
};

/// ARCHITECTURE.md "Inside an SST" — builds a whole SST in memory. A `BlobStore` has no streaming write, so
/// the object is assembled and handed to `put` as one immutable blob; with a
/// 16 MB default target file size that is the intended shape, not a compromise.
class SstWriter {
public:
    explicit SstWriter(SstOptions options = {});

    /// Keys strictly increasing. Tombstones are written like any other entry —
    /// they are dropped only when compaction output lands in the bottommost
    /// level (ARCHITECTURE.md "Compaction").
    void add(Slice key, ValueType type, Slice value);

    /// Records a range delete carried by this file. Bounds must arrive sorted and disjoint —
    /// fragmentation is the caller's job, because only the caller knows which overlapping ranges
    /// are still live.
    void add_range_tombstone(Slice lower, Slice upper);

    uint64_t num_entries() const { return num_entries_; }
    /// What the file would weigh if finished now — the input to output cutting.
    size_t estimated_bytes() const;

    Result<SstBuildResult> finish();

private:
    Status flush_data_block();

    SstOptions options_;
    BlockBuilder data_block_;
    BlockBuilder index_block_;
    BlockBuilder range_del_block_;
    BloomBuilder bloom_;
    std::string file_;
    std::string smallest_key_;
    std::string largest_key_;
    uint64_t num_entries_ = 0;
    uint64_t num_tombstones_ = 0;
    uint64_t num_range_tombstones_ = 0;
    std::string smallest_range_key_;
    std::string largest_range_key_;
    bool finished_ = false;
};

/// Drains `source` from its current position into one SST. Used by flush (over a
/// memtable iterator) and by compaction (over a merging iterator).
///
/// `drop_tombstones` is set only when the output lands in the bottommost level
/// that could contain the key — ARCHITECTURE.md "Compaction" admits no other condition, because without
/// snapshots there is nothing else to consider.
///
/// `range_tombstones` must already be a sorted disjoint cover — `merge_ranges` produces one.
Result<SstBuildResult> build_sst(InternalIterator& source, const SstOptions& options,
                                 bool drop_tombstones = false,
                                 const std::vector<RangeTombstone>& range_tombstones = {});

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_SST_WRITER_HPP
