#ifndef ELYSIUMKV_SST_SST_READER_HPP
#define ELYSIUMKV_SST_SST_READER_HPP

#include "cache/block.hpp"
#include "cache/block_cache.hpp"
#include "sst/block_reader.hpp"
#include "sst/footer.hpp"
#include "sst/internal_iterator.hpp"
#include "elysiumkv/blob_store.hpp"

#include <memory>
#include <optional>
#include <string>

namespace elysiumkv {

struct SstReaderOptions {
    /// Bounds `uncompressed_len` at `max(16 * block_bytes, 1 MiB)` (ARCHITECTURE.md "Inside an SST").
    size_t block_bytes = 4096;
    /// Block-cache key component; also what `evict_file` addresses.
    uint64_t file_number = 0;
    BlockCache* block_cache = nullptr;
};

/// ARCHITECTURE.md "Inside an SST" — lazy: open reads the footer, the index and the filter, and nothing else.
/// Data blocks load on demand and come back pinned.
class SstReader {
public:
    /// A value borrowed from a block. The `shared_ptr` is the pin: while it is
    /// held the bytes stay alive whatever the cache does (ARCHITECTURE.md "Reads don't copy").
    struct Found {
        std::shared_ptr<const Block> block;
        Slice value;
        ValueType type = ValueType::Put;
    };

    static Result<std::unique_ptr<SstReader>> open(BlobStore& store, std::string name,
                                                   uint64_t file_size, SstReaderOptions options);

    /// nullopt means "this file does not contain the key" — including when the
    /// filter rejected it. A tombstone is a hit whose type is Delete.
    Result<std::optional<Found>> get(Slice key);

    /// Iterates every entry, tombstones included; the merging iterator resolves
    /// them (ARCHITECTURE.md "Compaction").
    std::unique_ptr<InternalIterator> iterator();

    /// Whether one of this file's range tombstones covers `key`.
    ///
    /// **Says nothing about the file's own entries**, which is the rule that makes range tombstones
    /// implementable without sequence numbers: a range tombstone shadows everything strictly older
    /// in `(level, file_number)` order and nothing in the file that carries it. So a caller asks
    /// this only after finding no point entry here, and a hit means every *older* file is shadowed.
    Result<bool> range_deletes(Slice key);

    /// The file's range tombstones in key order, for compaction.
    Result<std::vector<RangeTombstone>> range_tombstones();

    bool has_range_tombstones() const { return footer_.range_del.length != 0; }

    uint64_t num_entries() const { return footer_.num_entries; }
    const std::string& name() const { return name_; }

    /// Reads and validates one framed block, consulting the block cache first.
    Result<std::shared_ptr<const Block>> load_block(const BlockHandle& handle);

    /// Resident bytes: the index block and the bloom filter, which are what a reader
    /// keeps alive. **The filter dominates** — 10 bits per key is ~1.25 MB for a
    /// million-entry file, which is why the reader cache is bounded by bytes rather
    /// than by a count of open files.
    size_t memory_bytes() const {
        return (index_block_ != nullptr ? index_block_->size() : 0) + filter_.size() +
               name_.size() + sizeof(SstReader);
    }

private:
    SstReader(BlobStore& store, std::string name, uint64_t file_size, SstReaderOptions options)
        : store_(store), name_(std::move(name)), file_size_(file_size), options_(options) {}

    size_t max_uncompressed() const;

    BlobStore& store_;
    std::string name_;
    uint64_t file_size_ = 0;
    SstReaderOptions options_;
    Footer footer_;
    std::shared_ptr<const Block> index_block_;
    Buffer filter_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_SST_READER_HPP
