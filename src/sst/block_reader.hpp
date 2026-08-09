#ifndef ELYSIUMKV_SST_BLOCK_READER_HPP
#define ELYSIUMKV_SST_BLOCK_READER_HPP

#include "cache/block.hpp"
#include "sst/format.hpp"
#include "sst/key_buffer.hpp"
#include "elysiumkv/status.hpp"

#include <memory>

namespace elysiumkv {

/// Forward iteration and seek over one decompressed block (ARCHITECTURE.md "Inside an SST"). Holds the
/// block alive, so key/value slices stay valid — this is what a `Pinned` value
/// at the public API ultimately borrows from.
///
/// Every decode path treats its input as untrusted: a structurally invalid block
/// reports `Status::Corrupt` rather than reading out of bounds. The CRC has
/// already run, so this is a backstop, not the primary defence.
class BlockIterator {
public:
    BlockIterator() = default;
    explicit BlockIterator(std::shared_ptr<const Block> block);

    bool valid() const { return valid_; }
    Status status() const { return status_; }

    void seek_to_first();
    /// Positions at the first entry with key >= target.
    void seek(Slice target);
    void next();

    void seek_to_last();
    /// Positions at the last entry with key <= target.
    void seek_for_prev(Slice target);
    /// Steps back one entry. The block format is forward-only — entries are prefix-compressed
    /// against their predecessor — so this re-reads from the nearest restart point at or before the
    /// current entry and scans up to it. Bounded by the restart interval, not by the block.
    void prev();

    Slice key() const { return key_.slice(); }
    Slice value() const { return value_; }
    ValueType type() const { return type_; }

    const std::shared_ptr<const Block>& block() const { return block_; }

private:
    uint32_t restart_offset(uint32_t index) const;
    uint32_t restart_before(size_t offset) const;
    void seek_to_restart(uint32_t index);
    /// Decodes the entry at `offset` into key_/value_. `shared` bytes are taken
    /// from the current key_, so callers must reset it at a restart point.
    bool parse_entry(size_t offset);
    void invalidate();

    std::shared_ptr<const Block> block_;
    const uint8_t* data_ = nullptr;
    size_t entries_end_ = 0;         // == start of the restart array
    size_t restart_array_ = 0;
    uint32_t num_restarts_ = 0;

    /// Where the current entry begins. Only prev() needs it, and only because a backward step is
    /// expressed as "find the entry whose next_offset_ is this one".
    size_t current_offset_ = 0;
    size_t next_offset_ = 0;
    /// Inline, not a std::string: rebuilding the key must not allocate (ARCHITECTURE.md "Benchmarks").
    KeyBuffer key_;
    Slice value_;
    ValueType type_ = ValueType::Put;
    bool valid_ = false;
    Status status_ = Status::Ok;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_BLOCK_READER_HPP
