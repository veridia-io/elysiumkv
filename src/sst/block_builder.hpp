#ifndef ELYSIUMKV_SST_BLOCK_BUILDER_HPP
#define ELYSIUMKV_SST_BLOCK_BUILDER_HPP

#include "sst/format.hpp"
#include "elysiumkv/slice.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Inside an SST" — entries with prefix-compressed keys and a restart point every
/// `restart_interval` entries. At a restart point `shared_len == 0`, so the full
/// key is present and binary search over the restart array is possible.
class BlockBuilder {
public:
    explicit BlockBuilder(int restart_interval);

    /// Keys must be strictly increasing; the caller guarantees it (the memtable
    /// deduplicates and compaction merges, so there is never a repeat).
    void add(Slice key, ValueType type, Slice value);

    /// Appends the restart array and returns the finished `content`. Valid until
    /// the next reset().
    Slice finish();

    void reset();

    bool empty() const { return entries_ == 0; }
    /// Includes the restart array that finish() will append.
    size_t size_estimate() const {
        return buffer_.size() + restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t);
    }
    Slice last_key() const { return Slice::from(last_key_); }

private:
    std::string buffer_;
    std::vector<uint32_t> restarts_;
    std::string last_key_;
    int restart_interval_;
    int since_restart_ = 0;
    uint64_t entries_ = 0;
    bool finished_ = false;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_BLOCK_BUILDER_HPP
