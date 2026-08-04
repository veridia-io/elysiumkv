#ifndef ELYSIUMKV_CACHE_BLOCK_HPP
#define ELYSIUMKV_CACHE_BLOCK_HPP

#include "elysiumkv/slice.hpp"

#include <utility>

namespace elysiumkv {

/// A decompressed block body — `content` in the grammar, with the restart
/// array still on the end. Immutable once constructed, handed out as
/// `shared_ptr<const Block>` so a reader's use outlives eviction (ARCHITECTURE.md "Reads don't copy"), which
/// is what backs `Pinned` at the public API.
class Block {
public:
    explicit Block(Buffer content) : content_(std::move(content)) {}

    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    Slice content() const { return Slice(content_.data(), content_.size()); }
    size_t size() const { return content_.size(); }

private:
    Buffer content_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_CACHE_BLOCK_HPP
