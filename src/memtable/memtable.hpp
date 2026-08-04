#ifndef ELYSIUMKV_MEMTABLE_MEMTABLE_HPP
#define ELYSIUMKV_MEMTABLE_MEMTABLE_HPP

#include "sst/format.hpp"
#include "sst/internal_iterator.hpp"
#include "elysiumkv/slice.hpp"

#include <memory>
#include <optional>

namespace elysiumkv {

/// One resolved entry. ARCHITECTURE.md "Positional recency" — the memtable deduplicates by key on insert, so a
/// flushed SST contains exactly one entry per key and there are no sequence
/// numbers to carry.
struct Entry {
    ValueType type = ValueType::Put;
    Slice value;
};

/// ARCHITECTURE.md "A write" — mutable, keyed, ordered. Deliberately *not* the same seam as
/// BlobStore: a mutable keyed map and an immutable byte object share no contract.
class Memtable {
public:
    virtual void put(Slice key, Slice value) = 0;
    virtual void remove(Slice key) = 0;
    virtual std::optional<Entry> get(Slice key) const = 0;
    virtual std::unique_ptr<InternalIterator> ascending() const = 0;
    virtual std::unique_ptr<InternalIterator> ascending_from(Slice key) const = 0;
    virtual size_t approximate_bytes() const = 0;
    virtual ~Memtable() = default;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MEMTABLE_MEMTABLE_HPP
