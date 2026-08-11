#ifndef ELYSIUMKV_MEMTABLE_MEMTABLE_HPP
#define ELYSIUMKV_MEMTABLE_MEMTABLE_HPP

#include "sst/format.hpp"
#include "sst/internal_iterator.hpp"
#include "elysiumkv/slice.hpp"

#include <memory>
#include <optional>
#include <vector>

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

    /// ARCHITECTURE.md "A range delete is a record, not a rewrite" — deletes `[lower, upper)`.
    ///
    /// **Two things happen, and both are needed.** The range is recorded, so that the SST this
    /// memtable flushes into carries it and shadows every older file. And every key the memtable
    /// *already holds* in the range is turned into a point delete right now — because a range
    /// tombstone shadows nothing in the file that carries it, and those keys are about to land in
    /// that very file.
    ///
    /// Materialising here is what lets the file format stay ordering-free. The memtable knows the
    /// order its own writes arrived in; the file does not and never needs to, because a put after
    /// this call simply overwrites the point delete, and a put before it does not.
    virtual void delete_range(Slice lower, Slice upper) = 0;

    /// Whether a recorded range covers `key`. Answers about *older* state only, for the same reason.
    virtual bool range_deletes(Slice key) const = 0;

    /// The recorded ranges, merged into a sorted disjoint cover.
    virtual std::vector<RangeTombstone> range_tombstones() const = 0;
    virtual std::optional<Entry> get(Slice key) const = 0;
    virtual std::unique_ptr<InternalIterator> ascending() const = 0;
    virtual std::unique_ptr<InternalIterator> ascending_from(Slice key) const = 0;
    virtual std::unique_ptr<InternalIterator> descending() const = 0;
    virtual std::unique_ptr<InternalIterator> descending_from(Slice key) const = 0;
    virtual size_t approximate_bytes() const = 0;
    virtual ~Memtable() = default;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_MEMTABLE_MEMTABLE_HPP
