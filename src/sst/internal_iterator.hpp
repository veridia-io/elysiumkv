#ifndef ELYSIUMKV_SST_INTERNAL_ITERATOR_HPP
#define ELYSIUMKV_SST_INTERNAL_ITERATOR_HPP

#include "sst/format.hpp"
#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

namespace elysiumkv {

/// The engine-internal iterator every source implements: memtable, SST, and the
/// merging iterator that stacks them. Distinct from the public `Iterator` (ARCHITECTURE.md "Absence is an answer, not an error"),
/// which is next()-only and hides tombstones.
class InternalIterator {
public:
    virtual bool valid() const = 0;
    virtual void seek_to_first() = 0;
    /// Positions at the first entry with key >= target.
    virtual void seek(Slice target) = 0;
    virtual void next() = 0;

    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual ValueType type() const = 0;
    /// Checked once iteration stops; `valid() == false` alone does not
    /// distinguish exhaustion from failure.
    virtual Status status() const = 0;

    virtual ~InternalIterator() = default;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_INTERNAL_ITERATOR_HPP
