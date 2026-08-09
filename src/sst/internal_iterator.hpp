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

    /// --- reverse navigation ---
    ///
    /// An iterator runs in one direction. The leaf iterators below tolerate a turn, but the
    /// merging iterator does not: reversing there means repositioning every child relative to the
    /// emitted key, and no caller has wanted it. Ask for the direction you intend at construction.
    virtual void seek_to_last() = 0;
    /// Positions at the last entry with key <= target — the mirror of seek(), and inclusive for the
    /// same reason seek() is: it answers "where does a scan from here begin", not "find this key".
    virtual void seek_for_prev(Slice target) = 0;
    virtual void prev() = 0;

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
