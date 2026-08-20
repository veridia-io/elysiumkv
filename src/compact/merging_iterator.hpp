#ifndef ELYSIUMKV_COMPACT_MERGING_ITERATOR_HPP
#define ELYSIUMKV_COMPACT_MERGING_ITERATOR_HPP

#include "sst/format.hpp"
#include "sst/internal_iterator.hpp"

#include <memory>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Positional recency" — recency is positional. With no sequence numbers anywhere, the merge
/// resolves duplicates by the order children are given: earlier wins. Callers
/// stack them newest-first — memtable, then L0 by descending file number, then
/// L1 and below, which are non-overlapping by construction.
///
/// Emits one entry per distinct key, tombstones included; dropping them is
/// compaction's decision (ARCHITECTURE.md "Compaction") and hiding them is the public iterator's.
///
/// One direction per scan. Forward (seek_to_first/seek/next) and reverse
/// (seek_to_last/seek_for_prev/prev) are each self-consistent, but turning around mid-scan is not
/// supported: after a forward step the children that shared the emitted key sit *past* it, and a
/// reverse pick would then choose one of them. Turning would mean re-seeking every child to the
/// emitted key, which nothing has asked for.
/// `child_ranges`, when given, is parallel to `children`: the range tombstones each child carries,
/// already a sorted disjoint cover. An entry is suppressed when a strictly earlier child covers
/// its key — earlier meaning newer, so a tombstone shadows older children and never the one that
/// carries it. That is the same rule the file format states, applied to whatever set of sources this
/// merge happens to hold, which is why compaction gets the dropping for free: it merges a subset,
/// and only the entries in that subset are dropped.
std::unique_ptr<InternalIterator> make_merging_iterator(
    std::vector<std::unique_ptr<InternalIterator>> children,
    std::vector<std::vector<RangeTombstone>> child_ranges = {});

}  // namespace elysiumkv

#endif  // ELYSIUMKV_COMPACT_MERGING_ITERATOR_HPP
