#ifndef ELYSIUMKV_COMPACT_MERGING_ITERATOR_HPP
#define ELYSIUMKV_COMPACT_MERGING_ITERATOR_HPP

#include "sst/internal_iterator.hpp"

#include <memory>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Positional recency" — **recency is positional.** With no sequence numbers anywhere, the merge
/// resolves duplicates by the order children are given: earlier wins. Callers
/// stack them newest-first — memtable, then L0 by descending file number, then
/// L1 and below, which are non-overlapping by construction.
///
/// Emits one entry per distinct key, tombstones included; dropping them is
/// compaction's decision (ARCHITECTURE.md "Compaction") and hiding them is the public iterator's.
std::unique_ptr<InternalIterator> make_merging_iterator(
    std::vector<std::unique_ptr<InternalIterator>> children);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_COMPACT_MERGING_ITERATOR_HPP
