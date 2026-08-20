#ifndef ELYSIUMKV_SST_CONCAT_ITERATOR_HPP
#define ELYSIUMKV_SST_CONCAT_ITERATOR_HPP

#include "sst/internal_iterator.hpp"
#include "version/version.hpp"

#include <functional>
#include <memory>
#include <vector>

namespace elysiumkv {

class SstReader;

/// One level, as a single child of the merge, opening one file at a time.
///
/// What the merge is given instead of a child per file, so building an iterator opens one file
/// rather than every file that could overlap the range — an `SstReader` each, with its index block,
/// and a round trip each on a remote tier.
///
/// Only for a level whose files are disjoint and sorted, and which carries no range tombstones.
/// Disjointness is what makes one file at a time correct: at most one can hold any key, so the file
/// the position lands in is the only one to open. Range tombstones break it for a different reason
/// — a tombstone in one file shadows entries in its *siblings*, and the merge can only apply that
/// when each file is its own child. `Version::carries_ranges` is the gate; L0 never qualifies,
/// because its files overlap by construction.
///
/// The current entry lives until the next step, as with any iterator here: crossing into the
/// next file drops the previous reader, and with it the block the last key pointed into.
/// `[begin, end)` indexes `version->files_at(level)`; the Version is held so those stay alive.
std::unique_ptr<InternalIterator> make_concat_iterator(
    std::shared_ptr<const Version> version, int level, size_t begin, size_t end,
    std::function<Result<std::shared_ptr<SstReader>>(const FileMetadata&)> open);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_CONCAT_ITERATOR_HPP
