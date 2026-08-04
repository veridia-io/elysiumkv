#ifndef ELYSIUMKV_SST_SST_READER_CACHE_HPP
#define ELYSIUMKV_SST_SST_READER_CACHE_HPP

#include "sst/sst_reader.hpp"

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace elysiumkv {

class MemoryBudget;

/// The open-`SstReader` cache: one entry per file whose footer, index block and
/// bloom filter are resident.
///
/// **It was unbounded, and the filter is what makes that expensive.** A reader holds
/// its file's bloom filter, which at the default 10 bits per key is ~1.25 MB for a
/// million-entry file; a store with a thousand such files held over a gigabyte with no
/// bound and no accounting. Every other cache in the engine reports to `MemoryBudget`
/// (ARCHITECTURE.md "A process-wide memory budget") and this one did not, so it was invisible in exactly the place a sizing
/// decision is made.
///
/// **Eviction is safe by construction, which is the only reason a bound is cheap
/// here.** Readers are handed out as `shared_ptr`, and every user — an iterator, a
/// point lookup, a compaction — holds its own. Dropping the cache's reference cannot
/// pull a reader out from under anyone; it only means the *next* open of that file
/// reads its footer, index and filter again.
///
/// That reread is also why the bound wants to be generous rather than tight: a miss
/// against a remote store is three round trips, not one, so a reader cache too small
/// for the working set is a much worse deal than the memory it saves.
class SstReaderCache {
public:
    /// `budget` is optional; when present every resident byte is reported to it and
    /// released on eviction. `max_bytes` of zero means unbounded, which exists for
    /// tests and for an embedder that genuinely wants the old behaviour.
    SstReaderCache(size_t max_bytes, MemoryBudget* budget);
    ~SstReaderCache();

    SstReaderCache(const SstReaderCache&) = delete;
    SstReaderCache& operator=(const SstReaderCache&) = delete;

    /// The resident reader for `file_number`, or null. Counts as a hit or a miss.
    std::shared_ptr<SstReader> get(uint64_t file_number);

    /// Makes `reader` resident, evicting least-recently-used entries to fit. Returns
    /// `reader` so a caller can insert and use in one expression — and returns it
    /// whether or not it stayed resident, because a reader too large for the whole
    /// cache is still a perfectly good reader.
    std::shared_ptr<SstReader> insert(uint64_t file_number, std::shared_ptr<SstReader> reader);

    /// Drops `file_number`. Called when a file becomes obsolete: keeping a reader for
    /// an unlinked object wastes the memory this class exists to bound.
    void forget(uint64_t file_number);

    void clear();

    size_t bytes() const;
    size_t count() const;
    uint64_t hits() const { return hits_.load(std::memory_order_relaxed); }
    uint64_t misses() const { return misses_.load(std::memory_order_relaxed); }

private:
    struct Entry {
        std::shared_ptr<SstReader> reader;
        size_t bytes = 0;
        std::list<uint64_t>::iterator recency;  // into `recency_`, newest at front
    };

    /// Caller holds `mutex_`. Never evicts `keep`, so inserting a reader cannot
    /// immediately throw it out again.
    void make_room_locked(size_t wanted, uint64_t keep);
    void drop_locked(uint64_t file_number);

    const size_t max_bytes_;
    MemoryBudget* budget_;

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, Entry> entries_;
    std::list<uint64_t> recency_;
    size_t bytes_ = 0;

    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_SST_READER_CACHE_HPP
