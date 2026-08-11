#ifndef ELYSIUMKV_BLOB_RANGE_CACHE_HPP
#define ELYSIUMKV_BLOB_RANGE_CACHE_HPP

#include "elysiumkv/blob_store.hpp"

#include <list>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace elysiumkv {

/// The bookkeeping shared by the cache decorators: which byte ranges are held,
/// which object they belong to, and which object to throw out next. Where the bytes
/// live is the `Payload`'s business — memory for one cache, files for the other.
///
/// **Shared rather than written twice.** The two rules below are subtle in exactly
/// the way that produces a cache which appears to work: this codebase has already
/// shipped two copies of a rule that disagreed (object-name validation) and two
/// spellings of one status (write-once). A cache that silently serves the wrong
/// bytes is worse than either.
///
/// **Keyed by the range that was asked for, evicted by whole object.** ARCHITECTURE.md "Caches chain" says
/// eviction is "LRU over whole files… per-block residency tracking is not worth the
/// bookkeeping", and also that "a partially-cached file is legitimate". Both hold
/// here: entries are the ranges a reader actually requested — an SST reader asks for
/// the same footer, filter, index and data ranges every time — and a name's entries
/// are evicted together.
///
/// **A larger cached range satisfies a smaller request.** This is what makes
/// `cache_on_write` worth anything: `put` caches the whole object, and every later
/// read is a block inside it. Without containment the write-through population
/// would never be read.
/// What to actually read when a request misses.
///
/// **The cache is the right place to read more than was asked for**, because it is the only layer
/// that keeps what it reads. A scan over a remote tier otherwise costs one round trip per block —
/// the block cache makes the *second* pass free, and does nothing for the first. Rounding a miss out
/// to a chunk turns that into one round trip per chunk, and unlike a readahead inside the iterator
/// it needs no notion of a scan: a point lookup that later reads a neighbouring key benefits too.
///
/// **Bounded amplification is the whole design.** Fetching the entire object would make a 4 KiB
/// lookup against a 64 MiB file pull the file, so the chunk is a fixed size and the read is aligned
/// to it — one chunk per miss, whatever was asked for. A request larger than the chunk is not
/// shrunk: it is rounded up to cover what was wanted.
struct FetchPlan {
    uint64_t offset;
    size_t len;
};

/// `granularity` of zero fetches exactly what was asked, which is the behaviour without chunking.
FetchPlan plan_fetch(uint64_t offset, size_t len, size_t granularity);

class RangeCacheCore {
public:
    /// Where cached bytes actually live.
    class Payload {
    public:
        /// Stores `bytes` as the entry `(name, offset)`. Returning false declines —
        /// a memory cache out of budget, a disk cache that could not write — and
        /// the core then does not record the entry, so nothing claims to hold bytes
        /// that are not there.
        virtual bool store(const std::string& name, uint64_t offset, Slice bytes) = 0;

        /// Reads `len` bytes from `skip` bytes into the entry `(name, offset)`.
        /// `nullopt` means the payload lost it — a wiped cache directory, a torn
        /// file — which is never an error: the point is that a missing cache
        /// entry costs latency and nothing else.
        virtual std::optional<Buffer> load(const std::string& name, uint64_t offset, uint64_t skip,
                                           size_t len) = 0;

        /// Drops every entry of `name`. Called on eviction and invalidation.
        virtual void drop(const std::string& name) = 0;

        virtual ~Payload() = default;
    };

    RangeCacheCore(Payload& payload, size_t max_bytes) : payload_(payload), max_bytes_(max_bytes) {}

    /// The bytes for `[offset, offset+len)` if some cached range covers them.
    /// `len` may be `BlobStore::kReadToEnd`, which only an entry known to reach the
    /// end of the object can satisfy.
    std::optional<Buffer> lookup(std::string_view name, uint64_t offset, size_t len);

    /// Records `bytes` as the range at `offset`. `to_end` says the bytes run to the
    /// end of the object, which is what a whole-object read or a `put` knows and a
    /// bounded range does not.
    void insert(std::string_view name, uint64_t offset, Slice bytes, bool to_end);

    /// Forgets everything about `name`.
    void invalidate(std::string_view name);

    size_t cached_bytes() const { return cached_bytes_; }
    size_t max_bytes() const { return max_bytes_; }

private:
    struct Range {
        uint64_t offset = 0;
        size_t size = 0;
        bool to_end = false;
    };
    struct Object {
        std::vector<Range> ranges;
        size_t bytes = 0;
        std::list<std::string>::iterator recency;  // into `recency_`, newest at front
    };

    /// Evicts least-recently-used objects until `wanted` more bytes fit. Returns
    /// false if it cannot make room even after evicting everything — a single range
    /// larger than the whole cache, which is simply not cacheable here.
    bool make_room(size_t wanted, std::string_view keep);
    void touch(const std::string& name, Object& object);
    /// **By value, not by reference.** `make_room` passes an element of `recency_`,
    /// and this erases that element before handing the name to the payload — a
    /// reference would dangle for the rest of the call. ASan caught it; nothing else
    /// did, on any platform.
    void drop_object(std::string name);

    Payload& payload_;
    const size_t max_bytes_;
    std::map<std::string, Object, std::less<>> objects_;
    std::list<std::string> recency_;
    size_t cached_bytes_ = 0;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_RANGE_CACHE_HPP
