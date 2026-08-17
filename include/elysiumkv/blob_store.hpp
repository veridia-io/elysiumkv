#ifndef ELYSIUMKV_BLOB_STORE_HPP
#define ELYSIUMKV_BLOB_STORE_HPP

#include "elysiumkv/slice.hpp"
#include "elysiumkv/status.hpp"

#include <future>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace elysiumkv {

using GetResult = Result<Buffer>;
using ListResult = Result<std::vector<std::string>>;

/// ARCHITECTURE.md "Immutable named objects" — immutable named objects. The only seam with a remote implementation,
/// hence asynchronous.
///
/// Contract, uniform across every implementation:
///
/// - Objects are **write-once and immutable**. `put` at a name that already
///   exists never overwrites; it reports `Status::Unusable`, because under a
///   single writer with a monotonic file-number counter a collision means a
///   zombie process is reusing numbers. A failed `put` must therefore not be
///   retried under the same name — allocate a new file number instead; the
///   partial object becomes an orphan and is collected.
/// - `Status::NotFound` is **positive evidence** that the named object is
///   absent. `Status::Io` means "could not determine" and is never evidence of
///   loss (ARCHITECTURE.md "A tier is not a level"). A store never reports whole-store absence.
/// - `list` returning an empty vector is a successful, meaningful empty result.
/// - Names are flat: non-empty, no '/', no leading '.'. A malformed name is a
///   programming error and reports `Status::Config`.
/// - Bytes passed to `put` must stay valid until the returned future is ready.
class CacheBlobStore;

/// Completes a future immediately. Declared before `BlobStore` because the
/// defaulted `remove_many` below uses it: a store whose work is already done has
/// no reason to involve a thread.
template <typename T>
std::future<T> make_ready_future(T value) {
    std::promise<T> promise;
    promise.set_value(std::move(value));
    return promise.get_future();
}

class BlobStore {
public:
    /// Read `len` bytes from `offset`. A read overlapping the end of the object
    /// is truncated to what exists; `offset` at or past the end yields an empty
    /// buffer, not an error.
    static constexpr size_t kReadToEnd = std::numeric_limits<size_t>::max();

    /// Stable identifier, unchanged across restarts. Recorded per file in
    /// FileMetadata so a file's location survives a change to the level→store
    /// map. A cache layer returns its delegate's id — caches are not locations.
    virtual std::string id() const = 0;

    virtual std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) = 0;
    virtual std::future<Status> put(std::string_view name, Slice bytes) = 0;
    virtual std::future<Status> remove(std::string_view name) = 0;

    /// Removes several objects. The default loops, so an implementation only
    /// overrides it when batching means something.
    ///
    /// It exists for the remote case: S3 deletes up to 1000 keys in one
    /// `DeleteObjects`, and obsolete-object collection after a compaction can
    /// have dozens to remove. Per-object DELETE turns that into dozens of round
    /// trips, each with its own latency and its own chance to fail.
    ///
    /// Failure is reported for the batch, not per name. A partial failure leaves
    /// some objects present, which is safe: they are unreferenced, and the next
    /// collection pass finds them again (ARCHITECTURE.md "Versions are immutable snapshots").
    /// **Best effort: every name is attempted, and the first failure is what is
    /// reported.** Stopping at the first failure would make one unremovable object
    /// shield every name behind it, and `remove` is idempotent — so there is
    /// nothing to gain by giving up early and a stuck batch to lose.
    virtual std::future<Status> remove_many(const std::vector<std::string>& names) {
        Status first_failure = Status::Ok;
        for (const std::string& name : names) {
            const Status status = remove(name).get();
            if (status != Status::Ok && first_failure == Status::Ok) first_failure = status;
        }
        return make_ready_future(first_failure);
    }

    virtual std::future<ListResult> list(std::string_view prefix) = 0;

    /// A view suitable for large sequential reads that will not be reread —
    /// compaction inputs, bulk scans. Caches override this to return their
    /// delegate's view, so bulk reads bypass the cache chain by composition
    /// rather than by a per-call flag.
    virtual BlobStore& bulk_view() { return *this; }

    /// Non-null exactly for the cache decorators. A virtual rather than a
    /// dynamic_cast because the engine builds with -fno-rtti.
    virtual CacheBlobStore* as_cache() { return nullptr; }

    virtual ~BlobStore() = default;
};

/// Marker for the cache decorators. Declared here so the open-time check
/// "the innermost store of every chain is authoritative" (ARCHITECTURE.md "A tier is not a level") can be written
/// without the cache layers existing yet.
class CacheBlobStore : public BlobStore {
public:
    virtual BlobStore& delegate() = 0;
    virtual size_t max_cache_bytes() const = 0;
    virtual bool cache_on_write() const = 0;
    /// A miss against a remote delegate is a round trip, so the ratio is read latency.
    virtual uint64_t hits() const = 0;
    virtual uint64_t misses() const = 0;

    std::string id() const override {
        return const_cast<CacheBlobStore*>(this)->delegate().id();
    }
    BlobStore& bulk_view() override { return delegate().bulk_view(); }
    CacheBlobStore* as_cache() override { return this; }
};

/// The innermost element of a chain — the store that actually holds the bytes.
inline BlobStore& authoritative_store(BlobStore& store) {
    BlobStore* s = &store;
    while (CacheBlobStore* cache = s->as_cache()) {
        s = &cache->delegate();
    }
    return *s;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_STORE_HPP
