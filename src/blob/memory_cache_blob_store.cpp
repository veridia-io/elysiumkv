#include "elysiumkv/memory_cache_blob_store.hpp"

#include "elysiumkv/memory_budget.hpp"
#include "blob/range_cache.hpp"
#include "blob/single_flight.hpp"
#include "blob/verify_cache_hit.hpp"

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <utility>

namespace elysiumkv {
namespace {

/// Sharded only when there is enough capacity to divide. One mutex over the whole cache makes
/// readers of unrelated objects wait on each other — measured at 1.8x throughput across eight
/// threads, where the work is independent. Splitting the capacity to fix that is a bad trade below
/// a few megabytes, where each shard would be too small to hold a working set and would evict what
/// an unsharded cache of the same size keeps.
constexpr size_t kMaxShards = 8;
constexpr size_t kMinBytesPerShard = 1u << 20;

size_t shard_count(size_t max_bytes) {
    const size_t affordable = max_bytes / kMinBytesPerShard;
    return affordable < kMaxShards ? (affordable == 0 ? 1 : affordable) : kMaxShards;
}

}  // namespace

struct MemoryCacheBlobStore::Impl {
    std::shared_ptr<BlobStore> delegate;
    std::shared_ptr<MemoryBudget> budget;
    size_t max_cache_bytes;
    bool cache_on_write;
    size_t fetch_granularity = 0;

    SingleFlight in_flight;
    std::atomic<bool> verify{true};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};

    /// One independent cache, holding the names that hash to it.
    struct Shard final : RangeCacheCore::Payload {
    Impl* owner;
    mutable std::mutex mutex_;
    RangeCacheCore core;
    /// `(name, offset)` -> bytes. Held beside the core rather than inside it so the
    /// core stays about *which* ranges exist, not where they live.
    std::map<std::pair<std::string, uint64_t>, Buffer> entries;

    Shard(Impl* impl, size_t bytes) : owner(impl), core(*this, bytes) {}

    ~Shard() override {
        // Whatever is still held has to come off the shared budget, or a
        // long-running process that opens and closes instances leaks the whole
        // budget one cache at a time.
        if (owner->budget != nullptr && held_ > 0) owner->budget->release(held_);
    }

    bool store(const std::string& name, uint64_t offset, Slice bytes) override {
        // The budget decides, and a refusal is not a failure. ARCHITECTURE.md "A process-wide memory budget" makes the
        // budget process-wide, so this cache competing with a dozen others is the
        // normal case; the correct response to a full budget is to serve the read
        // and cache nothing.
        if (owner->budget != nullptr && !owner->budget->try_acquire(bytes.size())) return false;

        // Give back what this key already held. A read that missed with a small
        // range and later missed with a larger one at the same offset repopulates the
        // same key, and charging both would drift the shared budget upward with no
        // bytes behind it. The core tracks its own accounting; this side has to track
        // the payload's.
        auto existing = entries.find({name, offset});
        if (existing != entries.end()) {
            held_ -= existing->second.size();
            if (owner->budget != nullptr) owner->budget->release(existing->second.size());
        }

        entries[{name, offset}] = Buffer(bytes.data(), bytes.data() + bytes.size());
        held_ += bytes.size();
        return true;
    }

    std::optional<Buffer> load(const std::string& name, uint64_t offset, uint64_t skip,
                               size_t len) override {
        auto found = entries.find({name, offset});
        if (found == entries.end()) return std::nullopt;
        const Buffer& bytes = found->second;
        if (skip > bytes.size()) return Buffer{};
        const size_t available = bytes.size() - static_cast<size_t>(skip);
        const size_t take = len < available ? len : available;
        const auto begin = bytes.begin() + static_cast<std::ptrdiff_t>(skip);
        return Buffer(begin, begin + static_cast<std::ptrdiff_t>(take));
    }

    void drop(const std::string& name) override {
        // Every offset under this name. `map` ordering puts them together, so this
        // is a range erase rather than a scan of the whole cache.
        auto it = entries.lower_bound({name, 0});
        while (it != entries.end() && it->first.first == name) {
            const size_t bytes = it->second.size();
            held_ -= bytes;
            if (owner->budget != nullptr) owner->budget->release(bytes);
            it = entries.erase(it);
        }
    }

    private:
        size_t held_ = 0;
    };

    std::vector<std::unique_ptr<Shard>> shards;

    Impl(std::shared_ptr<BlobStore> store, std::shared_ptr<MemoryBudget> memory_budget,
         size_t bytes, bool write_through)
        : delegate(std::move(store)),
          budget(std::move(memory_budget)),
          max_cache_bytes(bytes),
          cache_on_write(write_through) {
        const size_t count = shard_count(bytes);
        shards.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            shards.push_back(std::make_unique<Shard>(this, bytes / count));
        }
    }

    Shard& shard_for(std::string_view name) {
        return *shards[std::hash<std::string_view>{}(name) % shards.size()];
    }

    size_t cached_bytes() const {
        size_t total = 0;
        for (const auto& shard : shards) {
            std::lock_guard<std::mutex> lock(shard->mutex_);
            total += shard->core.cached_bytes();
        }
        return total;
    }
};

MemoryCacheBlobStore::MemoryCacheBlobStore(std::shared_ptr<BlobStore> delegate,
                                           std::shared_ptr<MemoryBudget> budget,
                                           size_t max_cache_bytes, bool cache_on_write,
                                           size_t fetch_granularity)
    : impl_(std::make_unique<Impl>(std::move(delegate), std::move(budget), max_cache_bytes,
                                   cache_on_write)) {
    impl_->fetch_granularity = fetch_granularity;
}

MemoryCacheBlobStore::~MemoryCacheBlobStore() = default;

BlobStore& MemoryCacheBlobStore::delegate() { return *impl_->delegate; }
size_t MemoryCacheBlobStore::max_cache_bytes() const { return impl_->max_cache_bytes; }
bool MemoryCacheBlobStore::cache_on_write() const { return impl_->cache_on_write; }
uint64_t MemoryCacheBlobStore::hits() const { return impl_->hits.load(); }
uint64_t MemoryCacheBlobStore::misses() const { return impl_->misses.load(); }

void MemoryCacheBlobStore::set_verify_against_delegate(bool verify) {
    impl_->verify.store(verify, std::memory_order_relaxed);
}

size_t MemoryCacheBlobStore::cached_bytes() const {
    return impl_->cached_bytes();
}

std::future<GetResult> MemoryCacheBlobStore::get(std::string_view name, uint64_t offset, size_t len) {
    return make_ready_future(get_sync(name, offset, len));
}

GetResult MemoryCacheBlobStore::get_sync(std::string_view name, uint64_t offset, size_t len) {
    GetResult result = serve_get(name, offset, len);
    note_get(result);
    return result;
}

GetResult MemoryCacheBlobStore::serve_get(std::string_view name, uint64_t offset, size_t len) {
    std::optional<Buffer> cached;
    {
        Impl::Shard& shard = impl_->shard_for(name);
        std::lock_guard<std::mutex> lock(shard.mutex_);
        cached = shard.core.lookup(name, offset, len);
    }
    if (cached.has_value()) {
        impl_->hits.fetch_add(1, std::memory_order_relaxed);
        // Verified with the lock released, for the same reason the fetch below is: the
        // check reads from the delegate, and holding a cache shut across a read from the
        // layer under it is the shape this class exists to avoid.
#ifdef ELYSIUMKV_PARANOID
        if (impl_->verify.load(std::memory_order_relaxed)) {
            verify_cache_hit(*impl_->delegate, "MemoryCacheBlobStore", name, offset, len, *cached);
        }
#endif
        return GetResult(std::move(*cached));
    }
    impl_->misses.fetch_add(1, std::memory_order_relaxed);

    // Fetched without the lock held: a miss against a remote delegate takes tens of
    // milliseconds, and holding the cache shut for that long would serialise every
    // other reader behind one network round trip.
    // Rounded out to a chunk, so the next block of a scan is already held. The plan is a superset
    // of the request, never a subset, so what comes back can always answer it.
    const FetchPlan plan = plan_fetch(offset, len, impl_->fetch_granularity);

    // One fetch per chunk, not one per reader. Threads arriving together on a cold chunk
    // otherwise each pay the round trip and each write the result; the first one here does the
    // work and the rest wait for it. Sharing the fetched buffer is safe because they share the
    // plan: each still cuts its own window out of it below.
    auto fetched = impl_->in_flight.run(name, plan.offset, plan.len, [&] {
        auto result = impl_->delegate->get_sync(name, plan.offset, plan.len);
        if (!result) return result;

        {
            Impl::Shard& shard = impl_->shard_for(name);
            std::lock_guard<std::mutex> lock(shard.mutex_);
            // A read the delegate truncated ran to the end of the object, so it is safe
            // to answer a later "read to the end" from it. A full-length answer to a
            // bounded read proves nothing about where the object ends.
            const bool to_end = plan.len == kReadToEnd || result->size() < plan.len;
            shard.core.insert(name, plan.offset, Slice(result->data(), result->size()), to_end);
        }
        return result;
    });
    if (!fetched) return fetched;

    // The caller asked for a window inside the chunk. Truncating at what actually arrived keeps the
    // contract for a read overlapping the end of the object: short is an answer, not an error.
    const size_t skip = static_cast<size_t>(offset - plan.offset);
    if (skip >= fetched->size()) return GetResult(Buffer{});
    size_t available = fetched->size() - skip;
    if (len != kReadToEnd) available = std::min(available, len);
    return 
        GetResult(Buffer(fetched->begin() + static_cast<std::ptrdiff_t>(skip),
                         fetched->begin() + static_cast<std::ptrdiff_t>(skip + available)));
}

std::future<Status> MemoryCacheBlobStore::put(std::string_view name, Slice bytes) {
    // Write-through, never write-back (ARCHITECTURE.md "Caches chain"). The authoritative store
    // acknowledges first; only then is anything cached. Acknowledging from a cache
    // and writing down later would make the cache authoritative for a window, which
    // is the entire class of problem this design exists to avoid.
    const Status status = impl_->delegate->put(name, bytes).get();
    note_put(status, bytes.size());
    if (status != Status::Ok) return make_ready_future(status);

    if (impl_->cache_on_write) {
        Impl::Shard& shard = impl_->shard_for(name);
        std::lock_guard<std::mutex> lock(shard.mutex_);
        shard.core.insert(name, 0, bytes, /*to_end=*/true);
    }
    return make_ready_future(Status::Ok);
}

void MemoryCacheBlobStore::invalidate(std::string_view name) {
    Impl::Shard& shard = impl_->shard_for(name);
    std::lock_guard<std::mutex> lock(shard.mutex_);
    shard.core.invalidate(name);
}

std::future<Status> MemoryCacheBlobStore::remove(std::string_view name) {
    // Invalidate first: if the delegate's delete fails the object still exists and a
    // later read repopulates, which costs a round trip. The other order can leave an
    // entry serving bytes for an object that is gone.
    {
        Impl::Shard& shard = impl_->shard_for(name);
        std::lock_guard<std::mutex> lock(shard.mutex_);
        shard.core.invalidate(name);
    }
    const Status status = impl_->delegate->remove(name).get();
    note_remove(status);
    return make_ready_future(status);
}

std::future<Status> MemoryCacheBlobStore::remove_many(const std::vector<std::string>& names) {
    {
        // One name at a time, each under its own shard's lock. Taking every shard would be the
        // whole cache shut for the duration, which is what sharding is for.
        for (const std::string& name : names) {
            Impl::Shard& shard = impl_->shard_for(name);
            std::lock_guard<std::mutex> lock(shard.mutex_);
            shard.core.invalidate(name);
        }
    }
    // Forwarded rather than looped: a cache layer must not undo the delegate's
    // batching, or a chain over S3 is back to one DELETE per object.
    const Status status = impl_->delegate->remove_many(names).get();
    note_remove(status, names.size());
    return make_ready_future(status);
}

std::future<ListResult> MemoryCacheBlobStore::list(std::string_view prefix) {
    // Pure delegation. A cache is not a location (ARCHITECTURE.md "Immutable named objects"), and reporting cached ranges
    // as objects would make a half-populated cache look like a store that had lost
    // everything else — which is the one signal the discard path acts on.
    ListResult result = impl_->delegate->list(prefix).get();
    note_list(result);
    return make_ready_future(std::move(result));
}

}  // namespace elysiumkv
