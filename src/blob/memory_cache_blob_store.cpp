#include "elysiumkv/memory_cache_blob_store.hpp"

#include "elysiumkv/memory_budget.hpp"
#include "blob/range_cache.hpp"
#include "blob/verify_cache_hit.hpp"

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <utility>

namespace elysiumkv {

struct MemoryCacheBlobStore::Impl final : RangeCacheCore::Payload {
    std::shared_ptr<BlobStore> delegate;
    std::shared_ptr<MemoryBudget> budget;
    size_t max_cache_bytes;
    bool cache_on_write;

    mutable std::mutex mutex_;
    RangeCacheCore core;
    /// `(name, offset)` -> bytes. Held beside the core rather than inside it so the
    /// core stays about *which* ranges exist, not where they live.
    std::map<std::pair<std::string, uint64_t>, Buffer> entries;
    std::atomic<bool> verify{true};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};

    Impl(std::shared_ptr<BlobStore> store, std::shared_ptr<MemoryBudget> memory_budget,
         size_t bytes, bool write_through)
        : delegate(std::move(store)),
          budget(std::move(memory_budget)),
          max_cache_bytes(bytes),
          cache_on_write(write_through),
          core(*this, bytes) {}

    ~Impl() override {
        // Whatever is still held has to come off the shared budget, or a
        // long-running process that opens and closes instances leaks the whole
        // budget one cache at a time.
        if (budget != nullptr && held_ > 0) budget->release(held_);
    }

    bool store(const std::string& name, uint64_t offset, Slice bytes) override {
        // **The budget decides, and a refusal is not a failure.** ARCHITECTURE.md "A process-wide memory budget" makes the
        // budget process-wide, so this cache competing with a dozen others is the
        // normal case; the correct response to a full budget is to serve the read
        // and cache nothing.
        if (budget != nullptr && !budget->try_acquire(bytes.size())) return false;

        // **Give back what this key already held.** A read that missed with a small
        // range and later missed with a larger one at the same offset repopulates the
        // same key, and charging both would drift the shared budget upward with no
        // bytes behind it. The core tracks its own accounting; this side has to track
        // the payload's.
        auto existing = entries.find({name, offset});
        if (existing != entries.end()) {
            held_ -= existing->second.size();
            if (budget != nullptr) budget->release(existing->second.size());
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
            if (budget != nullptr) budget->release(bytes);
            it = entries.erase(it);
        }
    }

private:
    size_t held_ = 0;
};

MemoryCacheBlobStore::MemoryCacheBlobStore(std::shared_ptr<BlobStore> delegate,
                                           std::shared_ptr<MemoryBudget> budget,
                                           size_t max_cache_bytes, bool cache_on_write)
    : impl_(std::make_unique<Impl>(std::move(delegate), std::move(budget), max_cache_bytes,
                                   cache_on_write)) {}

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
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    return impl_->core.cached_bytes();
}

std::future<GetResult> MemoryCacheBlobStore::get(std::string_view name, uint64_t offset,
                                                 size_t len) {
    std::optional<Buffer> cached;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        cached = impl_->core.lookup(name, offset, len);
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
        return make_ready_future(GetResult(std::move(*cached)));
    }
    impl_->misses.fetch_add(1, std::memory_order_relaxed);

    // Fetched without the lock held: a miss against a remote delegate takes tens of
    // milliseconds, and holding the cache shut for that long would serialise every
    // other reader behind one network round trip.
    auto fetched = impl_->delegate->get(name, offset, len).get();
    if (!fetched) return make_ready_future(std::move(fetched));

    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        // A read the delegate truncated ran to the end of the object, so it is safe
        // to answer a later "read to the end" from it. A full-length answer to a
        // bounded read proves nothing about where the object ends.
        const bool to_end = len == kReadToEnd || fetched->size() < len;
        impl_->core.insert(name, offset, Slice(fetched->data(), fetched->size()), to_end);
    }
    return make_ready_future(std::move(fetched));
}

std::future<Status> MemoryCacheBlobStore::put(std::string_view name, Slice bytes) {
    // **Write-through, never write-back (ARCHITECTURE.md "Caches chain").** The authoritative store
    // acknowledges first; only then is anything cached. Acknowledging from a cache
    // and writing down later would make the cache authoritative for a window, which
    // is the entire class of problem this design exists to avoid.
    const Status status = impl_->delegate->put(name, bytes).get();
    if (status != Status::Ok) return make_ready_future(status);

    if (impl_->cache_on_write) {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        impl_->core.insert(name, 0, bytes, /*to_end=*/true);
    }
    return make_ready_future(Status::Ok);
}

std::future<Status> MemoryCacheBlobStore::remove(std::string_view name) {
    // Invalidate first: if the delegate's delete fails the object still exists and a
    // later read repopulates, which costs a round trip. The other order can leave an
    // entry serving bytes for an object that is gone.
    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        impl_->core.invalidate(name);
    }
    return impl_->delegate->remove(name);
}

std::future<Status> MemoryCacheBlobStore::remove_many(const std::vector<std::string>& names) {
    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        for (const std::string& name : names) impl_->core.invalidate(name);
    }
    // Forwarded rather than looped: a cache layer must not undo the delegate's
    // batching, or a chain over S3 is back to one DELETE per object.
    return impl_->delegate->remove_many(names);
}

std::future<ListResult> MemoryCacheBlobStore::list(std::string_view prefix) {
    // Pure delegation. A cache is not a location (ARCHITECTURE.md "Immutable named objects"), and reporting cached ranges
    // as objects would make a half-populated cache look like a store that had lost
    // everything else — which is the one signal the discard path acts on.
    return impl_->delegate->list(prefix);
}

}  // namespace elysiumkv
