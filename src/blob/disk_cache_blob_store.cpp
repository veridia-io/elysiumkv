#include "elysiumkv/disk_cache_blob_store.hpp"

#include "blob/range_cache.hpp"
#include "blob/single_flight.hpp"
#include "blob/verify_cache_hit.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <mutex>
#include <optional>
#include <string>
#include <memory>
#include <utility>
#include <vector>

namespace elysiumkv {
namespace {

namespace fs = std::filesystem;

/// One subdirectory per object, one file per cached range. The subdirectory is what
/// makes evicting an object a single `remove_all` rather than a scan.
///
/// The range's offset is the file name. Object names are already flat and free of
/// separators (ARCHITECTURE.md "Immutable named objects"), so no escaping is needed — and a name that is not flat never
/// reaches here, because the delegate rejects it.
fs::path range_path(const fs::path& root, const std::string& name, uint64_t offset) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(offset));
    return root / name / buf;
}

bool write_whole_file(const fs::path& path, Slice bytes) {
    // A temp file plus rename, so a torn write is never visible as a cache entry.
    // No fsync anywhere: ARCHITECTURE.md "Caches chain" — the authoritative copy is already durable, so the
    // worst a lost entry costs is one refetch.
    const fs::path temp = fs::path(path).concat(".tmp");
    const int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) return false;

    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::write(fd, bytes.data() + written, bytes.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            ::unlink(temp.c_str());
            return false;
        }
        written += static_cast<size_t>(n);
    }
    ::close(fd);

    std::error_code ec;
    fs::rename(temp, path, ec);
    if (ec) {
        fs::remove(temp, ec);
        return false;
    }
    return true;
}

/// Sharded only when there is enough capacity to divide. One mutex over the whole cache is
/// held across the chunk file's `write` and across eviction's `remove_all`, so readers of unrelated
/// objects wait on each other's filesystem calls. Splitting the capacity to fix that is a bad trade
/// below a few megabytes, where each shard would be too small to hold a working set.
constexpr size_t kMaxShards = 8;
constexpr size_t kMinBytesPerShard = 1u << 20;

size_t shard_count(size_t max_bytes) {
    const size_t affordable = max_bytes / kMinBytesPerShard;
    return affordable < kMaxShards ? (affordable == 0 ? 1 : affordable) : kMaxShards;
}

}  // namespace

struct DiskCacheBlobStore::Impl {
    std::shared_ptr<BlobStore> delegate;
    fs::path root;
    size_t max_cache_bytes;
    bool cache_on_write;
    size_t fetch_granularity = 0;

    SingleFlight in_flight;
    std::atomic<bool> verify{true};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};

    /// One independent cache, holding the names that hash to it. They share the directory: entries
    /// are keyed by object name, which belongs to exactly one shard, so no two can collide.
    struct Shard final : RangeCacheCore::Payload {
    Impl* owner;
    mutable std::mutex mutex_;
    RangeCacheCore core;

    Shard(Impl* impl, size_t bytes) : owner(impl), core(*this, bytes) {}

    bool store(const std::string& name, uint64_t offset, Slice bytes) override {
        std::error_code ec;
        fs::create_directories(owner->root / name, ec);
        if (ec) return false;
        return write_whole_file(range_path(owner->root, name, offset), bytes);
    }

    std::optional<Buffer> load(const std::string& name, uint64_t offset, uint64_t skip,
                               size_t len) override {
        const int fd = ::open(range_path(owner->root, name, offset).c_str(), O_RDONLY);
        if (fd < 0) return std::nullopt;  // wiped from under us; refetch below

        Buffer out(len);
        size_t read_total = 0;
        while (read_total < len) {
            const ssize_t n = ::pread(fd, out.data() + read_total, len - read_total,
                                      static_cast<off_t>(skip + read_total));
            if (n < 0) {
                if (errno == EINTR) continue;
                ::close(fd);
                return std::nullopt;
            }
            if (n == 0) break;  // shorter than the index believed
            read_total += static_cast<size_t>(n);
        }
        ::close(fd);
        out.resize(read_total);
        return out;
    }

    void drop(const std::string& name) override {
        std::error_code ec;
        fs::remove_all(owner->root / name, ec);
    }
    };

    std::vector<std::unique_ptr<Shard>> shards;

    Impl(std::shared_ptr<BlobStore> store, fs::path directory, size_t bytes, bool write_through)
        : delegate(std::move(store)),
          root(std::move(directory)),
          max_cache_bytes(bytes),
          cache_on_write(write_through) {
        std::error_code ec;
        fs::create_directories(root, ec);
        // Start empty. ARCHITECTURE.md "Caches chain" says wiping a cache at startup is valid, and it is
        // the honest choice here: the alternative is trusting a directory whose
        // contents this process did not write, with no index to say what is in it.
        // Adopting it would mean either a persisted index to keep consistent or a
        // full rescan, both for bytes that are refetchable.
        for (const auto& entry : fs::directory_iterator(root, ec)) {
            fs::remove_all(entry.path(), ec);
        }

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

DiskCacheBlobStore::DiskCacheBlobStore(std::shared_ptr<BlobStore> delegate, fs::path directory,
                                       size_t max_cache_bytes, bool cache_on_write,
                                       size_t fetch_granularity)
    : impl_(std::make_unique<Impl>(std::move(delegate), std::move(directory), max_cache_bytes,
                                   cache_on_write)) {
    impl_->fetch_granularity = fetch_granularity;
}

DiskCacheBlobStore::~DiskCacheBlobStore() = default;

BlobStore& DiskCacheBlobStore::delegate() { return *impl_->delegate; }
size_t DiskCacheBlobStore::max_cache_bytes() const { return impl_->max_cache_bytes; }
bool DiskCacheBlobStore::cache_on_write() const { return impl_->cache_on_write; }
uint64_t DiskCacheBlobStore::hits() const { return impl_->hits.load(); }
uint64_t DiskCacheBlobStore::misses() const { return impl_->misses.load(); }

void DiskCacheBlobStore::set_verify_against_delegate(bool verify) {
    impl_->verify.store(verify, std::memory_order_relaxed);
}

size_t DiskCacheBlobStore::cached_bytes() const {
    return impl_->cached_bytes();
}

std::future<GetResult> DiskCacheBlobStore::get(std::string_view name, uint64_t offset, size_t len) {
    return make_ready_future(get_sync(name, offset, len));
}

GetResult DiskCacheBlobStore::get_sync(std::string_view name, uint64_t offset, size_t len) {
    GetResult result = serve_get(name, offset, len);
    note_get(result);
    return result;
}

GetResult DiskCacheBlobStore::serve_get(std::string_view name, uint64_t offset, size_t len) {
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
            verify_cache_hit(*impl_->delegate, "DiskCacheBlobStore", name, offset, len, *cached);
        }
#endif
        return GetResult(std::move(*cached));
    }
    impl_->misses.fetch_add(1, std::memory_order_relaxed);

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

std::future<Status> DiskCacheBlobStore::put(std::string_view name, Slice bytes) {
    // Write-through: the authoritative store acknowledges before anything is cached
    // (ARCHITECTURE.md "Caches chain"). `cache_on_write` pays mostly for L0, whose files are read almost
    // immediately by the next L0→L1 compaction.
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

void DiskCacheBlobStore::invalidate(std::string_view name) {
    Impl::Shard& shard = impl_->shard_for(name);
    std::lock_guard<std::mutex> lock(shard.mutex_);
    shard.core.invalidate(name);
}

std::future<Status> DiskCacheBlobStore::remove(std::string_view name) {
    {
        Impl::Shard& shard = impl_->shard_for(name);
        std::lock_guard<std::mutex> lock(shard.mutex_);
        shard.core.invalidate(name);
    }
    const Status status = impl_->delegate->remove(name).get();
    note_remove(status);
    return make_ready_future(status);
}

std::future<Status> DiskCacheBlobStore::remove_many(const std::vector<std::string>& names) {
    {
        // One name at a time, each under its own shard's lock. Taking every shard would be the
        // whole cache shut for the duration, which is what sharding is for.
        for (const std::string& name : names) {
            Impl::Shard& shard = impl_->shard_for(name);
            std::lock_guard<std::mutex> lock(shard.mutex_);
            shard.core.invalidate(name);
        }
    }
    const Status status = impl_->delegate->remove_many(names).get();
    note_remove(status, names.size());
    return make_ready_future(status);
}

std::future<ListResult> DiskCacheBlobStore::list(std::string_view prefix) {
    ListResult result = impl_->delegate->list(prefix).get();
    note_list(result);
    return make_ready_future(std::move(result));
}

}  // namespace elysiumkv
