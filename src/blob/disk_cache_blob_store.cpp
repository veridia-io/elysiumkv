#include "elysiumkv/disk_cache_blob_store.hpp"

#include "blob/range_cache.hpp"
#include "blob/verify_cache_hit.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

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

}  // namespace

struct DiskCacheBlobStore::Impl final : RangeCacheCore::Payload {
    std::shared_ptr<BlobStore> delegate;
    fs::path root;
    size_t max_cache_bytes;
    bool cache_on_write;

    mutable std::mutex mutex_;
    RangeCacheCore core;
    std::atomic<bool> verify{true};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};

    Impl(std::shared_ptr<BlobStore> store, fs::path directory, size_t bytes, bool write_through)
        : delegate(std::move(store)),
          root(std::move(directory)),
          max_cache_bytes(bytes),
          cache_on_write(write_through),
          core(*this, bytes) {
        std::error_code ec;
        fs::create_directories(root, ec);
        // **Start empty.** ARCHITECTURE.md "Caches chain" says wiping a cache at startup is valid, and it is
        // the honest choice here: the alternative is trusting a directory whose
        // contents this process did not write, with no index to say what is in it.
        // Adopting it would mean either a persisted index to keep consistent or a
        // full rescan, both for bytes that are refetchable.
        for (const auto& entry : fs::directory_iterator(root, ec)) {
            fs::remove_all(entry.path(), ec);
        }
    }

    bool store(const std::string& name, uint64_t offset, Slice bytes) override {
        std::error_code ec;
        fs::create_directories(root / name, ec);
        if (ec) return false;
        return write_whole_file(range_path(root, name, offset), bytes);
    }

    std::optional<Buffer> load(const std::string& name, uint64_t offset, uint64_t skip,
                               size_t len) override {
        const int fd = ::open(range_path(root, name, offset).c_str(), O_RDONLY);
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
        fs::remove_all(root / name, ec);
    }
};

DiskCacheBlobStore::DiskCacheBlobStore(std::shared_ptr<BlobStore> delegate, fs::path directory,
                                       size_t max_cache_bytes, bool cache_on_write)
    : impl_(std::make_unique<Impl>(std::move(delegate), std::move(directory), max_cache_bytes,
                                   cache_on_write)) {}

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
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    return impl_->core.cached_bytes();
}

std::future<GetResult> DiskCacheBlobStore::get(std::string_view name, uint64_t offset, size_t len) {
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
            verify_cache_hit(*impl_->delegate, "DiskCacheBlobStore", name, offset, len, *cached);
        }
#endif
        return make_ready_future(GetResult(std::move(*cached)));
    }
    impl_->misses.fetch_add(1, std::memory_order_relaxed);

    auto fetched = impl_->delegate->get(name, offset, len).get();
    if (!fetched) return make_ready_future(std::move(fetched));

    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        const bool to_end = len == kReadToEnd || fetched->size() < len;
        impl_->core.insert(name, offset, Slice(fetched->data(), fetched->size()), to_end);
    }
    return make_ready_future(std::move(fetched));
}

std::future<Status> DiskCacheBlobStore::put(std::string_view name, Slice bytes) {
    // Write-through: the authoritative store acknowledges before anything is cached
    // (ARCHITECTURE.md "Caches chain"). `cache_on_write` pays mostly for L0, whose files are read almost
    // immediately by the next L0→L1 compaction.
    const Status status = impl_->delegate->put(name, bytes).get();
    if (status != Status::Ok) return make_ready_future(status);

    if (impl_->cache_on_write) {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        impl_->core.insert(name, 0, bytes, /*to_end=*/true);
    }
    return make_ready_future(Status::Ok);
}

std::future<Status> DiskCacheBlobStore::remove(std::string_view name) {
    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        impl_->core.invalidate(name);
    }
    return impl_->delegate->remove(name);
}

std::future<Status> DiskCacheBlobStore::remove_many(const std::vector<std::string>& names) {
    {
        std::lock_guard<std::mutex> lock(impl_->mutex_);
        for (const std::string& name : names) impl_->core.invalidate(name);
    }
    return impl_->delegate->remove_many(names);
}

std::future<ListResult> DiskCacheBlobStore::list(std::string_view prefix) {
    return impl_->delegate->list(prefix);
}

}  // namespace elysiumkv
