#ifndef ELYSIUMKV_BLOB_OPEN_FILE_CACHE_HPP
#define ELYSIUMKV_BLOB_OPEN_FILE_CACHE_HPP

#include <array>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <unistd.h>

namespace elysiumkv {

/// A descriptor and the size it was opened at.
///
/// **The size is cached because objects are immutable.** Nothing may rewrite or extend a named
/// object, so one `fstat` at open time answers every later read — and dropping it is what turns a
/// block read from three syscalls into one `pread`.
struct OpenFile {
    int fd = -1;
    uint64_t size = 0;

    OpenFile(int descriptor, uint64_t bytes) : fd(descriptor), size(bytes) {}
    OpenFile(const OpenFile&) = delete;
    OpenFile& operator=(const OpenFile&) = delete;
    ~OpenFile() {
        if (fd >= 0) ::close(fd);
    }
};

/// Open descriptors keyed by object name, sharded and LRU-bounded.
///
/// **Handed out as `shared_ptr`, so a reader's `pread` outlives eviction.** Eviction drops the
/// cache's reference and the last holder closes, which is the same ownership rule the block cache
/// uses and the reason eviction never consults a use count.
///
/// **Bounded by descriptor count, and the bound must stay modest.** A process running dozens of
/// instances over dozens of stores multiplies this by every one of them, against a soft limit as
/// low as 256.
class OpenFileCache {
public:
    static constexpr size_t kNumShards = 8;

    explicit OpenFileCache(size_t capacity) : capacity_(capacity) {}

    /// The cached descriptor, or null if this name is not held.
    std::shared_ptr<const OpenFile> lookup(std::string_view name) {
        if (capacity_ == 0) return nullptr;
        Shard& shard = shard_for(name);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.index.find(name);
        if (it == shard.index.end()) return nullptr;
        shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
        return it->second->file;
    }

    /// Records `file` under `name`, evicting the least recently used if that puts the shard over
    /// its share. Returns `file` so a caller can insert and use in one expression.
    std::shared_ptr<const OpenFile> insert(std::string_view name,
                                           std::shared_ptr<const OpenFile> file) {
        if (capacity_ == 0) return file;
        Shard& shard = shard_for(name);
        // Evicted entries are closed after the lock is dropped: `~OpenFile` calls `close`, and a
        // syscall under a shard mutex is the shape this cache exists to remove.
        std::vector<std::shared_ptr<const OpenFile>> evicted;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto it = shard.index.find(name);
            if (it != shard.index.end()) {
                it->second->file = file;
                shard.lru.splice(shard.lru.begin(), shard.lru, it->second);
                return file;
            }
            shard.lru.push_front(Entry{std::string(name), file});
            shard.index.emplace(shard.lru.front().name, shard.lru.begin());
            while (shard.lru.size() > per_shard_capacity()) {
                evicted.push_back(std::move(shard.lru.back().file));
                shard.index.erase(shard.lru.back().name);
                shard.lru.pop_back();
            }
        }
        return file;
    }

    void erase(std::string_view name) {
        if (capacity_ == 0) return;
        Shard& shard = shard_for(name);
        std::shared_ptr<const OpenFile> evicted;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto it = shard.index.find(name);
            if (it == shard.index.end()) return;
            evicted = std::move(it->second->file);
            shard.lru.erase(it->second);
            shard.index.erase(it);
        }
    }

    /// Drops everything. Used when the process runs out of descriptors, where holding any is worse
    /// than holding none.
    void clear() {
        std::vector<std::shared_ptr<const OpenFile>> evicted;
        for (Shard& shard : shards_) {
            std::lock_guard<std::mutex> lock(shard.mutex);
            for (Entry& entry : shard.lru) evicted.push_back(std::move(entry.file));
            shard.lru.clear();
            shard.index.clear();
        }
    }

    size_t size() const {
        size_t total = 0;
        for (const Shard& shard : shards_) {
            std::lock_guard<std::mutex> lock(shard.mutex);
            total += shard.lru.size();
        }
        return total;
    }

private:
    struct Entry {
        std::string name;
        std::shared_ptr<const OpenFile> file;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::list<Entry> lru;  // front = most recently used
        /// Keyed by a view into the entry's own `name`, which `std::list` keeps stable.
        std::unordered_map<std::string_view, std::list<Entry>::iterator> index;
    };

    size_t per_shard_capacity() const {
        return capacity_ / kNumShards + (capacity_ % kNumShards != 0 ? 1 : 0);
    }

    Shard& shard_for(std::string_view name) {
        // Object names are zero-padded decimal, so they differ only in their last few bytes —
        // hashing the whole name is what keeps them off one shard.
        return shards_[std::hash<std::string_view>{}(name) % kNumShards];
    }

    std::array<Shard, kNumShards> shards_;
    size_t capacity_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_OPEN_FILE_CACHE_HPP
