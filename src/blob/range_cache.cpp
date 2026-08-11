#include "blob/range_cache.hpp"

#include <algorithm>
#include <utility>

namespace elysiumkv {

FetchPlan plan_fetch(uint64_t offset, size_t len, size_t granularity) {
    if (granularity == 0) return {offset, len};

    const uint64_t aligned = offset - (offset % granularity);
    if (len == BlobStore::kReadToEnd) {
        // Nothing to round up to: the read already runs to the end of the object. Aligning the
        // start still helps, since the entry then begins on a boundary a later miss can share.
        return {aligned, BlobStore::kReadToEnd};
    }

    // Round the far edge up to a boundary, and never return less than was asked for — a plan that
    // shrank a request would turn a complete answer into a partial one, which the cache treats as
    // wrong rather than as merely short.
    const uint64_t end = offset + len;
    uint64_t rounded_end = end % granularity == 0 ? end : end + (granularity - end % granularity);
    if (rounded_end < end) rounded_end = end;  // overflow: fall back to exactly what was wanted
    return {aligned, static_cast<size_t>(rounded_end - aligned)};
}

std::optional<Buffer> RangeCacheCore::lookup(std::string_view name, uint64_t offset, size_t len) {
    auto found = objects_.find(name);
    if (found == objects_.end()) return std::nullopt;

    for (const Range& range : found->second.ranges) {
        if (range.offset > offset) continue;
        const uint64_t skip = offset - range.offset;
        if (skip > range.size && !range.to_end) continue;

        size_t wanted = len;
        if (len == BlobStore::kReadToEnd) {
            // Only an entry that reaches the end of the object can answer "to the
            // end"; anything else would silently truncate the read.
            if (!range.to_end) continue;
            wanted = range.size - std::min<size_t>(skip, range.size);
        } else if (skip + len > range.size) {
            // A bounded read past this entry is a partial answer, and a partial
            // answer is a wrong one. Only an entry running to the end of the object
            // may return fewer bytes than asked — because then there are no more.
            if (!range.to_end) continue;
            wanted = range.size - std::min<size_t>(skip, range.size);
        }

        auto bytes = payload_.load(std::string(name), range.offset, skip, wanted);
        if (!bytes) {
            // The payload lost it. Forget the whole object rather than keep an index
            // that lies, and let the caller fall through to the store below.
            drop_object(std::string(name));
            return std::nullopt;
        }
        touch(found->first, found->second);
        return bytes;
    }
    return std::nullopt;
}

void RangeCacheCore::insert(std::string_view name, uint64_t offset, Slice bytes, bool to_end) {
    if (bytes.size() > max_bytes_) return;  // never worth evicting everything for
    if (!make_room(bytes.size(), name)) return;

    const std::string key(name);
    if (!payload_.store(key, offset, bytes)) return;  // declined; record nothing

    auto [it, inserted] = objects_.try_emplace(key);
    if (inserted) {
        recency_.push_front(key);
        it->second.recency = recency_.begin();
    }

    // Replacing an identical range rather than accumulating duplicates: a reader
    // that missed and repopulated should not be charged twice for the same bytes.
    auto same = std::find_if(it->second.ranges.begin(), it->second.ranges.end(),
                             [&](const Range& r) { return r.offset == offset; });
    if (same != it->second.ranges.end()) {
        cached_bytes_ -= same->size;
        it->second.bytes -= same->size;
        *same = Range{offset, bytes.size(), to_end};
    } else {
        it->second.ranges.push_back(Range{offset, bytes.size(), to_end});
    }
    it->second.bytes += bytes.size();
    cached_bytes_ += bytes.size();
    touch(it->first, it->second);
}

void RangeCacheCore::invalidate(std::string_view name) {
    if (objects_.find(name) == objects_.end()) {
        // Still tell the payload: a previous run may have left files behind that
        // this index knows nothing about.
        payload_.drop(std::string(name));
        return;
    }
    drop_object(std::string(name));
}

bool RangeCacheCore::make_room(size_t wanted, std::string_view keep) {
    while (cached_bytes_ + wanted > max_bytes_) {
        if (recency_.empty()) return false;
        // Evict from the back — least recently used. Never the object being
        // inserted into: a reader populating two ranges of one file would otherwise
        // evict its own first range to make room for its second.
        auto victim = std::find_if(recency_.rbegin(), recency_.rend(),
                                   [&](const std::string& n) { return n != keep; });
        if (victim == recency_.rend()) return false;
        drop_object(*victim);
    }
    return true;
}

void RangeCacheCore::touch(const std::string& name, Object& object) {
    recency_.splice(recency_.begin(), recency_, object.recency);
    object.recency = recency_.begin();
    (void)name;
}

void RangeCacheCore::drop_object(std::string name) {
    auto found = objects_.find(name);
    if (found == objects_.end()) return;
    cached_bytes_ -= found->second.bytes;
    recency_.erase(found->second.recency);
    objects_.erase(found);
    payload_.drop(name);
}

}  // namespace elysiumkv
