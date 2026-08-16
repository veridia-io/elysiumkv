#ifndef ELYSIUMKV_BLOB_WINDOWED_BLOB_STORE_HPP
#define ELYSIUMKV_BLOB_WINDOWED_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <algorithm>
#include <string>

namespace elysiumkv {

/// Serves block-sized reads out of one large sequential read, refilling as the cursor advances.
///
/// **For compaction inputs on a remote store.** A merge reads every block of every input exactly
/// once, in order, and `SstReader` asks for them one at a time — which against object storage is
/// one round trip per block. At the default 4 KiB block that is ~11,800 requests to read a 46 MiB
/// file, and the cost is latency rather than bandwidth: measured on a production shadow, a single
/// L0→L1 compaction took about eight minutes and moved ~190 KB/s, nowhere near the link.
///
/// The window makes that one request per `window_bytes`. It is deliberately not a whole-file
/// prefetch: inputs can total tens of megabytes per compaction and every partition compacts on its
/// own thread, so holding whole files would multiply into the shared memory budget.
///
/// Not thread-safe, and not meant to be: one instance belongs to one reader inside one compaction.
class WindowedBlobStore final : public BlobStore {
public:
    WindowedBlobStore(BlobStore& delegate, size_t window_bytes)
        : delegate_(delegate), window_bytes_(window_bytes) {}

    std::string id() const override { return delegate_.id(); }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
        // A read-to-end is already one request, and buffering it here would be the whole-file
        // prefetch this class exists to avoid.
        if (len == kReadToEnd) return delegate_.get(name, offset, len);

        if (name != name_ || offset < window_offset_ ||
            offset + len > window_offset_ + window_.size()) {
            auto filled = delegate_.get(name, offset, std::max(window_bytes_, len)).get();
            if (!filled) return make_ready_future(GetResult(std::unexpected(filled.error())));
            name_ = std::string(name);
            window_offset_ = offset;
            window_ = std::move(*filled);
        }

        const size_t start = static_cast<size_t>(offset - window_offset_);
        // A short window means the object ended, which `get` reports by truncation rather than as
        // an error; hand back what exists and let the caller judge it.
        const size_t available = window_.size() - std::min(start, window_.size());
        const size_t take = std::min(len, available);
        return make_ready_future(
            GetResult(Buffer(window_.begin() + static_cast<long>(start),
                             window_.begin() + static_cast<long>(start + take))));
    }

    std::future<Status> put(std::string_view name, Slice bytes) override {
        return delegate_.put(name, bytes);
    }

    std::future<Status> remove(std::string_view name) override { return delegate_.remove(name); }

    std::future<ListResult> list(std::string_view prefix) override {
        return delegate_.list(prefix);
    }

private:
    BlobStore& delegate_;
    size_t window_bytes_;
    std::string name_;
    uint64_t window_offset_ = 0;
    Buffer window_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_WINDOWED_BLOB_STORE_HPP
