#ifndef ELYSIUMKV_BLOB_WINDOWED_BLOB_STORE_HPP
#define ELYSIUMKV_BLOB_WINDOWED_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"
#include "util/budget_charge.hpp"

#include <algorithm>
#include <future>
#include <memory>
#include <optional>
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
    /// `object_bytes` of zero means unknown, and only costs one speculative read per object: the
    /// prefetch after the last full window finds nothing and is thrown away. Compaction knows the
    /// size from the manifest, so it passes it and pays nothing.
    ///
    /// **Charged for two windows, not one**, and for the instance's whole life rather than as the
    /// buffers come and go: the one being merged and the one being fetched ahead of it are both
    /// live, a compaction holds one of these per input, and the point of the charge is to bound the
    /// worst case rather than track the current one.
    WindowedBlobStore(BlobStore& delegate, size_t window_bytes, uint64_t object_bytes = 0,
                      const std::shared_ptr<MemoryBudget>& budget = nullptr)
        : delegate_(delegate),
          window_bytes_(window_bytes),
          object_bytes_(object_bytes),
          charge_(budget, 2 * window_bytes) {}

    std::string id() const override { return delegate_.id(); }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
        // A read-to-end is already one request, and buffering it here would be the whole-file
        // prefetch this class exists to avoid.
        if (len == kReadToEnd) return delegate_.get(name, offset, len);

        if (name != name_ || offset < window_offset_ ||
            offset + len > window_offset_ + window_.size()) {
            if (!refill(name, offset, len)) {
                return make_ready_future(GetResult(std::unexpected(last_error_)));
            }
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
    /// Moves the window so it covers `[offset, offset + len)`. False leaves `last_error_` set.
    ///
    /// **The read that misses starts *inside* the window it is leaving**, not at its end: blocks do
    /// not divide evenly into windows, so the one that overruns began before the boundary. A
    /// prefetch addressed at the boundary can still serve it — joined to the tail of the window it
    /// replaces, which is bytes already in hand. Predicting the miss offset instead is not
    /// possible, and requiring it to match is why the first version of this never once hit.
    bool refill(std::string_view name, uint64_t offset, size_t len) {
        const uint64_t want = std::max(static_cast<uint64_t>(window_bytes_), static_cast<uint64_t>(len));
        const uint64_t current_end = window_offset_ + window_.size();
        // Walking forward through the same object, which is the only pattern worth prefetching for.
        const bool forward =
            name == name_ && offset >= window_offset_ && offset <= current_end;

        Buffer filled;
        bool have = false;
        if (auto fetched = take_prefetch(name, current_end); fetched.has_value() && forward) {
            const size_t keep = static_cast<size_t>(current_end - offset);
            if (keep + fetched->size() >= len) {
                filled.reserve(keep + fetched->size());
                filled.insert(filled.end(), window_.end() - static_cast<long>(keep), window_.end());
                filled.insert(filled.end(), fetched->begin(), fetched->end());
                have = true;
            }
        }
        if (!have) {
            auto direct = delegate_.get(name, offset, static_cast<size_t>(want)).get();
            if (!direct) {
                last_error_ = direct.error();
                return false;
            }
            filled = std::move(*direct);
        }

        sequential_ = forward;
        name_ = std::string(name);
        window_offset_ = offset;
        window_ = std::move(filled);
        start_prefetch();
        return true;
    }

    /// Fetches the window after this one while the caller is still merging the current one.
    ///
    /// **The seam is not enough on its own.** Every `BlobStore` here completes its future
    /// synchronously, so a refill is a round trip the merge waits out with nothing else in flight —
    /// and a compaction refills once per window per input. The window removed the request *count*;
    /// this is what removes the latency, and it needs a thread because nothing below is
    /// asynchronous.
    ///
    /// **Only once the reader is walking the file forward.** Opening an SST reads its footer and
    /// index near the end and then starts at the beginning, so prefetching from the first fill
    /// would fetch a window nobody wants. A short window means the object ended, and there is
    /// nothing after it to want.
    void start_prefetch() {
        if (!sequential_ || window_.size() < window_bytes_) return;
        prefetch_offset_ = window_offset_ + window_.size();
        if (object_bytes_ != 0 && prefetch_offset_ >= object_bytes_) return;
        prefetch_ = std::async(std::launch::async,
                               [this, name = name_, offset = prefetch_offset_] {
                                   return delegate_.get(name, offset, window_bytes_).get();
                               });
    }

    /// The prefetched bytes if they are the ones wanted, else nothing — and either way the future
    /// is consumed, because `std::async` blocks in its destructor and a stale one would be waited
    /// for at the worst possible moment.
    std::optional<Buffer> take_prefetch(std::string_view name, uint64_t at) {
        if (!prefetch_.valid()) return std::nullopt;
        GetResult fetched = prefetch_.get();
        if (name != name_ || at != prefetch_offset_ || !fetched) return std::nullopt;
        return std::move(*fetched);
    }

    BlobStore& delegate_;
    size_t window_bytes_;
    uint64_t object_bytes_ = 0;
    std::string name_;
    uint64_t window_offset_ = 0;
    Buffer window_;

    /// Declared last so it is released only after the buffers it accounts for are gone.
    bool sequential_ = false;
    std::future<GetResult> prefetch_;
    uint64_t prefetch_offset_ = 0;
    Status last_error_ = Status::Io;
    BudgetCharge charge_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_WINDOWED_BLOB_STORE_HPP
