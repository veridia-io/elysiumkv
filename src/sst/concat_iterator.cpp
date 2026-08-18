#include "sst/concat_iterator.hpp"

#include "sst/sst_reader.hpp"

#include <algorithm>
#include <utility>

namespace elysiumkv {
namespace {

class ConcatIterator final : public InternalIterator {
public:
    using Open = std::function<Result<std::shared_ptr<SstReader>>(const FileMetadata&)>;

    ConcatIterator(std::shared_ptr<const Version> version, int level, size_t begin, size_t end,
                   Open open)
        : version_(std::move(version)),
          files_(version_->files_at(level)),
          begin_(begin),
          end_(end),
          open_(std::move(open)) {}

    bool valid() const override { return inner_ != nullptr && inner_->valid(); }

    void seek_to_first() override {
        index_ = begin_;
        while (index_ < end_) {
            if (!enter(index_)) return;
            inner_->seek_to_first();
            if (inner_->valid()) return;
            ++index_;  // an empty file, which a truncating compaction can leave
        }
        leave();
    }

    void seek_to_last() override {
        if (begin_ >= end_) return leave();
        index_ = end_ - 1;
        while (true) {
            if (!enter(index_)) return;
            inner_->seek_to_last();
            if (inner_->valid()) return;
            if (index_ == begin_) break;
            --index_;
        }
        leave();
    }

    /// The first file whose largest key is at or above the target: files are disjoint and sorted,
    /// so it is the only one that can hold it, and the first of those that follow.
    void seek(Slice target) override {
        const auto found = std::lower_bound(
            files_.begin() + static_cast<std::ptrdiff_t>(begin_),
            files_.begin() + static_cast<std::ptrdiff_t>(end_), target,
            [](const FileMetadata& file, Slice probe) {
                return Slice::from(file.largest_key) < probe;
            });
        index_ = static_cast<size_t>(found - files_.begin());
        while (index_ < end_) {
            if (!enter(index_)) return;
            inner_->seek(target);
            if (inner_->valid()) return;
            // Past every entry of this file — the target sits in the gap before the next one.
            ++index_;
        }
        leave();
    }

    void seek_for_prev(Slice target) override {
        // The last file whose smallest key is at or below the target, mirroring `seek`.
        const auto after = std::upper_bound(
            files_.begin() + static_cast<std::ptrdiff_t>(begin_),
            files_.begin() + static_cast<std::ptrdiff_t>(end_), target,
            [](Slice probe, const FileMetadata& file) {
                return probe < Slice::from(file.smallest_key);
            });
        if (static_cast<size_t>(after - files_.begin()) == begin_) return leave();
        index_ = static_cast<size_t>(after - files_.begin()) - 1;
        while (true) {
            if (!enter(index_)) return;
            inner_->seek_for_prev(target);
            if (inner_->valid()) return;
            if (index_ == begin_) break;
            --index_;
        }
        leave();
    }

    void next() override {
        if (inner_ == nullptr) return;
        inner_->next();
        while (!inner_->valid()) {
            if (inner_->status() != Status::Ok) return;
            if (index_ + 1 >= end_) return leave();
            ++index_;
            if (!enter(index_)) return;
            inner_->seek_to_first();
        }
    }

    void prev() override {
        if (inner_ == nullptr) return;
        inner_->prev();
        while (!inner_->valid()) {
            if (inner_->status() != Status::Ok) return;
            if (index_ == begin_) return leave();
            --index_;
            if (!enter(index_)) return;
            inner_->seek_to_last();
        }
    }

    Slice key() const override { return inner_->key(); }
    Slice value() const override { return inner_->value(); }
    ValueType type() const override { return inner_->type(); }

    Status status() const override {
        if (status_ != Status::Ok) return status_;
        return inner_ == nullptr ? Status::Ok : inner_->status();
    }

private:
    /// Opens file `at` and positions nothing. False means the open failed and the iterator is done;
    /// `status()` says why, which is how a caller tells that from exhaustion.
    bool enter(size_t at) {
        auto opened = open_(files_[at]);
        if (!opened) {
            status_ = opened.error();
            reader_.reset();
            inner_.reset();
            return false;
        }
        // The reader is held for as long as its iterator is: the entry the caller is looking at
        // points into a block the reader owns.
        reader_ = std::move(*opened);
        inner_ = reader_->iterator();
        return true;
    }

    void leave() {
        inner_.reset();
        reader_.reset();
    }

    std::shared_ptr<const Version> version_;
    const std::vector<FileMetadata>& files_;
    size_t begin_ = 0;
    size_t end_ = 0;
    Open open_;
    size_t index_ = 0;
    std::shared_ptr<SstReader> reader_;
    std::unique_ptr<InternalIterator> inner_;
    Status status_ = Status::Ok;
};

}  // namespace

std::unique_ptr<InternalIterator> make_concat_iterator(
    std::shared_ptr<const Version> version, int level, size_t begin, size_t end,
    std::function<Result<std::shared_ptr<SstReader>>(const FileMetadata&)> open) {
    return std::make_unique<ConcatIterator>(std::move(version), level, begin, end,
                                            std::move(open));
}

}  // namespace elysiumkv
