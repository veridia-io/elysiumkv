#include "compact/merging_iterator.hpp"

#include <utility>

namespace elysiumkv {
namespace {

/// Linear scan over children rather than a heap: level counts are small (a
/// memtable, a handful of L0 files, one file per deeper level), and a scan keeps
/// the tie-break — lowest child index wins — obvious.
class MergingIterator final : public InternalIterator {
public:
    explicit MergingIterator(std::vector<std::unique_ptr<InternalIterator>> children)
        : children_(std::move(children)) {}

    bool valid() const override { return current_ >= 0; }

    Status status() const override {
        if (status_ != Status::Ok) return status_;
        for (const auto& child : children_) {
            if (child->status() != Status::Ok) return child->status();
        }
        return Status::Ok;
    }

    void seek_to_first() override {
        for (auto& child : children_) child->seek_to_first();
        pick_smallest();
    }

    void seek(Slice target) override {
        for (auto& child : children_) child->seek(target);
        pick_smallest();
    }

    /// The mirror of next(), and the mirror of the tie-break too: on equal keys the earlier child
    /// still wins, because recency is positional and does not depend on which way the scan runs.
    void seek_to_last() override {
        for (auto& child : children_) child->seek_to_last();
        pick_largest();
    }

    void seek_for_prev(Slice target) override {
        for (auto& child : children_) child->seek_for_prev(target);
        pick_largest();
    }

    void prev() override {
        if (current_ < 0) return;
        const size_t current = static_cast<size_t>(current_);
        const Slice key = children_[current]->key();
        for (size_t i = 0; i < children_.size(); ++i) {
            if (i == current) continue;
            if (children_[i]->valid() && children_[i]->key() == key) children_[i]->prev();
        }
        children_[current]->prev();
        pick_largest();
    }

    void next() override {
        if (current_ < 0) return;

        // Advance every *other* child sitting on the emitted key first, so a
        // shadowed entry from an older source is never seen — then advance the
        // current one last. Doing it in that order means the emitted key can be
        // borrowed from the current child instead of copied, which is the
        // difference between one allocation per entry and none (ARCHITECTURE.md "Benchmarks").
        const size_t current = static_cast<size_t>(current_);
        const Slice key = children_[current]->key();
        for (size_t i = 0; i < children_.size(); ++i) {
            if (i == current) continue;
            if (children_[i]->valid() && children_[i]->key() == key) children_[i]->next();
        }
        children_[current]->next();
        pick_smallest();
    }

    Slice key() const override { return children_[static_cast<size_t>(current_)]->key(); }
    Slice value() const override { return children_[static_cast<size_t>(current_)]->value(); }
    ValueType type() const override { return children_[static_cast<size_t>(current_)]->type(); }

private:
    void pick_largest() {
        current_ = -1;
        for (size_t i = 0; i < children_.size(); ++i) {
            if (!children_[i]->valid()) continue;
            if (current_ < 0 ||
                children_[i]->key() > children_[static_cast<size_t>(current_)]->key()) {
                current_ = static_cast<int>(i);
            }
            // A tie leaves `current_` on the earlier child, exactly as the forward pick does.
        }
    }

    void pick_smallest() {
        current_ = -1;
        for (size_t i = 0; i < children_.size(); ++i) {
            if (!children_[i]->valid()) continue;
            if (current_ < 0 ||
                children_[i]->key() < children_[static_cast<size_t>(current_)]->key()) {
                current_ = static_cast<int>(i);
            }
            // On a tie the earlier child already holds `current_`, which is
            // exactly the recency rule: lower level wins, and within L0 the
            // higher file number wins.
        }
    }

    std::vector<std::unique_ptr<InternalIterator>> children_;
    int current_ = -1;
    Status status_ = Status::Ok;
};

}  // namespace

std::unique_ptr<InternalIterator> make_merging_iterator(
    std::vector<std::unique_ptr<InternalIterator>> children) {
    return std::make_unique<MergingIterator>(std::move(children));
}

}  // namespace elysiumkv
