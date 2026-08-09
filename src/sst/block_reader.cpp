#include "sst/block_reader.hpp"

#include <utility>

namespace elysiumkv {

BlockIterator::BlockIterator(std::shared_ptr<const Block> block) : block_(std::move(block)) {
    const Slice content = block_->content();
    if (content.size() < sizeof(uint32_t)) {
        status_ = Status::Corrupt;
        return;
    }
    data_ = content.data();
    num_restarts_ = decode_fixed32(data_ + content.size() - sizeof(uint32_t));

    const size_t restart_bytes = static_cast<size_t>(num_restarts_) * sizeof(uint32_t);
    if (num_restarts_ == 0 || restart_bytes + sizeof(uint32_t) > content.size()) {
        status_ = Status::Corrupt;
        data_ = nullptr;
        return;
    }
    restart_array_ = content.size() - sizeof(uint32_t) - restart_bytes;
    entries_end_ = restart_array_;

    // The first restart must address the first entry; anything else means the
    // block was not produced by BlockBuilder.
    if (restart_offset(0) != 0) {
        status_ = Status::Corrupt;
        data_ = nullptr;
    }
}

uint32_t BlockIterator::restart_offset(uint32_t index) const {
    return decode_fixed32(data_ + restart_array_ + static_cast<size_t>(index) * sizeof(uint32_t));
}

void BlockIterator::invalidate() {
    valid_ = false;
    key_.clear();
    value_ = Slice();
}

bool BlockIterator::parse_entry(size_t offset) {
    if (data_ == nullptr || offset >= entries_end_) return false;

    const uint8_t* p = data_ + offset;
    const uint8_t* const limit = data_ + entries_end_;

    uint32_t shared = 0;
    uint32_t unshared = 0;
    uint32_t value_len = 0;
    if (!get_varint32(p, limit, shared) || !get_varint32(p, limit, unshared)) {
        status_ = Status::Corrupt;
        return false;
    }
    if (p >= limit) {
        status_ = Status::Corrupt;
        return false;
    }
    const uint8_t raw_type = *p++;
    if (!is_known_value_type(raw_type)) {
        // 0x02..0xFF are reserved (ARCHITECTURE.md "Inside an SST"). A reader must never guess.
        status_ = Status::Corrupt;
        return false;
    }
    if (!get_varint32(p, limit, value_len)) {
        status_ = Status::Corrupt;
        return false;
    }
    if (shared > key_.size() || static_cast<size_t>(limit - p) < static_cast<size_t>(unshared) ||
        static_cast<size_t>(limit - p) - unshared < value_len) {
        status_ = Status::Corrupt;
        return false;
    }

    key_.truncate(shared);
    key_.append(p, unshared);
    p += unshared;
    type_ = static_cast<ValueType>(raw_type);
    value_ = Slice(p, value_len);
    p += value_len;

    current_offset_ = offset;
    next_offset_ = static_cast<size_t>(p - data_);
    valid_ = true;
    return true;
}

void BlockIterator::seek_to_first() {
    if (status_ != Status::Ok || data_ == nullptr) return;
    key_.clear();
    if (!parse_entry(0)) invalidate();
}

void BlockIterator::next() {
    if (!valid_) return;
    if (!parse_entry(next_offset_)) invalidate();
}

void BlockIterator::seek_to_restart(uint32_t index) {
    key_.clear();  // a restart entry stores its key in full
    if (!parse_entry(restart_offset(index))) invalidate();
}

/// Largest restart index whose entry begins strictly before `offset`.
///
/// Restart offsets ascend, so this is a binary search. A linear walk would be correct too, but a
/// 64 KiB block holds thousands of restarts and prev() would then cost more than the scan it saves.
uint32_t BlockIterator::restart_before(size_t offset) const {
    uint32_t low = 0;
    uint32_t high = num_restarts_ - 1;
    while (low < high) {
        const uint32_t mid = low + (high - low + 1) / 2;
        if (restart_offset(mid) < offset) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    return low;
}

void BlockIterator::seek_to_last() {
    if (status_ != Status::Ok || data_ == nullptr) return;
    seek_to_restart(num_restarts_ - 1);
    while (valid_ && next_offset_ < entries_end_) next();
}

void BlockIterator::prev() {
    if (!valid_) return;
    const size_t target = current_offset_;
    if (target == 0) {
        // Already at the first entry, so there is no previous one. Not an error: the caller above
        // reads this as "this block is spent" and moves to the preceding block.
        invalidate();
        return;
    }

    // restart_offset(0) is 0, which is < target, so a restart at or before us always exists.
    seek_to_restart(restart_before(target));
    while (valid_ && next_offset_ < target) next();
    if (!valid_ || next_offset_ != target) {
        // Landing anywhere but exactly on the boundary means the offsets disagree with the entry
        // stream — a corrupt block, not an exhausted one.
        if (status_ == Status::Ok) status_ = Status::Corrupt;
        invalidate();
    }
}

void BlockIterator::seek_for_prev(Slice target) {
    if (status_ != Status::Ok || data_ == nullptr) return;

    seek(target);
    if (!valid_) {
        // Every key is below target, so the last entry is the answer. Distinguish that from a
        // decode failure, which seek() reports through status_.
        if (status_ != Status::Ok) return;
        seek_to_last();
        return;
    }
    if (key_.slice() == target) return;  // an exact hit satisfies <=
    prev();                              // seek() landed just past target
}

void BlockIterator::seek(Slice target) {
    if (status_ != Status::Ok || data_ == nullptr) return;

    // Largest restart whose key is <= target.
    uint32_t low = 0;
    uint32_t high = num_restarts_ - 1;
    while (low < high) {
        const uint32_t mid = low + (high - low + 1) / 2;
        seek_to_restart(mid);
        if (!valid_) return;
        if (key_.slice() <= target) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }

    seek_to_restart(low);
    while (valid_ && key_.slice() < target) next();
}

}  // namespace elysiumkv
