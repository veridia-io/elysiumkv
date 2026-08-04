#include "sst/block_builder.hpp"

#include <algorithm>
#include <cassert>

namespace elysiumkv {

BlockBuilder::BlockBuilder(int restart_interval) : restart_interval_(restart_interval) {
    assert(restart_interval > 0);
    restarts_.push_back(0);
}

void BlockBuilder::reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    last_key_.clear();
    since_restart_ = 0;
    entries_ = 0;
    finished_ = false;
}

void BlockBuilder::add(Slice key, ValueType type, Slice value) {
    assert(!finished_);
    assert(entries_ == 0 || Slice::from(last_key_) < key);

    size_t shared = 0;
    if (since_restart_ < restart_interval_) {
        const size_t limit = std::min(last_key_.size(), key.size());
        const auto* previous = reinterpret_cast<const uint8_t*>(last_key_.data());
        while (shared < limit && previous[shared] == key.data()[shared]) ++shared;
    } else {
        // Restart point: the full key is stored, which is what makes binary
        // search over the restart array possible.
        restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
        since_restart_ = 0;
    }

    const size_t unshared = key.size() - shared;
    put_varint32(buffer_, static_cast<uint32_t>(shared));
    put_varint32(buffer_, static_cast<uint32_t>(unshared));
    buffer_.push_back(static_cast<char>(static_cast<uint8_t>(type)));
    put_varint32(buffer_, type == ValueType::Delete ? 0u : static_cast<uint32_t>(value.size()));
    buffer_.append(reinterpret_cast<const char*>(key.data() + shared), unshared);
    if (type != ValueType::Delete) {
        buffer_.append(reinterpret_cast<const char*>(value.data()), value.size());
    }

    last_key_.assign(key.as_string_view());
    ++since_restart_;
    ++entries_;
}

Slice BlockBuilder::finish() {
    for (uint32_t offset : restarts_) put_fixed32(buffer_, offset);
    put_fixed32(buffer_, static_cast<uint32_t>(restarts_.size()));
    finished_ = true;
    return Slice::from(buffer_);
}

}  // namespace elysiumkv
