#ifndef ELYSIUMKV_SST_KEY_BUFFER_HPP
#define ELYSIUMKV_SST_KEY_BUFFER_HPP

#include "elysiumkv/slice.hpp"

#include <algorithm>
#include <cstring>

namespace elysiumkv {

/// The reconstructed key of a prefix-compressed entry (ARCHITECTURE.md "Inside an SST"), held in an inline
/// buffer with a heap fallback.
///
/// A `std::string` here costs one allocation per entry for any key longer than
/// the small-string limit — which is to say, for most real keys. ARCHITECTURE.md "Benchmarks" makes
/// zero allocations on the hot path a hard assertion, and this is where the
/// allocations were.
class KeyBuffer {
public:
    static constexpr size_t kInlineCapacity = 128;

    KeyBuffer() = default;
    ~KeyBuffer() { release(); }

    KeyBuffer(KeyBuffer&& other) noexcept { adopt(std::move(other)); }
    KeyBuffer& operator=(KeyBuffer&& other) noexcept {
        if (this != &other) {
            release();
            adopt(std::move(other));
        }
        return *this;
    }
    KeyBuffer(const KeyBuffer&) = delete;
    KeyBuffer& operator=(const KeyBuffer&) = delete;

    void clear() { size_ = 0; }

    /// Keeps the first `n` bytes — the shared prefix of the next entry.
    void truncate(size_t n) { size_ = std::min(size_, n); }

    void append(const uint8_t* data, size_t count) {
        if (count == 0) return;
        reserve(size_ + count);
        std::memcpy(buffer() + size_, data, count);
        size_ += count;
    }

    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }
    const uint8_t* data() const { return heap_ != nullptr ? heap_ : inline_; }
    Slice slice() const { return Slice(data(), size_); }

private:
    uint8_t* buffer() { return heap_ != nullptr ? heap_ : inline_; }

    void reserve(size_t needed) {
        if (needed <= capacity_) return;
        size_t capacity = capacity_ * 2;
        while (capacity < needed) capacity *= 2;

        auto* grown = new uint8_t[capacity];
        std::memcpy(grown, data(), size_);
        delete[] heap_;
        heap_ = grown;
        capacity_ = capacity;
    }

    void release() {
        delete[] heap_;
        heap_ = nullptr;
        capacity_ = kInlineCapacity;
        size_ = 0;
    }

    void adopt(KeyBuffer&& other) {
        size_ = other.size_;
        capacity_ = other.capacity_;
        heap_ = other.heap_;
        if (heap_ == nullptr && size_ > 0) std::memcpy(inline_, other.inline_, size_);
        other.heap_ = nullptr;
        other.capacity_ = kInlineCapacity;
        other.size_ = 0;
    }

    uint8_t inline_[kInlineCapacity] = {};
    uint8_t* heap_ = nullptr;
    size_t capacity_ = kInlineCapacity;
    size_t size_ = 0;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_KEY_BUFFER_HPP
