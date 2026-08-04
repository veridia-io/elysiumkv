#ifndef ELYSIUMKV_SLICE_HPP
#define ELYSIUMKV_SLICE_HPP

#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace elysiumkv {

/// An owning byte buffer. What a BlobStore hands back.
using Buffer = std::vector<uint8_t>;

/// ARCHITECTURE.md "Absence is an answer, not an error". A non-owning view; never outlives its source.
class Slice {
public:
    constexpr Slice() = default;
    constexpr Slice(const uint8_t* data, size_t size) : data_(data), size_(size) {}

    static Slice from(std::string_view s) {
        return Slice(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }
    static Slice from(const Buffer& b) { return Slice(b.data(), b.size()); }

    constexpr const uint8_t* data() const { return data_; }
    constexpr size_t size() const { return size_; }
    constexpr bool empty() const { return size_ == 0; }

    std::string_view as_string_view() const {
        return {reinterpret_cast<const char*>(data_), size_};
    }
    std::string to_string() const { return std::string(as_string_view()); }

    /// Bytewise comparison — the only comparator this engine has (ARCHITECTURE.md "Positional recency").
    friend std::strong_ordering operator<=>(Slice a, Slice b) {
        const size_t n = a.size_ < b.size_ ? a.size_ : b.size_;
        if (n != 0) {
            const int c = std::memcmp(a.data_, b.data_, n);
            if (c < 0) return std::strong_ordering::less;
            if (c > 0) return std::strong_ordering::greater;
        }
        return a.size_ <=> b.size_;
    }
    friend bool operator==(Slice a, Slice b) {
        return a.size_ == b.size_ && (a.size_ == 0 || std::memcmp(a.data_, b.data_, a.size_) == 0);
    }

private:
    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
};

/// True when `key` starts with `prefix`. Used by the prefix iterator (ARCHITECTURE.md "Absence is an answer, not an error").
inline bool starts_with(Slice key, Slice prefix) {
    return key.size() >= prefix.size() &&
           std::memcmp(key.data(), prefix.data(), prefix.size()) == 0;
}

/// The smallest key greater than every key with this prefix, or nullopt when the
/// prefix is all 0xFF bytes (or empty) and no such bound exists — the iterator
/// then simply runs to the end of the keyspace.
inline bool prefix_upper_bound(Slice prefix, std::string& out) {
    out.assign(prefix.as_string_view());
    for (size_t i = out.size(); i-- > 0;) {
        auto c = static_cast<unsigned char>(out[i]);
        if (c != 0xFF) {
            out[i] = static_cast<char>(c + 1);
            out.resize(i + 1);
            return true;
        }
    }
    out.clear();
    return false;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SLICE_HPP
