#ifndef ELYSIUMKV_UTIL_JITTER_HPP
#define ELYSIUMKV_UTIL_JITTER_HPP

#include <cstdint>

namespace elysiumkv {

/// How wide a window `fraction` of `span_ms` buys. Zero when either is, and NaN reads as zero.
inline uint64_t jitter_window_ms(uint64_t span_ms, double fraction) {
    if (!(fraction > 0.0) || span_ms == 0) return 0;
    if (fraction > 1.0) fraction = 1.0;
    return static_cast<uint64_t>(static_cast<double>(span_ms) * fraction);
}

/// A stable offset in `[0, window_ms)`, derived from the seeds rather than rolled.
///
/// A rolled offset would let the two sites that need one disagree: `placement()` decides which
/// tier a file belongs on and `next_time_transition()` decides when the store next looks at it,
/// so a store would wake at a deadline the placement check no longer agrees is due — or never
/// wake for one that came early. Re-deriving gives both the same answer, and keeps a reopen from
/// re-clustering the files it just spread.
///
/// splitmix64's finalizer, which is what makes two seeds enough: file numbers are handed out
/// consecutively, and a plain modulo would map a burst of them onto a narrow band of offsets
/// rather than across the window.
inline uint64_t jitter_offset(uint64_t seed_a, uint64_t seed_b, uint64_t window_ms) {
    if (window_ms == 0) return 0;
    uint64_t x = seed_a * 0x9e3779b97f4a7c15ull + seed_b;
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x % window_ms;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_UTIL_JITTER_HPP
