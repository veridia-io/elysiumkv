#ifndef ELYSIUMKV_VERSION_WATERMARK_HPP
#define ELYSIUMKV_VERSION_WATERMARK_HPP

#include <algorithm>
#include <cstdint>
#include <optional>

namespace elysiumkv {

/// The embedder's durability frontier, carried as an **interval** rather than a single value.
///
/// A watermark is a position in someone else's log — a changelog offset, for the Kafka Streams
/// adapter. The engine orders it, propagates it and hands it back; it never invents, interpolates
/// or interprets one, and it is unrelated to `min_write_time_ms` or to a tier's `max_age`, which
/// are wall-clock quantities driving placement.
///
/// Why two values and not one. `set_watermark(M)` asserts that every write completed so far is at
/// a position ≤ M, so every write accepted afterwards is at a position > M. That makes
///
/// - **`low`** — the last watermark established when the source memtable was *created* — a strict
///   *lower* bound on the positions the data occupies: the file contains no write at a position
///   ≤ `low`. This is the half recovery leans on. Losing a file invalidates only positions
///   after its `low`, so `min(low)` over a discarded set is a position everything below which
///   still survives.
/// - **`high`** — the last watermark established before that memtable was sealed — an *upper*
///   bound, and the half that is safe to report when nothing was lost.
///
/// A single scalar cannot do both jobs, and reporting `high` for a lost file over-reports
/// durability: a memtable that begins at 80 and takes writes up to 100 before
/// `set_watermark(100)` has `low = 80, high = 100`, and losing it costs the write at 81.
///
/// Presence is tracked separately from the value because **zero is a valid position** — a store
/// legitimately at the start of its log. `high` can be present while `low` is absent: a memtable
/// that predates the first `set_watermark` call has no lower bound at all, and losing it means
/// nothing can be certified. `low` present implies `high` present, since a `low` is only ever a
/// previously established `high`.
struct WatermarkInterval {
    std::optional<uint64_t> low;
    std::optional<uint64_t> high;

    bool empty() const { return !low.has_value() && !high.has_value(); }

    /// How a compaction output's interval is formed from its inputs: `min` of the lows, `max` of
    /// the highs. The `min` is deliberate and is what the recovery proof needs — the output holds
    /// every input's data, so it inherits the weakest lower bound among them. An absent `low` on
    /// any input makes the output's `low` absent for the same reason: that input carried no
    /// lower bound to inherit.
    void merge(const WatermarkInterval& other) {
        if (!other.low.has_value()) {
            low.reset();
        } else if (low.has_value()) {
            low = std::min(*low, *other.low);
        }
        if (other.high.has_value()) {
            high = high.has_value() ? std::max(*high, *other.high) : other.high;
        }
    }

    bool operator==(const WatermarkInterval&) const = default;
};

/// Folds `value` into a running `min`, treating an absent accumulator as "nothing seen yet".
inline void accumulate_min(std::optional<uint64_t>& into, std::optional<uint64_t> value) {
    if (!value.has_value()) return;
    into = into.has_value() ? std::min(*into, *value) : value;
}

/// The `max` counterpart of `accumulate_min`.
inline void accumulate_max(std::optional<uint64_t>& into, std::optional<uint64_t> value) {
    if (!value.has_value()) return;
    into = into.has_value() ? std::max(*into, *value) : value;
}

/// Everything recovery needs to choose a resume position, shaped so that the unsound answer cannot
/// be written.
///
/// **The controlling variable is `anything_discarded`, and it is not derivable from the bounds.**
/// The tempting formulation is `coalesce(discarded_lower_bound, surviving_upper_bound)` — take the
/// discard bound if there is one, otherwise fall back. That conflates two states with *opposite*
/// recovery implications: an empty discard set, where the surviving upper bound certifies a complete
/// prefix, and a non-empty discard set that could not produce a bound, where nothing can be
/// certified at all. So the flag is stored rather than inferred, and `resume_after` branches on it
/// first.
///
/// `surviving_upper_bound` is deliberately kept even when files were discarded, and deliberately
/// not read in that case. Keeping it puts the rejected fallback in front of the next reader at the
/// exact point they would reach for it, instead of leaving the absence to be rediscovered.
class RecoveryWatermark {
public:
    /// Folds in a file that recovery is dropping. **A file with no lower bound makes the whole
    /// discard bound absent, not merely skipped** — it is not evidence of nothing, it is the absence
    /// of evidence. Taking `min` over only the files that happen to have a bound would ignore
    /// precisely the file that might hold the only copy of an earlier write.
    void observe_discarded(const WatermarkInterval& watermark) {
        anything_discarded_ = true;
        if (!watermark.low.has_value()) {
            discarded_lower_bound_absent_ = true;
            return;
        }
        accumulate_min(discarded_lower_bound_, watermark.low);
    }

    /// Folds in a file that survived recovery.
    void observe_survivor(const WatermarkInterval& watermark) {
        accumulate_max(surviving_upper_bound_, watermark.high);
    }

    bool anything_discarded() const { return anything_discarded_; }

    /// `min` of the lows over the discarded set, or `nullopt` when any of them had no low. Only
    /// meaningful when `anything_discarded()`.
    std::optional<uint64_t> discarded_lower_bound() const {
        return discarded_lower_bound_absent_ ? std::nullopt : discarded_lower_bound_;
    }

    std::optional<uint64_t> surviving_upper_bound() const { return surviving_upper_bound_; }

    /// The position to resume strictly after, or `nullopt` to replay from the beginning.
    ///
    /// | State | Result | Why |
    /// | --- | --- | --- |
    /// | nothing discarded | `max(high)` over survivors | every write ever made is still in some file, so the newest established watermark is fully covered |
    /// | discarded, all had a low | `min(low)` over the discarded set | a write at or below that minimum cannot have lived only in a discarded file |
    /// | discarded, any lacked a low | `nullopt` | that file may hold the only copy of a write at *any* position, so no prefix is provable |
    ///
    /// **`max(high)` is not a weaker-but-valid fallback after a loss — it stops being a bound at
    /// all.** Its justification is that watermarks are non-decreasing, that compaction takes the
    /// `max` of its inputs' highs, and that *no state was lost*. A discard falsifies the last
    /// clause: a surviving file with `high = 100` says the prefix through 100 had been established
    /// when its lineage was produced, not that this surviving set still contains all state through
    /// 100 after unrelated files were deleted.
    std::optional<uint64_t> resume_after() const {
        if (anything_discarded_) return discarded_lower_bound();
        return surviving_upper_bound_;
    }

private:
    bool anything_discarded_ = false;
    bool discarded_lower_bound_absent_ = false;
    std::optional<uint64_t> discarded_lower_bound_;
    std::optional<uint64_t> surviving_upper_bound_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_VERSION_WATERMARK_HPP
