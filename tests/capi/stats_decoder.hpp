#ifndef ELYSIUMKV_TESTS_CAPI_STATS_DECODER_HPP
#define ELYSIUMKV_TESTS_CAPI_STATS_DECODER_HPP

#include "elysiumkv/elysiumkv.h"

#include <cstdint>
#include <cstring>
#include <vector>

namespace elysiumkv::test {

/// Decodes what `elysiumkv_stats_snapshot` writes. This is a *reference* decoder as
/// much as a test helper: every binding has to write one of these, and the rule
/// it demonstrates is the one that keeps the format extensible — records are
/// located by the declared `header_bytes` and `*_record_bytes`, never by summing
/// the field widths this decoder happens to know about. Add a field to the C++
/// `Stats` and this keeps working, reading the old subset from longer records.
struct DecodedLevel {
    int32_t level = 0;
    int32_t file_count = 0;
    uint64_t bytes = 0;
    uint64_t oldest_file_age_ms = 0;
    int32_t files_stale_codec = 0;
    bool age_triggered = false;
    bool stalling = false;
    uint64_t entries = 0;
    uint64_t tombstones = 0;
};

struct DecodedTier {
    int32_t tier = 0;
    int32_t file_count = 0;
    uint64_t bytes = 0;
    uint64_t oldest_file_age_ms = 0;
    int32_t files_pending_migration = 0;
    bool stalling = false;
};

struct DecodedStats {
    uint32_t format_version = 0;
    bool requires_recovery = false;
    uint64_t memtable_bytes = 0;
    uint64_t memtable_age_ms = 0;
    uint64_t compactions = 0;
    uint64_t compaction_bytes_read = 0;
    uint64_t compaction_bytes_written = 0;
    uint64_t migrations = 0;
    uint64_t migration_bytes = 0;
    uint64_t stalled_total_ms = 0;
    uint64_t stall_count = 0;
    uint64_t block_cache_hits = 0;
    uint64_t block_cache_misses = 0;
    uint64_t block_cache_bytes = 0;
    uint64_t pins_outstanding = 0;
    uint64_t reader_cache_hits = 0;
    uint64_t reader_cache_misses = 0;
    uint64_t reader_cache_bytes = 0;
    uint64_t open_readers = 0;
    uint64_t memory_budget_used = 0;
    uint64_t memory_budget_total = 0;
    uint64_t budget_sheds = 0;
    uint64_t flushes = 0;
    /// Absent, not zero, when no watermark has been set — zero is a valid position, so an
    /// exporter must omit the series rather than publish it.
    uint64_t durable_watermark = 0;
    bool watermark_present = false;
    uint64_t memtable_entries = 0;
    uint64_t memtable_tombstones = 0;
    uint64_t background_failures = 0;
    uint64_t compactions_trimmed = 0;
    uint64_t reencryptions = 0;
    uint64_t files_pending_reencryption = 0;
    std::vector<DecodedLevel> levels;
    std::vector<DecodedTier> tiers;

    /// Every file sits in exactly one level and exactly one tier, so these two
    /// totals are the same number viewed along the two axes — but only within a
    /// single snapshot. This is the identity the torn per-accessor design could
    /// not hold.
    uint64_t level_bytes_total() const {
        uint64_t total = 0;
        for (const DecodedLevel& level : levels) total += level.bytes;
        return total;
    }
    uint64_t tier_bytes_total() const {
        uint64_t total = 0;
        for (const DecodedTier& tier : tiers) total += tier.bytes;
        return total;
    }

    /// Records the store physically holds — an upper bound on distinct live keys, never an
    /// estimate of them. See `LevelStats::entries`.
    uint64_t entry_count() const {
        uint64_t total = memtable_entries - memtable_tombstones;
        for (const DecodedLevel& level : levels) total += level.entries - level.tombstones;
        return total;
    }
};

namespace detail {

inline uint32_t read_u32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | static_cast<uint32_t>(p[1]) << 8 |
           static_cast<uint32_t>(p[2]) << 16 | static_cast<uint32_t>(p[3]) << 24;
}

inline uint64_t read_u64(const uint8_t* p) {
    return static_cast<uint64_t>(read_u32(p)) | static_cast<uint64_t>(read_u32(p + 4)) << 32;
}

inline int32_t read_i32(const uint8_t* p) { return static_cast<int32_t>(read_u32(p)); }

}  // namespace detail

inline DecodedStats decode_stats(const uint8_t* buf, size_t size) {
    using detail::read_i32;
    using detail::read_u32;
    using detail::read_u64;

    DecodedStats out;
    if (size < 24) return out;

    out.format_version = read_u32(buf);
    const size_t header_bytes = read_u32(buf + 4);
    const size_t level_record_bytes = read_u32(buf + 8);
    const size_t tier_record_bytes = read_u32(buf + 12);
    const size_t level_count = read_u32(buf + 16);
    const size_t tier_count = read_u32(buf + 20);

    out.requires_recovery = buf[24] != 0;
    const uint8_t* scalars = buf + 32;
    out.memtable_bytes = read_u64(scalars + 0);
    out.memtable_age_ms = read_u64(scalars + 8);
    out.compactions = read_u64(scalars + 16);
    out.compaction_bytes_read = read_u64(scalars + 24);
    out.compaction_bytes_written = read_u64(scalars + 32);
    out.migrations = read_u64(scalars + 40);
    out.migration_bytes = read_u64(scalars + 48);
    out.stalled_total_ms = read_u64(scalars + 56);
    out.stall_count = read_u64(scalars + 64);
    out.block_cache_hits = read_u64(scalars + 72);
    out.block_cache_misses = read_u64(scalars + 80);
    out.block_cache_bytes = read_u64(scalars + 88);
    out.pins_outstanding = read_u64(scalars + 96);
    out.reader_cache_hits = read_u64(scalars + 104);
    out.reader_cache_misses = read_u64(scalars + 112);
    out.reader_cache_bytes = read_u64(scalars + 120);
    out.open_readers = read_u64(scalars + 128);
    out.memory_budget_used = read_u64(scalars + 136);
    out.memory_budget_total = read_u64(scalars + 144);
    out.budget_sheds = read_u64(scalars + 152);
    out.flushes = read_u64(scalars + 160);              // buffer offset 192
    out.durable_watermark = read_u64(scalars + 168);    // buffer offset 200
    out.watermark_present = scalars[176] != 0;          // buffer offset 208
    out.memtable_entries = read_u64(scalars + 184);     // buffer offset 216
    out.memtable_tombstones = read_u64(scalars + 192);  // buffer offset 224
    out.background_failures = read_u64(scalars + 200);  // buffer offset 232
    out.compactions_trimmed = read_u64(scalars + 208);  // buffer offset 240
    out.reencryptions = read_u64(scalars + 216);        // buffer offset 248
    out.files_pending_reencryption = read_u64(scalars + 224);  // buffer offset 256

    size_t offset = header_bytes;
    for (size_t i = 0; i < level_count && offset + level_record_bytes <= size; ++i) {
        const uint8_t* r = buf + offset;
        DecodedLevel level;
        level.level = read_i32(r + 0);
        level.file_count = read_i32(r + 4);
        level.bytes = read_u64(r + 8);
        level.oldest_file_age_ms = read_u64(r + 16);
        level.files_stale_codec = read_i32(r + 24);
        level.age_triggered = r[28] != 0;
        level.stalling = r[29] != 0;
        if (level_record_bytes >= 48) {
            level.entries = read_u64(r + 32);
            level.tombstones = read_u64(r + 40);
        }
        out.levels.push_back(level);
        offset += level_record_bytes;
    }
    for (size_t i = 0; i < tier_count && offset + tier_record_bytes <= size; ++i) {
        const uint8_t* r = buf + offset;
        DecodedTier tier;
        tier.tier = read_i32(r + 0);
        tier.file_count = read_i32(r + 4);
        tier.bytes = read_u64(r + 8);
        tier.oldest_file_age_ms = read_u64(r + 16);
        tier.files_pending_migration = read_i32(r + 24);
        tier.stalling = r[28] != 0;
        out.tiers.push_back(tier);
        offset += tier_record_bytes;
    }
    return out;
}

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_CAPI_STATS_DECODER_HPP
