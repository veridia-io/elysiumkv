#include "version/version_edit.hpp"

#include "sst/compression.hpp"
#include "sst/format.hpp"

namespace elysiumkv {
namespace {

constexpr uint32_t kEditFormatVersion = 6;
constexpr uint32_t kSnapshotFormatVersion = 6;
/// A manifest snapshot for a mature store is a few hundred KB; the bound only
/// has to keep a corrupt length from becoming an allocation.
constexpr size_t kMaxManifestBytes = 256u << 20;

void put_string(std::string& out, const std::string& value) {
    put_varint64(out, value.size());
    out.append(value);
}

bool get_string(const uint8_t*& p, const uint8_t* limit, std::string& out) {
    uint64_t size = 0;
    if (!get_varint64(p, limit, size)) return false;
    if (static_cast<uint64_t>(limit - p) < size) return false;
    out.assign(reinterpret_cast<const char*>(p), size);
    p += size;
    return true;
}

/// Presence is encoded ahead of the values rather than inferred from them, because **zero is a
/// valid watermark**. One varint carrying two bits keeps the record all-varint; `high` present
/// with `low` absent is the legitimate state of a file whose memtable predates the first
/// `set_watermark` call, so the two bits are independent rather than a tri-state count.
void put_watermark(std::string& out, const WatermarkInterval& watermark) {
    const uint64_t flags = (watermark.low.has_value() ? 1u : 0u) |
                           (watermark.high.has_value() ? 2u : 0u);
    put_varint64(out, flags);
    put_varint64(out, watermark.low.value_or(0));
    put_varint64(out, watermark.high.value_or(0));
}

bool get_watermark(const uint8_t*& p, const uint8_t* limit, WatermarkInterval& watermark) {
    uint64_t flags = 0;
    if (!get_varint64(p, limit, flags)) return false;
    if (flags > 3) return false;   // only the two defined bits
    uint64_t low = 0;
    uint64_t high = 0;
    if (!get_varint64(p, limit, low)) return false;
    if (!get_varint64(p, limit, high)) return false;

    // A `low` without a `high` is not reachable — a `low` is only ever a previously established
    // `high` — so it is corruption rather than a state to tolerate.
    if ((flags & 1u) != 0 && (flags & 2u) == 0) return false;
    watermark.low = (flags & 1u) != 0 ? std::optional<uint64_t>(low) : std::nullopt;
    watermark.high = (flags & 2u) != 0 ? std::optional<uint64_t>(high) : std::nullopt;
    // An inverted interval cannot arise from `min` over lows and `max` over highs.
    if (watermark.low.has_value() && *watermark.low > *watermark.high) return false;
    return true;
}

void put_file(std::string& out, const FileMetadata& file) {
    put_varint64(out, static_cast<uint64_t>(file.level));
    put_varint64(out, file.file_number);
    put_string(out, file.store_id);
    put_string(out, file.smallest_key);
    put_string(out, file.largest_key);
    put_varint64(out, file.file_bytes);
    put_varint64(out, file.num_entries);
    put_varint64(out, file.num_tombstones);
    put_varint64(out, file.num_range_tombstones);
    put_string(out, file.smallest_range_key);
    put_string(out, file.largest_range_key);
    put_varint64(out, static_cast<uint64_t>(file.compression));
    put_varint64(out, file.min_write_time_ms);
    put_varint64(out, file.max_write_time_ms);
    put_watermark(out, file.watermark);
}

bool get_file(const uint8_t*& p, const uint8_t* limit, FileMetadata& file) {
    uint64_t level = 0;
    if (!get_varint64(p, limit, level)) return false;
    file.level = static_cast<int>(level);
    if (!get_varint64(p, limit, file.file_number)) return false;
    if (!get_string(p, limit, file.store_id)) return false;
    if (!get_string(p, limit, file.smallest_key)) return false;
    if (!get_string(p, limit, file.largest_key)) return false;
    if (!get_varint64(p, limit, file.file_bytes)) return false;
    if (!get_varint64(p, limit, file.num_entries)) return false;
    if (!get_varint64(p, limit, file.num_tombstones)) return false;
    if (!get_varint64(p, limit, file.num_range_tombstones)) return false;
    if (!get_string(p, limit, file.smallest_range_key)) return false;
    if (!get_string(p, limit, file.largest_range_key)) return false;
    uint64_t codec = 0;
    if (!get_varint64(p, limit, codec)) return false;
    if (codec > static_cast<uint64_t>(Compression::Zstd)) return false;
    file.compression = static_cast<Compression>(codec);
    if (!get_varint64(p, limit, file.min_write_time_ms)) return false;
    if (!get_varint64(p, limit, file.max_write_time_ms)) return false;
    return get_watermark(p, limit, file.watermark);
}

void put_pointers(std::string& out, const std::vector<std::pair<int, std::string>>& pointers) {
    put_varint64(out, pointers.size());
    for (const auto& [level, key] : pointers) {
        put_varint64(out, static_cast<uint64_t>(level));
        put_string(out, key);
    }
}

bool get_pointers(const uint8_t*& p, const uint8_t* limit,
                  std::vector<std::pair<int, std::string>>& pointers) {
    uint64_t count = 0;
    if (!get_varint64(p, limit, count)) return false;
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t level = 0;
        std::string key;
        if (!get_varint64(p, limit, level) || !get_string(p, limit, key)) return false;
        pointers.emplace_back(static_cast<int>(level), std::move(key));
    }
    return true;
}

/// Records reuse the SST block framing: payload, uncompressed length, codec byte
/// and CRC32C. One framing, one place that can be wrong.
std::string frame(const std::string& content, Compression codec) {
    std::string out;
    (void)frame_block(Slice::from(content), codec, out);
    return out;
}

/// Three states, not two: **no loss recorded**, **a loss permitting a position**, and **a loss
/// permitting nothing**. Zero is a valid position, so none of them can be inferred from the value.
/// An edit's instruction: 0 silent, 1 set a position, 2 set "certifies nothing", 3 clear.
void put_floor_update(std::string& out, VersionEdit::FloorUpdate update,
                      const WatermarkFloor& floor) {
    uint64_t state = 0;
    switch (update) {
        case VersionEdit::FloorUpdate::Silent: state = 0; break;
        case VersionEdit::FloorUpdate::Set: state = floor.position.has_value() ? 1u : 2u; break;
        case VersionEdit::FloorUpdate::Clear: state = 3; break;
    }
    put_varint64(out, state);
    put_varint64(out, floor.position.value_or(0));
}

bool get_floor_update(const uint8_t*& p, const uint8_t* limit, VersionEdit::FloorUpdate& update,
                      WatermarkFloor& floor) {
    uint64_t state = 0;
    uint64_t value = 0;
    if (!get_varint64(p, limit, state)) return false;
    if (state > 3) return false;
    if (!get_varint64(p, limit, value)) return false;
    switch (state) {
        case 0: update = VersionEdit::FloorUpdate::Silent; break;
        case 1:
            update = VersionEdit::FloorUpdate::Set;
            floor.position = value;
            break;
        case 2:
            update = VersionEdit::FloorUpdate::Set;
            floor.position = std::nullopt;
            break;
        default: update = VersionEdit::FloorUpdate::Clear; break;
    }
    return true;
}

void put_floor(std::string& out, const std::optional<WatermarkFloor>& floor) {
    const uint64_t state = !floor.has_value()          ? 0u
                           : floor->position.has_value() ? 1u
                                                         : 2u;
    put_varint64(out, state);
    put_varint64(out, floor.has_value() ? floor->position.value_or(0) : 0);
}

bool get_floor(const uint8_t*& p, const uint8_t* limit, std::optional<WatermarkFloor>& floor) {
    uint64_t state = 0;
    uint64_t value = 0;
    if (!get_varint64(p, limit, state)) return false;
    if (state > 2) return false;
    if (!get_varint64(p, limit, value)) return false;
    if (state == 0) {
        floor = std::nullopt;
    } else {
        floor = WatermarkFloor{state == 1 ? std::optional<uint64_t>(value) : std::nullopt};
    }
    return true;
}

}  // namespace

std::string encode_version_edit(const VersionEdit& edit) {
    std::string content;
    put_varint32(content, kEditFormatVersion);
    put_varint64(content, edit.next_file_number);

    put_varint64(content, edit.added.size());
    for (const FileMetadata& file : edit.added) put_file(content, file);

    put_varint64(content, edit.deleted.size());
    for (const FileRef& ref : edit.deleted) {
        put_varint64(content, static_cast<uint64_t>(ref.level));
        put_varint64(content, ref.file_number);
    }

    put_pointers(content, edit.compaction_pointers);
    put_string(content, edit.truncation_point);
    put_floor_update(content, edit.floor_update, edit.watermark_floor);
    return frame(content, Compression::None);
}

Result<VersionEdit> decode_version_edit(Slice bytes) {
    auto content = unframe_block(bytes, kMaxManifestBytes);
    if (!content) return std::unexpected(content.error());

    const uint8_t* p = content->data();
    const uint8_t* const limit = p + content->size();

    uint32_t format = 0;
    if (!get_varint32(p, limit, format)) return std::unexpected(Status::Corrupt);
    // `unframe_block` has already verified the checksum, so these bytes are the ones that were
    // written: a version we don't recognise is a real version, not damage. There is deliberately
    // no dual-read path — a format change is a clean break, and reporting `Unsupported` tells the
    // operator to run a different binary rather than to reach for a restore.
    if (format != kEditFormatVersion) return std::unexpected(Status::Unsupported);

    VersionEdit edit;
    if (!get_varint64(p, limit, edit.next_file_number)) return std::unexpected(Status::Corrupt);

    uint64_t added = 0;
    if (!get_varint64(p, limit, added)) return std::unexpected(Status::Corrupt);
    for (uint64_t i = 0; i < added; ++i) {
        FileMetadata file;
        if (!get_file(p, limit, file)) return std::unexpected(Status::Corrupt);
        edit.added.push_back(std::move(file));
    }

    uint64_t deleted = 0;
    if (!get_varint64(p, limit, deleted)) return std::unexpected(Status::Corrupt);
    for (uint64_t i = 0; i < deleted; ++i) {
        uint64_t level = 0;
        FileRef ref;
        if (!get_varint64(p, limit, level) || !get_varint64(p, limit, ref.file_number)) {
            return std::unexpected(Status::Corrupt);
        }
        ref.level = static_cast<int>(level);
        edit.deleted.push_back(ref);
    }

    if (!get_pointers(p, limit, edit.compaction_pointers)) return std::unexpected(Status::Corrupt);
    if (!get_string(p, limit, edit.truncation_point)) return std::unexpected(Status::Corrupt);
    if (!get_floor_update(p, limit, edit.floor_update, edit.watermark_floor)) {
        return std::unexpected(Status::Corrupt);
    }
    return edit;
}

std::string encode_version_snapshot(const VersionSnapshot& snapshot) {
    std::string content;
    put_varint32(content, kSnapshotFormatVersion);
    put_varint64(content, snapshot.next_file_number);
    put_varint64(content, snapshot.files.size());
    for (const FileMetadata& file : snapshot.files) put_file(content, file);
    put_pointers(content, snapshot.compaction_pointers);
    put_string(content, snapshot.truncation_point);
    put_floor(content, snapshot.watermark_floor);

    // Unlike SST data blocks, a snapshot is always read whole, so whole-object
    // compression is the right shape here.
    return frame(content, Compression::Zstd);
}

Result<VersionSnapshot> decode_version_snapshot(Slice bytes) {
    auto content = unframe_block(bytes, kMaxManifestBytes);
    if (!content) return std::unexpected(content.error());

    const uint8_t* p = content->data();
    const uint8_t* const limit = p + content->size();

    uint32_t format = 0;
    if (!get_varint32(p, limit, format)) return std::unexpected(Status::Corrupt);
    // `unframe_block` has already verified the checksum, so these bytes are the ones that were
    // written: a version we don't recognise is a real version, not damage. There is deliberately
    // no dual-read path — a format change is a clean break, and reporting `Unsupported` tells the
    // operator to run a different binary rather than to reach for a restore.
    if (format != kSnapshotFormatVersion) return std::unexpected(Status::Unsupported);

    VersionSnapshot snapshot;
    uint64_t files = 0;
    if (!get_varint64(p, limit, snapshot.next_file_number) || !get_varint64(p, limit, files)) {
        return std::unexpected(Status::Corrupt);
    }
    for (uint64_t i = 0; i < files; ++i) {
        FileMetadata file;
        if (!get_file(p, limit, file)) return std::unexpected(Status::Corrupt);
        snapshot.files.push_back(std::move(file));
    }
    if (!get_pointers(p, limit, snapshot.compaction_pointers)) {
        return std::unexpected(Status::Corrupt);
    }
    if (!get_string(p, limit, snapshot.truncation_point)) return std::unexpected(Status::Corrupt);
    if (!get_floor(p, limit, snapshot.watermark_floor)) return std::unexpected(Status::Corrupt);
    return snapshot;
}

}  // namespace elysiumkv
