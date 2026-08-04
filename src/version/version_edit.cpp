#include "version/version_edit.hpp"

#include "sst/compression.hpp"
#include "sst/format.hpp"

namespace elysiumkv {
namespace {

constexpr uint32_t kEditFormatVersion = 1;
constexpr uint32_t kSnapshotFormatVersion = 1;
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

void put_file(std::string& out, const FileMetadata& file) {
    put_varint64(out, static_cast<uint64_t>(file.level));
    put_varint64(out, file.file_number);
    put_string(out, file.store_id);
    put_string(out, file.smallest_key);
    put_string(out, file.largest_key);
    put_varint64(out, file.file_bytes);
    put_varint64(out, file.num_entries);
    put_varint64(out, file.num_tombstones);
    put_varint64(out, static_cast<uint64_t>(file.compression));
    put_varint64(out, file.min_write_time_ms);
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
    uint64_t codec = 0;
    if (!get_varint64(p, limit, codec)) return false;
    if (codec > static_cast<uint64_t>(Compression::Zstd)) return false;
    file.compression = static_cast<Compression>(codec);
    return get_varint64(p, limit, file.min_write_time_ms);
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
    return frame(content, Compression::None);
}

Result<VersionEdit> decode_version_edit(Slice bytes) {
    auto content = unframe_block(bytes, kMaxManifestBytes);
    if (!content) return std::unexpected(content.error());

    const uint8_t* p = content->data();
    const uint8_t* const limit = p + content->size();

    uint32_t format = 0;
    if (!get_varint32(p, limit, format) || format != kEditFormatVersion) {
        return std::unexpected(Status::Corrupt);
    }

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
    return edit;
}

std::string encode_version_snapshot(const VersionSnapshot& snapshot) {
    std::string content;
    put_varint32(content, kSnapshotFormatVersion);
    put_varint64(content, snapshot.next_file_number);
    put_varint64(content, snapshot.files.size());
    for (const FileMetadata& file : snapshot.files) put_file(content, file);
    put_pointers(content, snapshot.compaction_pointers);

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
    if (!get_varint32(p, limit, format) || format != kSnapshotFormatVersion) {
        return std::unexpected(Status::Corrupt);
    }

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
    return snapshot;
}

}  // namespace elysiumkv
