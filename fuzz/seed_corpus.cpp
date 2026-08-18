/* Seeds the fuzzers, by encoding the structures rather than by shipping bytes.
 *
 * **The seeds have to come from the encoders, not from git.** These decoders reject almost
 * everything before they reach anything interesting — a manifest record is CRC-checked framing
 * around a version-tagged body, so random mutation essentially never produces input that gets past
 * the first few bytes. Without valid structure to mutate *from*, the fuzzer spends its whole budget
 * rediscovering the frame.
 *
 * Committing that structure as blobs is the obvious alternative and it rots silently: the manifest
 * format has moved six times, and a seed encoding version five decodes to `Unsupported` on the
 * first byte that matters. It would still be a file in a directory, and the run would still be
 * green, and it would be exercising nothing. Generating at build time cannot drift, because it
 * fails to compile instead.
 *
 * `fuzz/corpus/` is therefore for *regression* inputs only — anything that once crashed — and those
 * are reviewed bytes with a reason to exist.
 */
#include "sst/footer.hpp"
#include "sst/sst_writer.hpp"
#include "version/version_edit.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

using namespace elysiumkv;

static void emit(const std::string& dir, const std::string& name, const void* data, size_t size) {
    std::filesystem::create_directories(dir);
    std::ofstream out(dir + "/" + name, std::ios::binary);
    out.write(static_cast<const char*>(data), static_cast<std::streamsize>(size));
}

int main(int argc, char** argv) {
    const std::string root = argc > 1 ? argv[1] : "fuzz/corpus";

    // A manifest edit with every field populated, so the seed exercises each decode branch.
    VersionEdit edit;
    edit.next_file_number = 4243;
    FileMetadata file;
    file.level = 2;
    file.file_number = 4242;
    file.store_id = "store-1";
    file.smallest_key = "aaa";
    file.largest_key = "zzz";
    file.file_bytes = 123456;
    file.num_entries = 777;
    file.num_tombstones = 3;
    file.num_range_tombstones = 5;
    file.smallest_range_key = "rrr";
    file.largest_range_key = "sss";
    file.compression = Compression::Zstd;
    file.min_write_time_ms = 1'700'000'000'000ull;
    file.max_write_time_ms = 1'700'000'009'999ull;
    file.watermark = WatermarkInterval{80, 100};
    edit.added.push_back(file);
    edit.deleted.push_back({1, 99});
    edit.compaction_pointers.emplace_back(1, "mmm");
    edit.truncation_point = "ttt";
    edit.floor_update = VersionEdit::FloorUpdate::Set;
    edit.watermark_floor = WatermarkFloor{80};
    const std::string encoded_edit = encode_version_edit(edit);
    emit(root + "/edit", "full", encoded_edit.data(), encoded_edit.size());

    VersionEdit bare;
    bare.next_file_number = 1;
    const std::string encoded_bare = encode_version_edit(bare);
    emit(root + "/edit", "minimal", encoded_bare.data(), encoded_bare.size());

    VersionSnapshot snapshot;
    snapshot.next_file_number = 4243;
    snapshot.files.push_back(file);
    snapshot.compaction_pointers.emplace_back(1, "mmm");
    snapshot.truncation_point = "ttt";
    snapshot.watermark_floor = WatermarkFloor{80};
    const std::string encoded_snapshot = encode_version_snapshot(snapshot);
    emit(root + "/snapshot", "full", encoded_snapshot.data(), encoded_snapshot.size());

    // A real SST: its footer seeds one target and its blocks the other.
    SstWriter writer({.bloom_bits_per_key = 10, .compression = Compression::Zstd});
    for (int i = 0; i < 500; ++i) {
        char key[32];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        writer.add(Slice::from(std::string_view(key)), ValueType::Put,
                   Slice::from(std::string(64, 'v')));
    }
    writer.add_range_tombstone(Slice::from(std::string("zz:000")),
                               Slice::from(std::string("zz:100")));
    auto built = writer.finish();
    if (!built) return 1;

    const std::string& bytes = built->bytes;
    const Slice tail(reinterpret_cast<const uint8_t*>(bytes.data()) + bytes.size() -
                         Footer::kFooterLengthV2,
                     Footer::kFooterLengthV2);
    emit(root + "/footer", "v2", tail.data(), tail.size());

    auto footer = Footer::decode(tail);
    if (!footer) return 1;
    // The framed index block, taken by its recorded handle: a whole valid block for the framing
    // decoder, compression byte and CRC included.
    emit(root + "/block", "index", bytes.data() + footer->index.offset, footer->index.length);
    emit(root + "/block", "filter", bytes.data() + footer->filter.offset, footer->filter.length);
    return 0;
}
