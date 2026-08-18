#include "sst/sst_writer.hpp"

#include "sst/compression.hpp"
#include "sst/format.hpp"

#include <cassert>

namespace elysiumkv {
namespace {

std::string encode_handle(const BlockHandle& handle) {
    std::string out;
    put_varint64(out, handle.offset);
    put_varint64(out, handle.length);
    return out;
}

}  // namespace

SstWriter::SstWriter(SstOptions options)
    : options_(options),
      data_block_(options.restart_interval),
      index_block_(options.restart_interval),
      range_del_block_(options.restart_interval),
      bloom_(options.bloom_bits_per_key, options.bloom_probes) {}

void SstWriter::add(Slice key, ValueType type, Slice value) {
    assert(!finished_);
    if (num_entries_ == 0) smallest_key_.assign(key.as_string_view());
    largest_key_.assign(key.as_string_view());
    ++num_entries_;
    if (type == ValueType::Delete) ++num_tombstones_;

    bloom_.add(key);
    data_block_.add(key, type, value);

    if (data_block_.size_estimate() >= options_.block_bytes) {
        (void)flush_data_block();
    }
}

size_t SstWriter::estimated_bytes() const {
    return file_.size() + data_block_.size_estimate() + index_block_.size_estimate() +
           static_cast<size_t>(Footer::kFooterLengthV1);
}

Status SstWriter::flush_data_block() {
    if (data_block_.empty()) return Status::Ok;

    const std::string last_key = data_block_.last_key().to_string();
    const Slice content = data_block_.finish();

    BlockHandle handle;
    handle.offset = file_.size();
    const Status status = frame_block(content, options_.compression, file_);
    if (status != Status::Ok) return status;
    handle.length = static_cast<uint32_t>(file_.size() - handle.offset);

    // One index entry per data block: key = last key in that block (ARCHITECTURE.md "The invariant trailer").
    const std::string encoded = encode_handle(handle);
    index_block_.add(Slice::from(last_key), ValueType::Put, Slice::from(encoded));

    data_block_.reset();
    return Status::Ok;
}

void SstWriter::add_range_tombstone(Slice lower, Slice upper) {
    assert(!finished_);
    assert(lower < upper);   // an empty range deletes nothing and has no business in the file
    // Stored as an ordinary block entry — start as the key, end as the value — so the range
    // tombstones get the same framing, checksum, compression and prefix compression as everything
    // else, and `seek_for_prev` answers "which tombstone covers this key" without a scan.
    //
    // **`Put`, not `Delete`**, even though every entry here is a deletion: the type byte says
    // whether the entry carries a value, and `BlockBuilder` drops the value of a `Delete` on the
    // floor. The upper bound *is* the value, so a tombstone written as a `Delete` would lose the
    // half of itself that says where it ends. What makes these deletions is the block they are in.
    range_del_block_.add(lower, ValueType::Put, upper);
    if (num_range_tombstones_ == 0) smallest_range_key_.assign(lower.data(), lower.data() + lower.size());
    largest_range_key_.assign(upper.data(), upper.data() + upper.size());
    ++num_range_tombstones_;
}

Result<SstBuildResult> SstWriter::finish() {
    assert(!finished_);
    finished_ = true;

    if (Status status = flush_data_block(); status != Status::Ok) {
        return std::unexpected(status);
    }

    // Filter block: never compressed. A bloom bitmap is near-incompressible and
    // decompression would sit on the critical path of every negative lookup —
    // exactly what the filter exists to make cheap (ARCHITECTURE.md "Inside an SST").
    Footer footer;
    const std::string filter = bloom_.finish();
    footer.filter.offset = file_.size();
    if (Status status = frame_block(Slice::from(filter), Compression::None, file_);
        status != Status::Ok) {
        return std::unexpected(status);
    }
    footer.filter.length = static_cast<uint32_t>(file_.size() - footer.filter.offset);

    const Slice index_content = index_block_.finish();
    footer.index.offset = file_.size();
    if (Status status = frame_block(index_content, options_.compression, file_);
        status != Status::Ok) {
        return std::unexpected(status);
    }
    footer.index.length = static_cast<uint32_t>(file_.size() - footer.index.offset);

    // Only a file that carries one is written as v2: the version is per file, so a reader that
    // predates range tombstones keeps reading every file that has none, and refuses exactly the
    // files whose contents it would otherwise misreport.
    if (num_range_tombstones_ != 0) {
        const Slice range_content = range_del_block_.finish();
        footer.range_del.offset = file_.size();
        if (Status status = frame_block(range_content, options_.compression, file_);
            status != Status::Ok) {
            return std::unexpected(status);
        }
        footer.range_del.length = static_cast<uint32_t>(file_.size() - footer.range_del.offset);
    }

    // **Every file, not only the ones that need a new field.** `range_del` is per file because a
    // reader that cannot honour a range tombstone must refuse exactly the files carrying one; a
    // checksum is not like that — one written only sometimes leaves most files unprotected.
    footer.format_version = Footer::kFormatVersion3;

    footer.num_entries = num_entries_;
    file_.append(footer.encode());

    SstBuildResult result;
    result.bytes = std::move(file_);
    result.num_entries = num_entries_;
    result.num_tombstones = num_tombstones_;
    result.smallest_key = std::move(smallest_key_);
    result.largest_key = std::move(largest_key_);
    result.num_range_tombstones = num_range_tombstones_;
    result.smallest_range_key = std::move(smallest_range_key_);
    result.largest_range_key = std::move(largest_range_key_);
    return result;
}

Result<SstBuildResult> build_sst(InternalIterator& source, const SstOptions& options,
                                 bool drop_tombstones,
                                 const std::vector<RangeTombstone>& range_tombstones) {
    SstWriter writer(options);
    for (const RangeTombstone& range : range_tombstones) {
        writer.add_range_tombstone(Slice::from(range.lower), Slice::from(range.upper));
    }
    for (; source.valid(); source.next()) {
        if (drop_tombstones && source.type() == ValueType::Delete) continue;
        writer.add(source.key(), source.type(), source.value());
    }
    if (source.status() != Status::Ok) return std::unexpected(source.status());
    return writer.finish();
}

}  // namespace elysiumkv
