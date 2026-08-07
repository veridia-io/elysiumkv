#include "sst/sst_reader.hpp"

#include "sst/bloom.hpp"
#include "sst/compression.hpp"
#include "sst/format.hpp"

#include <algorithm>
#include <utility>

namespace elysiumkv {
namespace {

/// Index values are `(offset varint, length varint)`.
bool decode_handle(Slice encoded, BlockHandle& out) {
    const uint8_t* p = encoded.data();
    const uint8_t* const limit = p + encoded.size();
    uint64_t offset = 0;
    uint64_t length = 0;
    if (!get_varint64(p, limit, offset) || !get_varint64(p, limit, length)) return false;
    if (length > std::numeric_limits<uint32_t>::max()) return false;
    out.offset = offset;
    out.length = static_cast<uint32_t>(length);
    return true;
}

/// Two-level iteration: the index block names data blocks, each of which is
/// loaded on demand and held for as long as it is being read.
class SstIterator final : public InternalIterator {
public:
    SstIterator(SstReader& reader, std::shared_ptr<const Block> index_block)
        : reader_(reader), index_(std::move(index_block)) {}

    bool valid() const override { return data_.valid(); }
    Status status() const override {
        if (status_ != Status::Ok) return status_;
        if (index_.status() != Status::Ok) return index_.status();
        return data_.status();
    }

    void seek_to_first() override {
        index_.seek_to_first();
        load_current_block();
        if (data_.valid()) return;
        advance_block();
    }

    void seek(Slice target) override {
        // Index keys are the last key of each block, so the first index entry
        // with key >= target names the only block that can hold it.
        index_.seek(target);
        load_current_block();
        if (data_.valid()) data_.seek(target);
        while (!data_.valid() && index_.valid() && status_ == Status::Ok) advance_block();
    }

    void next() override {
        if (!data_.valid()) return;
        data_.next();
        if (!data_.valid()) advance_block();
    }

    Slice key() const override { return data_.key(); }
    Slice value() const override { return data_.value(); }
    ValueType type() const override { return data_.type(); }

private:
    void load_current_block() {
        data_ = BlockIterator();
        if (!index_.valid()) return;

        BlockHandle handle;
        if (!decode_handle(index_.value(), handle)) {
            status_ = Status::Corrupt;
            return;
        }
        auto block = reader_.load_block(handle);
        if (!block) {
            status_ = block.error();
            return;
        }
        data_ = BlockIterator(*block);
        data_.seek_to_first();
    }

    void advance_block() {
        while (status_ == Status::Ok) {
            if (!index_.valid()) return;
            index_.next();
            if (!index_.valid()) return;
            load_current_block();
            if (data_.valid()) return;
        }
    }

    SstReader& reader_;
    BlockIterator index_;
    BlockIterator data_;
    Status status_ = Status::Ok;
};

}  // namespace

size_t SstReader::max_uncompressed() const {
    // Whichever is larger: sixteen configured blocks, or the largest block a
    // writer is permitted to emit. The second term is what keeps the bound and
    // the write-side limit from drifting apart — they now share one constant.
    return std::max<size_t>(16 * options_.block_bytes, kMaxEntryBlockBytes);
}

Result<std::unique_ptr<SstReader>> SstReader::open(BlobStore& store, std::string name,
                                                   uint64_t file_size, SstReaderOptions options) {
    if (file_size < static_cast<uint64_t>(Footer::kFooterLengthV1)) {
        return std::unexpected(Status::Corrupt);
    }

    // One read for the whole footer; the trailer is validated out of the same
    // bytes, so a well-formed file costs a single round trip here.
    const auto tail_len = static_cast<size_t>(Footer::kFooterLengthV1);
    auto tail = store.get(name, file_size - tail_len, tail_len).get();
    if (!tail) return std::unexpected(tail.error());
    if (tail->size() != tail_len) return std::unexpected(Status::Corrupt);

    const Slice tail_slice = Slice::from(*tail);
    auto footer_length = Footer::footer_length_from_trailer(tail_slice);
    if (!footer_length) return std::unexpected(footer_length.error());
    if (*footer_length != Footer::kFooterLengthV1) return std::unexpected(Status::Corrupt);

    auto footer = Footer::decode(tail_slice);
    if (!footer) return std::unexpected(footer.error());

    std::unique_ptr<SstReader> reader(
        new SstReader(store, std::move(name), file_size, options));
    reader->footer_ = *footer;

    auto index = reader->load_block(reader->footer_.index);
    if (!index) return std::unexpected(index.error());
    reader->index_block_ = *index;

    auto filter = store.get(reader->name_, reader->footer_.filter.offset,
                            reader->footer_.filter.length)
                      .get();
    if (!filter) return std::unexpected(filter.error());
    auto filter_content = unframe_block(Slice::from(*filter), reader->max_uncompressed());
    if (!filter_content) return std::unexpected(filter_content.error());
    reader->filter_ = std::move(*filter_content);

    return reader;
}

Result<std::shared_ptr<const Block>> SstReader::load_block(const BlockHandle& handle) {
    if (options_.block_cache != nullptr) {
        if (auto cached = options_.block_cache->get(options_.file_number, handle.offset)) {
            return cached;
        }
    }
    if (handle.length < kBlockTrailerLength || handle.offset + handle.length > file_size_) {
        return std::unexpected(Status::Corrupt);
    }

    auto raw = store_.get(name_, handle.offset, handle.length).get();
    if (!raw) return std::unexpected(raw.error());
    if (raw->size() != handle.length) return std::unexpected(Status::Corrupt);

    auto content = unframe_block(Slice::from(*raw), max_uncompressed());
    if (!content) return std::unexpected(content.error());

    auto block = std::make_shared<const Block>(std::move(*content));
    if (options_.block_cache != nullptr) {
        options_.block_cache->insert(options_.file_number, handle.offset, block);
    }
    return block;
}

Result<std::optional<SstReader::Found>> SstReader::get(Slice key) {
    // The filter exists to make this rejection cheap; it is consulted before any
    // data block is touched.
    if (!bloom_may_contain(Slice::from(filter_), key)) return std::optional<Found>{};

    BlockIterator index(index_block_);
    index.seek(key);
    if (!index.valid()) {
        if (index.status() != Status::Ok) return std::unexpected(index.status());
        return std::optional<Found>{};  // beyond the last block's last key
    }

    BlockHandle handle;
    if (!decode_handle(index.value(), handle)) return std::unexpected(Status::Corrupt);

    auto block = load_block(handle);
    if (!block) {
        // **A block the index says is here cannot be "absent".** The store reporting `NotFound` for
        // it means the object went away underneath this reader, and returning that verbatim would
        // hand the caller the same status a missing *key* produces — collapsing an I/O failure into
        // absence, which is the one confusion `Absence is an answer, not an error` exists to
        // prevent. A caller told "no such key" writes a replacement for data that still exists.
        return std::unexpected(block.error() == Status::NotFound ? Status::Corrupt : block.error());
    }

    BlockIterator entries(*block);
    entries.seek(key);
    if (!entries.valid()) {
        if (entries.status() != Status::Ok) return std::unexpected(entries.status());
        return std::optional<Found>{};
    }
    if (entries.key() != key) return std::optional<Found>{};

    return std::optional<Found>(Found{*block, entries.value(), entries.type()});
}

std::unique_ptr<InternalIterator> SstReader::iterator() {
    return std::make_unique<SstIterator>(*this, index_block_);
}

}  // namespace elysiumkv
