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

    void seek_to_last() override {
        index_.seek_to_last();
        load_current_block(Edge::Last);
        if (data_.valid()) return;
        retreat_block();
    }

    void seek_for_prev(Slice target) override {
        // Index keys are the last key of each block, so the first index entry with key >= target
        // names the only block that can contain target — and if none does, target is past every
        // key in the file and the answer is simply the last entry.
        index_.seek(target);
        if (!index_.valid()) {
            seek_to_last();
            return;
        }
        load_current_block(Edge::First);
        if (data_.valid()) data_.seek_for_prev(target);
        // An empty result here means target precedes this block's first key, so the entry we want
        // is the last one of an earlier block.
        while (!data_.valid() && index_.valid() && status_ == Status::Ok) retreat_block();
    }

    void prev() override {
        if (!data_.valid()) return;
        data_.prev();
        if (!data_.valid()) retreat_block();
    }

    Slice key() const override { return data_.key(); }
    Slice value() const override { return data_.value(); }
    ValueType type() const override { return data_.type(); }

private:
    /// Which end of a freshly loaded block to stand on — the direction of travel decides.
    enum class Edge { First, Last };

    void load_current_block(Edge edge = Edge::First) {
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
        if (edge == Edge::First) {
            data_.seek_to_first();
        } else {
            data_.seek_to_last();
        }
    }

    /// Carries a spent block's failure into the iterator before the block is replaced.
    ///
    /// Without this, a block that failed to decode is indistinguishable from one that merely ran
    /// out: both leave `data_` invalid, and the next load overwrites the status that said which it
    /// was. A corrupt block would then be silently skipped rather than reported.
    void absorb_data_status() {
        if (status_ == Status::Ok && data_.status() != Status::Ok) status_ = data_.status();
    }

    void retreat_block() {
        while (status_ == Status::Ok) {
            absorb_data_status();
            if (status_ != Status::Ok || !index_.valid()) return;
            index_.prev();
            if (!index_.valid()) return;
            load_current_block(Edge::Last);
            if (data_.valid()) return;
        }
    }

    void advance_block() {
        while (status_ == Status::Ok) {
            absorb_data_status();
            if (status_ != Status::Ok) return;
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

    // One read for the whole footer; the trailer is validated out of the same bytes, so a
    // well-formed file costs a single round trip here. Sized to the **widest** footer this build
    // knows rather than to the version it hopes for — the width is only discoverable from the
    // trailer, which is inside these bytes, so reading a v1 width first would cost a second round
    // trip on every v2 file to fetch the twelve bytes it turned out to need.
    const auto tail_len = static_cast<size_t>(
        std::min<uint64_t>(file_size, static_cast<uint64_t>(Footer::kFooterLengthV2)));
    auto tail = store.get(name, file_size - tail_len, tail_len).get();
    if (!tail) return std::unexpected(tail.error());
    if (tail->size() != tail_len) return std::unexpected(Status::Corrupt);

    const Slice tail_slice = Slice::from(*tail);
    auto footer_length = Footer::footer_length_from_trailer(tail_slice);
    if (!footer_length) return std::unexpected(footer_length.error());
    if (static_cast<uint64_t>(*footer_length) > file_size) return std::unexpected(Status::Corrupt);

    auto footer = Footer::decode(tail_slice);
    if (!footer) return std::unexpected(footer.error());

    std::unique_ptr<SstReader> reader(
        new SstReader(store, std::move(name), file_size, options));
    reader->footer_ = *footer;

    auto index = reader->load_block(reader->footer_.index);
    if (!index) return std::unexpected(index.error());
    reader->index_block_ = *index;

    // **The filter is not read here.** Iteration never consults it, and a compaction opens a
    // reader per input, so this was a third round trip and ~1.25 MB per million entries fetched
    // and discarded on every merge. `get` loads it on first use.
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

    auto content = raw->size() == handle.length
                       ? unframe_block(Slice::from(*raw), max_uncompressed())
                       : Result<Buffer>(std::unexpected(Status::Corrupt));
    if (!content) {
        // **A rotted cache file is not a corrupt store.** The bytes may have come from a disk
        // cache whose chunk was truncated or flipped, and the authoritative object is untouched —
        // so reporting `Corrupt` here sends an operator to a restore for a healthy store. The
        // range cache already treats a *missing* entry as costing latency and nothing else;
        // silent corruption went past that door because only this layer runs the checksum.
        //
        // A cache is allowed to be absent and not to be wrong, so every layer is told to forget
        // the object and the read is retried against the authority. If that fails too, the store
        // really is damaged and the original status stands.
        BlobStore& authority = authoritative_store(store_);
        if (&authority != &store_) {
            for (BlobStore* layer = &store_;;) {
                CacheBlobStore* cache = layer->as_cache();
                if (cache == nullptr) break;
                cache->invalidate(name_);
                layer = &cache->delegate();
            }

            auto retried = authority.get(name_, handle.offset, handle.length).get();
            if (retried && retried->size() == handle.length) {
                auto fresh = unframe_block(Slice::from(*retried), max_uncompressed());
                if (fresh) content = std::move(fresh);
            }
        }
        if (!content) return std::unexpected(content.error());
    }

    auto block = std::make_shared<const Block>(std::move(*content));
    if (options_.block_cache != nullptr) {
        options_.block_cache->insert(options_.file_number, handle.offset, block);
    }
    return block;
}

Status SstReader::ensure_filter() {
    std::lock_guard<std::mutex> lock(lazy_mutex_);
    if (filter_loaded_) return Status::Ok;

    auto filter = store_.get(name_, footer_.filter.offset, footer_.filter.length).get();
    if (!filter) return filter.error();
    auto content = unframe_block(Slice::from(*filter), max_uncompressed());
    if (!content) return content.error();

    filter_ = std::move(*content);
    filter_loaded_ = true;
    return Status::Ok;
}

Result<std::optional<SstReader::Found>> SstReader::get(Slice key) {
    // The filter exists to make this rejection cheap; it is consulted before any
    // data block is touched. Loaded on the first `get` rather than at open — see `open`.
    if (Status status = ensure_filter(); status != Status::Ok) return std::unexpected(status);
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

Result<bool> SstReader::range_deletes(Slice key) {
    if (!has_range_tombstones()) return false;
    auto block = load_block(footer_.range_del);
    if (!block) return std::unexpected(block.error());

    // The tombstones are disjoint and sorted by lower bound, so the only candidate is the last one
    // starting at or before `key` — one seek rather than a scan, which is why they are stored in an
    // ordinary block rather than a bespoke list.
    BlockIterator it(*block);
    it.seek_for_prev(key);
    if (!it.valid()) return it.status() == Status::Ok ? Result<bool>{false}
                                                      : std::unexpected(it.status());
    return key < it.value();
}

/// **Decoded once per reader.** This is asked for once per carrying file per iterator
/// construction and once per input per compaction, and it used to fetch the block and walk it
/// every time. The reader is already where the index and the filter live; a file is immutable, so
/// its tombstones are too.
///
/// Still returns a copy, because every caller takes ownership — the merging iterator holds one
/// vector per child and the memtable path has no reader to share from. What this removes is the
/// block fetch and the decode, not the caller's copy.
Result<std::vector<RangeTombstone>> SstReader::range_tombstones() {
    if (!has_range_tombstones()) return std::vector<RangeTombstone>{};

    std::lock_guard<std::mutex> lock(lazy_mutex_);
    if (ranges_loaded_) return ranges_;

    auto block = load_block(footer_.range_del);
    if (!block) return std::unexpected(block.error());

    std::vector<RangeTombstone> out;
    BlockIterator it(*block);
    for (it.seek_to_first(); it.valid(); it.next()) {
        const Slice lower = it.key();
        const Slice upper = it.value();
        out.push_back(RangeTombstone{std::string(lower.data(), lower.data() + lower.size()),
                                     std::string(upper.data(), upper.data() + upper.size())});
    }
    if (it.status() != Status::Ok) return std::unexpected(it.status());

    ranges_ = std::move(out);
    ranges_loaded_ = true;
    return ranges_;
}

std::unique_ptr<InternalIterator> SstReader::iterator() {
    return std::make_unique<SstIterator>(*this, index_block_);
}

}  // namespace elysiumkv
