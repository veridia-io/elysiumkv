#include "db/db_impl.hpp"

#include "sst/format.hpp"

#include "compact/merging_iterator.hpp"
#include "crypt/encrypted_object.hpp"
#include "elysiumkv/no_encryption_provider.hpp"
#include "sst/concat_iterator.hpp"
#include "util/budget_charge.hpp"
#include "util/jitter.hpp"
#include "compact/picker.hpp"
#include "sst/sst_writer.hpp"

#include <algorithm>
#include <limits>
#include <ranges>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <unordered_set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace elysiumkv {
namespace {

/// ARCHITECTURE.md "The manifest is snapshots plus edits" — `{file_number:012d}.sst`, from one monotonic counter, so names are
/// globally unique and no collision arises across stores.
constexpr std::string_view kSstSuffix = ".sst";

class DbIterator final : public Iterator {
public:
    DbIterator(std::unique_ptr<InternalIterator> merged,
               std::shared_ptr<const Version> version,
               std::vector<std::shared_ptr<SkiplistMemtable>> memtables,
               std::vector<std::shared_ptr<SstReader>> readers, std::string lower,
               std::string upper, bool has_upper, bool reverse)
        : merged_(std::move(merged)),
          version_(std::move(version)),
          memtables_(std::move(memtables)),
          readers_(std::move(readers)),
          lower_(std::move(lower)),
          upper_(std::move(upper)),
          has_upper_(has_upper),
          reverse_(reverse) {}

    bool next() override {
        if (!started_) {
            started_ = true;
            start();
        } else if (merged_->valid()) {
            step();
        }

        // The merge yields one entry per key, newest first; a tombstone here
        // means the key is deleted, so the public iterator skips it entirely.
        while (merged_->valid()) {
            if (out_of_range()) {
                // Leaving the range ends the scan — a prefix iterator must never
                // degrade into a full keyspace scan (ARCHITECTURE.md "Absence is an answer, not an error").
                return false;
            }
            if (merged_->type() == ValueType::Put) return true;
            step();
        }
        return false;
    }

    Slice key() const override { return merged_->key(); }
    Slice value() const override { return merged_->value(); }
    Status status() const override { return merged_->status(); }

private:
    void start() {
        if (!reverse_) {
            if (lower_.empty()) {
                merged_->seek_to_first();
            } else {
                merged_->seek(Slice::from(lower_));
            }
            return;
        }
        if (!has_upper_) {
            merged_->seek_to_last();
            return;
        }
        // The upper bound is exclusive, and seek_for_prev is inclusive, so a key landing exactly on
        // it is one step too far. Stepping back is correct rather than seeking to a synthesised
        // predecessor of `upper_`, which is unrepresentable for arbitrary byte strings.
        merged_->seek_for_prev(Slice::from(upper_));
        if (merged_->valid() && !(merged_->key() < Slice::from(upper_))) merged_->prev();
    }

    void step() {
        if (reverse_) {
            merged_->prev();
        } else {
            merged_->next();
        }
    }

    /// Whether the current entry has left the requested range, which ends the scan.
    ///
    /// Descending, the bound that stops the scan is the lower one — and an empty `lower_` needs no
    /// special case, since no key sorts below the empty string.
    bool out_of_range() const {
        if (reverse_) return merged_->key() < Slice::from(lower_);
        return has_upper_ && !(merged_->key() < Slice::from(upper_));
    }

    std::unique_ptr<InternalIterator> merged_;
    /// Pinned for the iterator's lifetime; releasing it is what finally allows a
    /// compacted-away file to be unlinked (ARCHITECTURE.md "Versions are immutable snapshots").
    std::shared_ptr<const Version> version_;
    std::vector<std::shared_ptr<SkiplistMemtable>> memtables_;
    std::vector<std::shared_ptr<SstReader>> readers_;
    std::string lower_;
    std::string upper_;
    bool has_upper_ = false;
    bool reverse_ = false;
    bool started_ = false;
};

/// An iterator that yields nothing but carries a status — used when a source
/// cannot be opened, so the failure surfaces from `status()` rather than as an
/// empty-but-successful scan.
class ErrorIterator final : public InternalIterator {
public:
    explicit ErrorIterator(Status status) : status_(status) {}
    bool valid() const override { return false; }
    void seek_to_first() override {}
    void seek(Slice) override {}
    void next() override {}
    void seek_to_last() override {}
    void seek_for_prev(Slice) override {}
    void prev() override {}
    Slice key() const override { return {}; }
    Slice value() const override { return {}; }
    ValueType type() const override { return ValueType::Put; }
    Status status() const override { return status_; }

private:
    Status status_;
};

bool file_may_contain(const FileMetadata& file, Slice key) {
    return !(key < Slice::from(file.smallest_key)) && !(Slice::from(file.largest_key) < key);
}

}  // namespace

std::string sst_object_name(uint64_t file_number) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%012llu%s", static_cast<unsigned long long>(file_number),
                  std::string(kSstSuffix).c_str());
    return buf;
}

/// The inverse of `sst_object_name`, or nullopt for anything this engine did not write.
/// Strict on purpose: a store may hold objects that are none of our business, and guessing a
/// number out of one of them would move the file-number counter for no reason.
std::optional<uint64_t> sst_file_number(std::string_view name) {
    if (!name.ends_with(kSstSuffix)) return std::nullopt;
    const std::string_view digits = name.substr(0, name.size() - kSstSuffix.size());
    if (digits.size() != 12) return std::nullopt;

    uint64_t number = 0;
    for (const char c : digits) {
        if (c < '0' || c > '9') return std::nullopt;
        number = number * 10 + static_cast<uint64_t>(c - '0');
    }
    return number;
}

uint64_t default_clock() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

WriteBatch& WriteBatch::put(Slice key, Slice value) {
    ops_.push_back({Kind::Put, key.to_string(), value.to_string()});
    return *this;
}

WriteBatch& WriteBatch::remove(Slice key) {
    ops_.push_back({Kind::Remove, key.to_string(), {}});
    return *this;
}

WriteBatch& WriteBatch::delete_range(Slice lower, Slice upper) {
    ops_.push_back({Kind::DeleteRange, lower.to_string(), upper.to_string()});
    return *this;
}

DbImpl::DbImpl(const Options& options, ResolvedLevels config, ResolvedTiers tiers)
    : options_(options),
      config_(std::move(config)),
      tiers_(std::move(tiers)),
      readers_(options.reader_cache_bytes, options.memory_budget.get()) {
    if (!options_.clock) options_.clock = &default_clock;
    block_cache_ = options_.block_cache;
    if (block_cache_ == nullptr) {
        auto cache = std::make_shared<ShardedLruBlockCache>(
            8ull << 20, options_.memory_budget.get());
        block_cache_ = cache;
    }

    mem_ = new_memtable();
}

DbImpl::~DbImpl() {
    // Before anything is torn down, because `flush()` waits on the flush executor and returns
    // early once `shutting_down_` is set — a flush attempted after the lines below would quietly do
    // nothing. Best-effort by construction: there is no write-ahead log, so a memtable dropped on a
    // clean shutdown is lost for no reason, but a destructor has nowhere to report a failure to and
    // so promises nothing. See `DB::abandon_unflushed`.
    if (!read_only_ && !abandoned_.load(std::memory_order_relaxed)) {
        static_cast<void>(flush());
    }
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        shutting_down_ = true;
    }
    {
        std::lock_guard<std::mutex> lock(compaction_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        shutting_down_compaction_ = true;
    }
    // The coordinator goes first: it is the only thing that hands the executors new work, so
    // stopping it before joining them keeps shutdown from racing a dispatch.
    {
        std::lock_guard<std::mutex> lock(maintenance_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        shutting_down_maintenance_ = true;
    }
    maintenance_tick_.notify_all();
    if (maintenance_thread_.joinable()) maintenance_thread_.join();
    flush_scheduled_.notify_all();
    flush_finished_.notify_all();
    compaction_scheduled_.notify_all();
    compaction_finished_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
    if (compaction_thread_.joinable()) compaction_thread_.join();
}

const ResolvedLevel& DbImpl::level_config(int level) const {
    const int clamped = std::clamp(level, 0, config_.last());
    return config_.levels[static_cast<size_t>(clamped)];
}

void DbImpl::note_generation_roll() const {
    if (!logger_enabled(LogLevel::Info)) return;
    const uint64_t current = versions_->generation();
    uint64_t seen = last_seen_generation_.load(std::memory_order_relaxed);
    if (current == seen) return;
    if (!last_seen_generation_.compare_exchange_strong(seen, current)) return;  // already reported
    // Zero is "have not looked yet", not a roll from generation zero.
    if (seen == 0) return;
    log_event(LogLevel::Info, LogEvent::GenerationRolled, "manifest generation ", seen, " -> ",
              current, "; the previous generation's edits are collapsed into a snapshot");
}

void DbImpl::log_emit(LogLevel level, LogEvent event, const std::string& message) const {
    // Not a style rule: an appender that blocks under `mem_mutex_` stalls every writer, and a sink
    // that reads the store it is logging about deadlocks.
    assert(locks_held() == 0 && "the logger sink must not run under an engine lock");
    const Logger& sink = *options_.logger;
    sink.write(sink.context, level, event, message.data(), message.size());
}

Result<OpenResult> DbImpl::open(const Options& options, bool require_all_durable,
                                bool read_only) {
    // Every rejection below says which one it was. A dozen distinct configuration mistakes all
    // report `Status::Config`, and an operator holding only that has to guess; the message is the
    // whole difference between a five-minute fix and a bisect through the options struct.
    const auto refuse = [](std::string_view why) {
        internal::set_last_error(why);
        return std::unexpected(Status::Config);
    };
    internal::set_last_error("");

    auto config = resolve_levels(options.levels);
    if (!config) {
        return refuse("levels are not a usable configuration: they must start at 0 and be "
                      "contiguous, and each must be within its bounds");
    }
    auto tiers = resolve_tiers(options.tiers, options.age_jitter);
    if (!tiers) {
        return refuse("tiers are not a usable configuration: at least one is required, each needs "
                      "a store, and a cache may not be a tier's innermost store");
    }
    if (!(options.flush_interval_jitter >= 0.0) || options.flush_interval_jitter > 1.0) {
        return refuse("flush_interval_jitter must be between 0.0 and 1.0");
    }
    if (options.manifest_catalog == nullptr) return refuse("no manifest_catalog was given");
    // Zero would make every block its own request, which is the shape the window exists to remove,
    // and it reads as "unset" rather than as a choice.
    //
    // The upper bound is arithmetic and nothing more: the budget charges two windows, so anything
    // past half the address space wraps. Deliberately not bounded by `max_compaction_bytes` — a
    // store with a small compaction budget is a real configuration, and a window wider than the
    // whole compaction simply reads each input in one request, which is the best case rather than
    // an error. Bounding it that way rejected `TinyCompactionBudget` outright.
    if (options.compaction_window_bytes == 0 ||
        options.compaction_window_bytes > std::numeric_limits<size_t>::max() / 2) {
        return refuse("compaction_window_bytes must be non-zero and below half the address space");
    }

    // ARCHITECTURE.md "A tier is not a level" — `open` is guarded rather than merely documented. Adding a Transient
    // tier later must not leave existing call sites compiling and silently
    // serving stale values.
    if (require_all_durable && tiers->any_transient()) {
        return refuse("a Transient tier is configured, so open_with_result() must be used instead "
                      "of open(): a transient store can lose data and the caller has to see that");
    }

    // A `Transient` tier needs at least two levels. An L0 file cannot be migrated — that
    // would reorder L0's positional recency — so it leaves its tier by being compacted into L1,
    // and `compact_l0_file_off_its_tier` gives up when there is no L1 to compact into. With one
    // level that is permanent exposure: L0 files can never leave the transient tier, no timer
    // helps, and the stall valve eventually holds every write — a store that is neither durable
    // nor writable. Rejected here, so a silent livelock is a configuration error instead.
    if (tiers->any_transient() && config->last() < 1) {
        return refuse("a Transient tier needs at least two levels: an L0 file leaves its tier only "
                      "by being compacted into L1, so with one level it can never leave");
    }

    // The orphan window must be at least the reader window. An obsolete object is, to the
    // sweep, indistinguishable from an orphan — the edit that removed it is committed, so the
    // current manifest does not reference it, which is the sweep's own test. The pending queue keeps
    // the sweep off objects this instance obsoleted, but a crash empties that queue: an object
    // obsoleted before the crash comes back as an orphan afterwards, protected by the orphan window
    // and nothing else. Ordered the other way, the reader window would be silently inert after any
    // restart. Checked rather than documented, like every other bound here.
    if (options.obsolete_retention.has_value() &&
        options.orphan_retention < *options.obsolete_retention) {
        return refuse("orphan_retention must be at least obsolete_retention: a crash turns "
                      "superseded objects into orphans, protected by the orphan window alone");
    }

    std::unique_ptr<DbImpl> db(new DbImpl(options, std::move(*config), std::move(*tiers)));
    db->read_only_ = read_only;
    if (Status status = db->recover(); status != Status::Ok) {
        // The instance is about to be destroyed and its message with it, so it moves to the
        // thread's slot first. This is the path that carries "which encryption provider is
        // missing" and every other recovery failure worth naming.
        internal::set_last_error(db->last_error());
        return std::unexpected(status);
    }
    db->start_background();

    OpenResult result;
    result.discarded_stores = db->discarded_stores_;
    result.discarded_files = db->discarded_files_;
    result.requires_recovery = db->requires_recovery_.load();
    result.db = std::move(db);
    return result;
}

const Tier& DbImpl::tier_for(uint64_t file_number, uint64_t min_write_time_ms) const {
    const int index = placement(tiers_, file_number, min_write_time_ms, options_.clock());
    return tiers_.tiers[static_cast<size_t>(index)];
}

BlobStore* DbImpl::store_for(const std::string& store_id) const {
    auto it = tiers_.stores.find(store_id);
    return it == tiers_.stores.end() ? nullptr : it->second.get();
}

Result<std::unique_ptr<DB>> DB::open(const Options& options) {
    auto result = DbImpl::open(options, /*require_all_durable=*/true);
    if (!result) return std::unexpected(result.error());
    return std::move(result->db);
}

Result<OpenResult> DB::open_with_result(const Options& options) {
    return DbImpl::open(options, /*require_all_durable=*/false);
}

Result<std::unique_ptr<ReadOnlyDB>> DB::open_read_only(const Options& options) {
    // A reader has no authority to delete anything, so reclamation is forced off here rather than
    // trusted to the caller's options — the flag is about what the *writer* may do.
    Options read_options = options;
    read_options.orphan_sweep_interval.reset();

    auto result = DbImpl::open(read_options, /*require_all_durable=*/false, /*read_only=*/true);
    if (!result) return std::unexpected(result.error());
    return std::unique_ptr<ReadOnlyDB>(std::move(result->db));
}

Status DbImpl::recover() {
    // Resolved before the manifest is touched, because the manifest is itself encrypted: the
    // first thing `VersionSet::recover` does is open a payload, and it needs the registry to do it.
    // The passthrough is added rather than configured, so there is always a provider and never a
    // null one; an embedder registering its reserved id is refused rather than silently overriding
    // the one case the engine owns.
    encryption_.providers = options_.encryption.providers;
    if (encryption_.providers.contains(std::string(kNoEncryptionProviderId))) {
        last_error_ = "the empty encryption provider id is reserved for the passthrough";
        return Status::Config;
    }
    for (const auto& [id, provider] : encryption_.providers) {
        if (provider == nullptr) {
            last_error_ = "encryption provider '" + id + "' is null";
            return Status::Config;
        }
    }
    encryption_.providers.emplace(std::string(kNoEncryptionProviderId),
                                  std::make_shared<NoEncryptionProvider>());

    encryption_.primary = options_.encryption.primary_provider;
    encryption_.accept_plaintext = options_.encryption.accept_plaintext;
    if (!encryption_.providers.contains(encryption_.primary)) {
        last_error_ = "primary encryption provider '" + encryption_.primary + "' is not registered";
        return Status::Config;
    }

    versions_ = std::make_unique<VersionSet>(
        *options_.manifest_catalog, options_.manifest_edits_per_generation,
        [this](const std::vector<FileMetadata>& files) { return delete_obsolete(files); },
        encryption_,
        [this] { return now_ms(); },
        options_.obsolete_retention.value_or(Duration(0)));

    published_level_counts_ = std::ranges::all_of(config_.levels, [](const ResolvedLevel& level) {
        return level.level < VersionSet::kPublishedLevels;
    });

    const Status status = versions_->recover();
    if (status != Status::Ok && !versions_->last_error().empty()) {
        last_error_ = versions_->last_error();
    }
    if (status == Status::NotFound && read_only_) {
        // A reader does not create a store. Finding no manifest means it opened the wrong place
        // or arrived before the writer; either way that is not a reader's decision to make, and
        // creating one would be the manifest write this mode exists to avoid.
        last_error_ = "read-only open found no manifest: the store does not exist yet";
        return Status::NotFound;
    }
    if (status == Status::NotFound) {
        // Fresh store — no pointer. The directory may still hold objects, from a previous
        // store whose manifest is gone or a wiped catalog, and a counter starting at 1 would
        // collide with the first of them.
        if (Status created = versions_->create(); created != Status::Ok) return created;
        for (const ListResult& names : list_all_stores()) {
            if (!names) continue;  // unreadable here is open's problem, not this step's
            for (const std::string& name : *names) {
                if (auto number = sst_file_number(name)) versions_->observe_file_number(*number);
            }
        }
        return Status::Ok;
    }
    if (status != Status::Ok) return status;

    if (Status verified = verify_stores_and_discard(); verified != Status::Ok) return verified;
    adopt_recovered_watermark();
    // No lock: nothing else can reach this instance until `open` returns.
    write_floor_ = versions_->current()->truncation_point();
    return Status::Ok;
}

/// Turns what survived recovery into the position the embedder should resume after, and seeds the
/// live memtable's lower bound with it.
///
/// No lock: nothing else can reach this instance until `open` returns.
void DbImpl::adopt_recovered_watermark() {
    // The discarded set was folded in as it was dropped — those files are in no version now. What
    // is left is the surviving set, and it is gathered unconditionally so that `resume_after()` is
    // a pure function of one value rather than a decision spread across two branches.
    for (const FileMetadata& file : versions_->current()->all_files()) {
        recovery_watermark_.observe_survivor(file.watermark);
    }
    recovered_watermark_ = recovery_watermark_.resume_after();

    // A loss recorded by an earlier recovery still binds this one. `resume_after` reasons from
    // the files present now, and with nothing discarded it trusts `max(high)` over them — an
    // argument whose premise is that no state was ever lost. The discard that falsified it also
    // erased the evidence, so the floor is what carries it forward. See `Version::watermark_floor`.
    if (auto floor = versions_->current()->watermark_floor()) {
        if (!floor->position.has_value()) {
            recovered_watermark_ = std::nullopt;
        } else if (recovered_watermark_.has_value()) {
            recovered_watermark_ = std::min(*recovered_watermark_, *floor->position);
        } else {
            recovered_watermark_ = floor->position;
        }
    }

    // The recovered position is an established watermark: the embedder resumes strictly after it,
    // so every write this run accepts is at a later position. Seeding the live memtable with it
    // is what stops each restart from minting a file with no lower bound — a file that would
    // spread an absent `low` through every compaction lineage it joined. It also makes the
    // non-decreasing check span restarts.
    established_watermark_ = recovered_watermark_;
    if (established_watermark_.has_value() && mem_ != nullptr) {
        mem_->set_watermark_bounds({*established_watermark_, *established_watermark_});
    }
}

void DbImpl::start_background() {
    // A reader runs no background work at all: no flush, no compaction, no migration, no
    // collection, and no sweep. Every one of those either writes the manifest or deletes an object,
    // and a reader has authority to do neither.
    if (read_only_) return;
    if (inline_mode()) {
        // The op stream decides when work happens; do the compaction the
        // recovered version may already deserve before returning.
        (void)compact_until_quiet();
        return;
    }
    flush_thread_ = std::thread([this] { background_flush_loop(); });
    compaction_thread_ = std::thread([this] { background_compaction_loop(); });

    // One synchronous reconcile before the first write can arrive. A store reopened already
    // past a transient tier's `stall_age` must hold writes immediately; if the published flag
    // started false it would accept them for nearly a full interval. Forced, because there is no
    // previous reconcile for the gate to compare against.
    reconcile(/*force_full=*/true);
    maintenance_thread_ = std::thread([this] { maintenance_loop(); });
}

// --- write path ---------------------------------------------------------------

namespace {

/// ARCHITECTURE.md "Inside an SST" — refused at the door, because there is nowhere later that can refuse it
/// safely. Rejecting at flush would leave a memtable holding an entry no SST can
/// represent, so the flush would fail forever and the memtable could never drain.
Status check_entry_size(Slice key, Slice value) {
    if (key.size() > kMaxKeyBytes) return Status::Config;
    if (value.size() > kMaxValueBytes) return Status::Config;
    return Status::Ok;
}

}  // namespace

Status DbImpl::put(Slice key, Slice value) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    if (Status status = check_entry_size(key, value); status != Status::Ok) return status;
    if (Status status = throttle_writes(); status != Status::Ok) return status;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        // Checked here rather than on the way in, so that the floor cannot advance between the
        // check and the write — see `write_floor_`.
        if (Status status = check_below_truncation(key); status != Status::Ok) return status;
        mem_->put(key, value);
    }
    return maybe_freeze_memtable(false);
}

Status DbImpl::delete_range(Slice lower, Slice upper) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    if (Status status = check_entry_size(lower, Slice()); status != Status::Ok) return status;
    if (Status status = check_entry_size(upper, Slice()); status != Status::Ok) return status;

    // Clamped rather than refused, unlike a `put`. A write below the floor is refused because the
    // engine could not tell it from one made before the truncation; a *delete* below the floor asks
    // for something already true, so the honest answer is to narrow the range to the part that
    // still exists and get on with it.
    //
    // The version is held in a named variable for the length of the check. `current()` hands back
    // the `shared_ptr` by value, so `current()->truncation_point()` yields a reference into a
    // Version whose last owner dies at the semicolon — and a compaction installing a new version at
    // that moment frees the string this is still reading. Binding a reference to a member of a
    // temporary's pointee extends nothing.
    auto version = versions_->current();
    std::string clamped;
    const std::string& floor = version->truncation_point();
    if (!floor.empty() && lower < Slice::from(floor)) {
        clamped = floor;
        lower = Slice::from(clamped);
    }
    if (!(lower < upper)) return Status::Ok;   // deletes nothing, like an iterator over these bounds

    if (Status status = throttle_writes(); status != Status::Ok) return status;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        mem_->delete_range(lower, upper);
    }
    return maybe_freeze_memtable(false);
}

Status DbImpl::remove(Slice key) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    if (Status status = check_entry_size(key, Slice()); status != Status::Ok) return status;
    if (Status status = throttle_writes(); status != Status::Ok) return status;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        if (Status status = check_below_truncation(key); status != Status::Ok) return status;
        mem_->remove(key);
    }
    return maybe_freeze_memtable(false);
}

/// The floor is permanent, so a write below it is refused rather than accepted and hidden.
///
/// This is the whole of what makes truncation cheap. Keys below the point are unreadable
/// because the *point* says so, not because anything was written per key — so the engine has no
/// way to tell a key written before the truncation from one written after, positional recency
/// being the only ordering it has. Accepting such a write would mean a `put` that returned `Ok`
/// and then could not be read back, which is the one outcome worth ruling out by construction.
/// A caller that wants to delete a range and keep writing into it wants `delete_range`, not this.
Status DbImpl::check_below_truncation(Slice key) const {
    return !write_floor_.empty() && key < Slice::from(write_floor_) ? Status::Config : Status::Ok;
}

Status DbImpl::truncate_below(Slice key) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    if (key.empty()) return Status::Ok;      // nothing sorts below the empty key

    // Published before the edit, under the memtable lock. That is what makes the refusal
    // airtight: from here on every writer sees the new floor, and any write already in the
    // memtable happened before the truncation and is hidden by the read clamp, which is what a
    // truncation means. Holding the lock across the manifest write instead would stall every
    // writer on an object-store round trip.
    //
    // Monotone, and the check is here rather than only in `Version::apply` so that a repeated call
    // costs no manifest write at all. Idempotence is the property that makes this safe to drive
    // from a loop that does not track what it already asked for.
    std::string previous_floor;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (!(Slice::from(write_floor_) < key)) return Status::Ok;
        previous_floor = write_floor_;
        write_floor_ = key.to_string();
    }

    // The memtables are deliberately left alone. The read clamp hides keys below the point
    // wherever they live, memtable included, so touching them would change no answer — and it is
    // not even the optimisation it looks like: the skiplist cannot unlink a node, so `remove` adds
    // a tombstone record. Truncating a memtable would therefore make it *larger*, turn its puts
    // into tombstones, and still leave a file to flush. The whole-file reclaim collects that file
    // afterwards for a fraction of the cost.
    VersionEdit edit;
    edit.truncation_point = key.to_string();
    if (Status status = versions_->apply(std::move(edit)); status != Status::Ok) {
        // The floor was published on the strength of an edit that did not land, so it goes back —
        // unless another call has since moved it further, in which case that one owns it now and
        // its own failure path will do the same.
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (write_floor_ == key.to_string()) write_floor_ = previous_floor;
        return status;
    }

    // Whole files below the point are now unreadable and can go without a rewrite. Left to the
    // maintenance loop rather than done here: it already owns file removal and its retention
    // windows, and doing it inline would put an object-store round trip on the caller's thread.
    invalidate_maintenance();
    return Status::Ok;
}

Result<bool> DbImpl::range_is_erased(Slice lower, Slice upper) const {
    // The truncation point is deliberately not consulted. A band below the floor is
    // unreadable, and unreadable is not erased — the objects are still there until the reclaim
    // collects them. A receipt that counted "you cannot read it" as "it is gone" would be the one
    // kind of wrong answer this is for.
    auto version = versions_->current();
    return !version->any_file_holds(lower, upper);
}

Status DbImpl::write(WriteBatch& batch) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    // Checked in full before anything is applied: ARCHITECTURE.md "Absence is an answer, not an error" says a batch lands as a
    // unit, so discovering an oversized entry halfway through would leave the
    // store holding half of it.
    //
    // The floor is read once for the whole batch, so every range is clamped against the same one
    // and the refusal uses it too. A batch is a unit; a floor moving underneath it would make it a
    // sequence.
    // Each op's resolved range, or nothing for an op that is not one or deletes nothing.
    //
    // One entry per op rather than one per range, indexed by the loop variable below: a vector of
    // ranges alone would need a cursor advancing on a different condition than the loop, and the
    // first empty range would shift every later one onto the wrong bounds.
    std::vector<std::optional<std::pair<std::string, std::string>>> ranges(batch.ops().size());
    if (Status status = throttle_writes(); status != Status::Ok) return status;

    // Validation and application share one critical section, so the floor they see is the same
    // one and nothing lands before a later op is found to be invalid. Everything in here is
    // comparisons and memtable inserts; there is no I/O under the lock.
    //
    // Scoped, because `maybe_freeze_memtable` below takes the same mutex.
    {
    std::lock_guard<std::mutex> lock(mem_mutex_);
    ELYSIUMKV_LOCK_AUDIT();
    if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
    const std::string floor = write_floor_;

    for (size_t i = 0; i < batch.ops().size(); ++i) {
        const WriteBatch::Op& op = batch.ops()[i];
        if (op.kind == WriteBatch::Kind::DeleteRange) {
            if (Status status = check_entry_size(Slice::from(op.key), Slice()); status != Status::Ok) {
                return status;
            }
            if (Status status = check_entry_size(Slice::from(op.value), Slice());
                status != Status::Ok) {
                return status;
            }
            // Clamped, where a `put` in the same batch would be refused, and the difference is
            // not an inconsistency. A write below the floor is refused because the engine cannot
            // tell it from one made before the truncation; a delete below the floor asks for
            // something already true. Doing this per operation rather than per batch is what lets
            // one batch hold both.
            std::string lower = op.key;
            if (!floor.empty() && lower < floor) lower = floor;
            // Empty after clamping deletes nothing, and the entry simply stays absent.
            if (lower < op.value) ranges[i] = std::make_pair(std::move(lower), op.value);
            continue;
        }
        const Status status = check_entry_size(
            Slice::from(op.key), op.kind == WriteBatch::Kind::Remove ? Slice()
                                                                     : Slice::from(op.value));
        if (status != Status::Ok) return status;
        // Checked in the same pass and for the same reason: a batch lands whole or not at all, so
        // a key under the floor has to be found before anything is applied.
        if (Status below = check_below_truncation(Slice::from(op.key)); below != Status::Ok) {
            return below;
        }
    }
    {
        // Applied as a unit: the freeze decision is taken at batch boundaries
        // only, so a flush never splits a batch across two memtables.
        for (size_t i = 0; i < batch.ops().size(); ++i) {
            const WriteBatch::Op& op = batch.ops()[i];
            switch (op.kind) {
                case WriteBatch::Kind::Put:
                    mem_->put(Slice::from(op.key), Slice::from(op.value));
                    break;
                case WriteBatch::Kind::Remove:
                    mem_->remove(Slice::from(op.key));
                    break;
                case WriteBatch::Kind::DeleteRange: {
                    // In op order, so a `put` earlier in the batch is covered by this and a `put`
                    // later lands on top of it — the memtable resolves that as each op is applied,
                    // exactly as it does for the standalone call.
                    if (ranges[i].has_value()) {
                        mem_->delete_range(Slice::from(ranges[i]->first),
                                           Slice::from(ranges[i]->second));
                    }
                    break;
                }
            }
        }
    }
    }
    return maybe_freeze_memtable(false);
}

bool DbImpl::run_one_flush(Status& status) {
    std::shared_ptr<SkiplistMemtable> pending;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (imm_ == nullptr) return false;
        pending = imm_;
    }

    const uint64_t pending_bytes = pending->approximate_bytes();
    const uint64_t pending_entries = pending->num_entries();
    status = flush_memtable(pending);

    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (status == Status::Ok) {
            imm_.reset();
            flushes_.fetch_add(1, std::memory_order_relaxed);
        } else if (bg_error_ == Status::Ok || is_retryable(bg_error_)) {
            // The frozen memtable stays put: dropping it would lose writes the
            // caller was told nothing about.
            //
            // A terminal error is never overwritten. Everything that happens after
            // one is a consequence of it, and the consequence is the less useful of
            // the two things to report.
            bg_error_ = status;
        }
    }
    if (status == Status::Ok) {
        log_event(LogLevel::Info, LogEvent::FlushComplete, "flush wrote ", pending_entries,
                  " entries, ", pending_bytes, " bytes");
        // Flushes are the most frequent edit, so a roll is noticed within one of them wherever it
        // actually happened — compaction included.
        note_generation_roll();
    } else {
        background_failures_.fetch_add(1, std::memory_order_relaxed);
        log_event(LogLevel::Warn, LogEvent::BackgroundFailure, "flush failed: ",
                  status_name(status),
                  is_retryable(status) ? ", will retry" : ", terminal");
    }
    flush_finished_.notify_all();
    return status == Status::Ok;
}

bool DbImpl::memtable_flush_due(bool force) const {
    if (force) return true;
    if (mem_ == nullptr) return false;
    if (mem_->approximate_bytes() >= options_.memtable_bytes) return true;

    // Age is the second, independent trigger. An empty memtable is not "old" — there is
    // nothing in it whose durability could be at risk, and flushing it would write an
    // empty file on every interval of an idle store.
    if (!options_.flush_interval.has_value()) return false;
    if (mem_->empty()) return false;

    const uint64_t now = now_ms();
    const uint64_t born = mem_->creation_time_ms();
    if (now <= born) return false;   // a clock that went backwards is not evidence of age
    return now - born >= flush_interval_for(born);
}

uint64_t DbImpl::flush_interval_for(uint64_t created_ms) const {
    const uint64_t interval = static_cast<uint64_t>(options_.flush_interval->count());
    const uint64_t window = jitter_window_ms(interval, options_.flush_interval_jitter);
    if (window == 0) return interval;
    // Symmetric, so the span is 2 × window wide and the offset is subtracted back off its middle.
    // Seeded on the creation time alone: a memtable has no number, and it never outlives the
    // process, so there is nothing for a reopen to re-cluster.
    return interval - window + jitter_offset(created_ms, 0, 2 * window + 1);
}

Status DbImpl::freeze_and_flush_inline(bool force) {
    Status retried_from = Status::Ok;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        if (imm_ == nullptr) {
            if (!memtable_flush_due(force)) return Status::Ok;
            if (mem_->empty()) return Status::Ok;
            seal_memtable();
        }
        // A frozen memtable left over from a failed flush is retried here: Io
        // means "ask again later", and inline mode has nowhere else to ask.
        if (is_retryable(bg_error_)) {
            retried_from = bg_error_;
            bg_error_ = Status::Ok;
        }
    }
    if (retried_from != Status::Ok) {
        log_event(LogLevel::Warn, LogEvent::BackgroundRetry,
                  "retrying a flush that failed with ", status_name(retried_from));
    }

    Status status = Status::Ok;
    while (run_one_flush(status)) {
    }
    if (status != Status::Ok) return status;

    // Inline mode: the writer is also the compactor, so the flush is not done
    // until the compaction it just caused is done too.
    return compact_until_quiet();
}

void DbImpl::seal_memtable() {
    // Stamped at the freeze rather than at the flush, because this is the moment writes stop
    // arriving — a flush that queues behind another would otherwise record a newest-write time
    // later than any write in the file, and delay its expiry by however long the queue was.
    imm_ = std::move(mem_);
    imm_->set_sealed_time_ms(now_ms());
    mem_ = new_memtable();
}

Status DbImpl::maybe_freeze_memtable(bool force) {
    if (inline_mode()) return freeze_and_flush_inline(force);

    // Declared before the lock so it runs *after* the lock is released — the sink must never see
    // an engine mutex held, and every exit below is inside the critical section.
    struct Deferred {
        const DbImpl* self;
        StallLog log;
        ~Deferred() try {
            self->report_stall(log);
        } catch (...) {
        }
    } deferred{this};

    std::unique_lock<std::mutex> lock(mem_mutex_);
    ELYSIUMKV_LOCK_AUDIT();
    if (!memtable_flush_due(force)) return Status::Ok;
    if (mem_->empty() && imm_ == nullptr) return Status::Ok;

    // A non-forced freeze is opportunistic: the write it follows has already landed, so there is
    // nothing left to refuse and nothing a wait here would protect. `await_rotation_slot` is the
    // valve, and it runs before the insert; this rotation falls to the next writer or, on an idle
    // store, to the coordinator.
    if (!force && imm_ != nullptr) return Status::Ok;

    // Backpressure: one memtable may be in flight. Without a WAL there is
    // nowhere else to put writes, so the writer waits.
    if (Status status = await_flush_slot(lock, /*force=*/true, deferred.log);
        status != Status::Ok) {
        return status;
    }
    if (mem_->empty()) return Status::Ok;

    seal_memtable();
    lock.unlock();
    flush_scheduled_.notify_one();
    return Status::Ok;
}

Status DbImpl::refresh() {
    if (unusable_.load()) return Status::Unusable;
    // A writer's version is the newest by construction — it authored it — so there is nothing to
    // re-read and nothing to install.
    if (!read_only_) return Status::Ok;

    // The same read path `open` uses: pointer, snapshot, replay. A full rebuild rather than an
    // incremental one, which is the cheaper thing to get right and is bounded by the generation's
    // edit count because the writer rolls.
    const Status status = versions_->recover();
    if (status == Status::NotFound) return Status::NotFound;
    if (status != Status::Ok) return status;

    // Readers opened before the writer advanced hold `SstReader`s for files that may now be gone.
    // Nothing is invalidated by that — objects are write-once, so a reader still open on an old
    // file keeps reading correct bytes — and the cache is keyed by file number, which is never
    // reused. So there is deliberately nothing to evict here.
    return Status::Ok;
}

Status DbImpl::flush() {
    if (read_only_) return Status::Config;
    if (inline_mode()) return freeze_and_flush_inline(true);
    if (Status status = maybe_freeze_memtable(true); status != Status::Ok) return status;

    std::unique_lock<std::mutex> lock(mem_mutex_);
    ELYSIUMKV_LOCK_AUDIT();
    while (imm_ != nullptr && bg_error_ == Status::Ok && !shutting_down_) {
        flush_finished_.wait(lock);
    }
    return imm_ == nullptr ? Status::Ok : bg_error_;
}

/// The flush executor: memtable flushing and nothing else.
///
/// It has its own thread for write availability. One immutable memtable is allowed in
/// flight, so if flushing shared a worker with compaction, a memtable filling while a
/// compaction ran would block the next rotation and stall writes for the length of that
/// compaction — up to `max_compaction_bytes` over throughput.
///
/// It evaluates no predicates. Whether a flush is due belongs to the coordinator, which also
/// performs the rotation — a pointer swap under `mem_mutex_` is not long-running work — so the
/// age-driven flush is one entry in the shared predicate table rather than a private timer here.
void DbImpl::background_flush_loop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mem_mutex_);
            ELYSIUMKV_LOCK_AUDIT();
            while (!shutting_down_ && (imm_ == nullptr || bg_error_ != Status::Ok)) {
                // Bounded, never untimed: a retryable failure leaves `bg_error_` set with a
                // frozen memtable still waiting, and the coordinator's tick is what asks again.
                flush_scheduled_.wait_for(lock, options_.maintenance_interval);
                if (shutting_down_) return;
            }
            if (shutting_down_) return;
        }

        // The thread is a loop over the same function synchronous mode calls.
        Status status = Status::Ok;
        while (run_one_flush(status)) {
        }
        schedule_compaction();
    }
}

// --- maintenance scheduling ---------------------------------------------------
//
// See `db_impl.hpp` for why scheduling pulls rather than waits to be pushed.

namespace {

/// How often the coordinator evaluates every predicate regardless of the gate.
///
/// The gate is itself a push dependency — narrower than the one it replaced, but real — so a
/// predicate whose invalidating transitions were never wired into the epoch would otherwise be
/// hidden indefinitely. The bypass turns that into *late* rather than *never*. What it cannot
/// cover is a task nobody wrote a predicate for; nothing can.
///
/// Counted in ticks rather than milliseconds on purpose: at the default one-second interval this
/// is once a minute, and a test running a 20 ms tick gets a bypass every 1.2 seconds — short
/// enough that the convergence invariant exercises both paths without a second knob.
constexpr uint64_t kGateBypassEveryTicks = 60;

}  // namespace

uint64_t DbImpl::live_maintenance_epoch() const {
    // Two monotone counters added together: the sum is monotone, which is all an equality gate
    // needs. Version installs dominate — compaction, migration, eviction and flush all install —
    // and `maintenance_bumps_` carries what installs nothing.
    return versions_->installs() + maintenance_bumps_.load(std::memory_order_relaxed);
}

uint64_t DbImpl::maintenance_epoch() const {
    const int64_t pinned = pinned_maintenance_epoch_.load();
    if (pinned >= 0) return static_cast<uint64_t>(pinned);   // negative control; see the header
    return live_maintenance_epoch();
}

void DbImpl::invalidate_maintenance() {
    maintenance_bumps_.fetch_add(1, std::memory_order_relaxed);
    if (!suppress_maintenance_wakes_.load()) maintenance_tick_.notify_one();
}

uint64_t DbImpl::next_time_transition(const Version& version, uint64_t now) const {
    uint64_t earliest = std::numeric_limits<uint64_t>::max();
    const auto consider = [&](uint64_t at) {
        // Strictly in the future. A crossing already past has been folded into the reconcile
        // that observed it; returning it would leave `now >= next_time_transition_ms_` forever
        // and re-run the full scan every tick.
        if (at > now && at < earliest) earliest = at;
    };

    // The orphan sweep is time-driven and has nothing to do with any file, so it would be
    // invisible to a gate that only watches placement deadlines — and a quiet store would never
    // sweep. Exactly the defect this whole loop exists to have fixed, one policy later.
    if (options_.orphan_sweep_interval.has_value()) consider(next_sweep_ms_.load());

    // Iterated in place: `all_files()` returns a vector of `FileMetadata`, each carrying seven
    // strings, and this runs on every version install rather than on the clock.
    for (const auto& level : version.levels()) {
      for (const FileMetadata& file : level) {
        const int index = tiers_.tier_of_store(file.store_id);
        if (index < 0) continue;
        const Tier& tier = tiers_.tiers[static_cast<size_t>(index)];
        // Placement is monotone in age, so the next time this file's placement can change is
        // when it outgrows the tier it is on. Size mismatches are not time-driven — they are
        // true the moment the file exists, and the epoch covers them.
        if (tier.max_age.has_value()) {
            // The same offset `placement()` applies, re-derived rather than remembered: the two
            // must agree, or the store wakes at a deadline placement does not honour, or sleeps
            // past one that came early.
            const uint64_t span = static_cast<uint64_t>(tier.max_age->count());
            consider(file.min_write_time_ms + span
                     - tier_age_jitter_ms(tiers_, file.file_number, file.min_write_time_ms, span));
        }
        // The stall valve is a published predicate, so its crossing must open the gate too — the
        // flag would otherwise go stale on a store nothing is writing to.
        if (tier.durability == Durability::Transient && tier.stall_age.has_value()) {
            consider(file.min_write_time_ms + static_cast<uint64_t>(tier.stall_age->count()));
        }
      }
    }
    return earliest;
}

void DbImpl::publish_transient_stall(const Version& version, uint64_t now) {
    // Computed unconditionally, pin or no pin: `transient_stalled()` is where the override is
    // applied, so this keeps the real value fresh and unpinning is correct immediately rather than
    // at the next tick.
    // One pass over the files, keyed by store id rather than by tier index: several tiers may name
    // one store, and each has to see that store's oldest file.
    std::map<std::string, uint64_t> oldest_by_store;
    for (const auto& level : version.levels()) {
        for (const FileMetadata& file : level) {
            uint64_t& oldest = oldest_by_store[file.store_id];
            if (oldest == 0 || file.min_write_time_ms < oldest) oldest = file.min_write_time_ms;
        }
    }

    bool stalled = false;
    for (const Tier& tier : tiers_.tiers) {
        if (tier.durability != Durability::Transient || !tier.stall_age.has_value()) continue;
        const auto found = oldest_by_store.find(tier.store->id());
        if (found == oldest_by_store.end()) continue;
        const uint64_t oldest = found->second;
        if (oldest != 0 && now > oldest &&
            now - oldest > static_cast<uint64_t>(tier.stall_age->count())) {
            stalled = true;
        }
    }
    transient_stalled_.store(stalled);
    // A held writer has to re-evaluate. A bare flag plus a notify would have the classic
    // lost-wakeup shape — a writer reads "stalled", this clears and notifies, and only then does
    // the writer sleep, waiting for a notification that has already happened. What makes that
    // harmless here is that `throttle_writes` re-evaluates the whole condition on every iteration
    // and its wait is *bounded*, so a lost notification costs one short timeout rather than a hang.
    // The notify is the fast path, not the correctness argument.
    compaction_finished_.notify_all();
}

void DbImpl::reconcile(bool force_full) {
    // --- 1. The O(1) predicates, ahead of the gate.
    //
    // Memtable size is one comparison and its age is two, so there is no reason for either to
    // depend on the epoch — and representing them in it would be the fragile half. Writes grow
    // the memtable and install no version, so a version-generation gate would hide the most
    // common task in the engine behind a notification.
    bool rotated = false;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        // `imm_ != nullptr` means the flush executor is already busy; there is nothing to hand
        // it, and waiting for it here would be the coordinator doing long-running work.
        if (!shutting_down_ && imm_ == nullptr && bg_error_ == Status::Ok && mem_ != nullptr &&
            mem_->num_entries() > 0 && memtable_flush_due(false)) {
            seal_memtable();
            rotated = true;
        }
    }
    if (rotated) {
        flush_scheduled_.notify_one();
        invalidate_maintenance();   // a rotation changes predicate state and installs nothing
    }

    // --- 2. The gate.
    const uint64_t epoch = maintenance_epoch();
    const uint64_t now = now_ms();
    // The negative control drops both clock-driven ways in, leaving only an epoch change — which
    // means a write. See `suppress_timed_maintenance_for_test`.
    const bool timed = !suppress_timed_maintenance_.load();
    const bool gate_open = (force_full && timed) || epoch != last_reconciled_epoch_.load() ||
                           (timed && now >= next_time_transition_ms_);

    if (!gate_open) {
        // Nothing that needs a version scan can have become due. One predicate still can:
        // obsolete-object collection becomes possible when an iterator closes and releases the
        // last reference to an old version, and the *current* version's generation does not
        // change when that happens. The hint is O(1) and lock-free, and collection is the only
        // thing this could have made due — so it runs on its own, without waking the executor
        // into a full pick scan.
        if (versions_->pending_deletions_hint() != 0) versions_->collect_obsolete();
        return;
    }
    last_reconciled_epoch_.store(epoch);

    // --- 3. The full evaluation, which is the only O(files) part.
    {
        auto version = versions_->current();
        publish_transient_stall(*version, now);
        next_time_transition_ms_ = next_time_transition(*version, now);
    }

    // Dispatch. The executor re-picks precisely — the gate has established only that *something*
    // may be due, which is what makes the idle path free; it does not identify what. Not through
    // `schedule_compaction`, whose dispatch a test may suppress: this is the pull path, and
    // suppressing it is what the negative control removes.
    dispatch_maintenance();
}

void DbImpl::maintenance_loop() {
    while (true) {
        bool timed_wake = false;
        {
            std::unique_lock<std::mutex> lock(maintenance_mutex_);
            ELYSIUMKV_LOCK_AUDIT();
            if (shutting_down_maintenance_) return;
            // Always bounded. Waiting untimed when no tier has a `max_age` was the first
            // draft's shape and it reintroduces the defect: a tier crossing `max_bytes`, a
            // level's score passing 1, a version becoming collectible — none of those has an
            // age bound, so an indefinite wait plus a missed notification is a task that never
            // runs. A periodic wake with the gate closed costs two comparisons.
            timed_wake = maintenance_tick_.wait_for(lock, options_.maintenance_interval) ==
                         std::cv_status::timeout;
            if (shutting_down_maintenance_) return;
        }
        Status retried_from = Status::Ok;
        if (timed_wake) {
            std::lock_guard<std::mutex> lock(mem_mutex_);
            ELYSIUMKV_LOCK_AUDIT();
            if (imm_ != nullptr && is_retryable(bg_error_)) {
                retried_from = bg_error_;
                bg_error_ = Status::Ok;
            }
        }
        if (retried_from != Status::Ok) {
            log_event(LogLevel::Warn, LogEvent::BackgroundRetry,
                      "retrying a flush that failed with ", status_name(retried_from));
            flush_scheduled_.notify_one();
        }
        ++reconcile_ticks_;
        reconcile(/*force_full=*/reconcile_ticks_ % kGateBypassEveryTicks == 0);
    }
}

Status DbImpl::flush_memtable(const std::shared_ptr<SkiplistMemtable>& memtable) {
    auto source = memtable->ascending();
    const ResolvedLevel& level = level_config(0);

    SstOptions sst_options;
    sst_options.block_bytes = options_.block_bytes;
    sst_options.restart_interval = options_.restart_interval;
    sst_options.bloom_bits_per_key = options_.bloom_bits_per_key;
    sst_options.compression = level.compression;

    // A memtable that saw only a `delete_range` has no entries and still has something to say, so
    // the emptiness test is over both.
    const std::vector<RangeTombstone> ranges = memtable->range_tombstones();
    auto built = build_sst(*source, sst_options, /*drop_tombstones=*/false, ranges);
    if (!built) return built.error();
    if (built->num_entries == 0 && built->num_range_tombstones == 0) return Status::Ok;

    // ARCHITECTURE.md "A tier is not a level" — placement from the memtable's age alone.
    const Tier& tier = tier_for(/*file_number=*/0, memtable->creation_time_ms());

    // The flush output, whole, held until the put lands. Same reason as the migration copy.
    const BudgetCharge charged(options_.memory_budget, built->bytes.size());
    auto written = write_new_sst(*tier.store, Slice::from(built->bytes));
    if (!written) return written.error();

    FileMetadata file;
    file.level = 0;
    file.file_number = written->file_number;
    file.encryption_provider = written->encryption_provider;
    file.encryption_metadata = written->encryption_metadata;
    file.store_id = tier.store->id();
    file.smallest_key = built->smallest_key;
    file.largest_key = built->largest_key;
    file.file_bytes = built->bytes.size();
    file.num_entries = built->num_entries;
    file.num_tombstones = built->num_tombstones;
    file.num_range_tombstones = built->num_range_tombstones;
    file.smallest_range_key = built->smallest_range_key;
    file.largest_range_key = built->largest_range_key;
    file.compression = level.compression;
    // A flushed L0 file inherits its memtable's creation time (ARCHITECTURE.md "The manifest is snapshots plus edits"); this is
    // the only place the value originates.
    file.min_write_time_ms = memtable->creation_time_ms();
    // The seal time: an exact upper bound on the writes in this memtable, and what an age-based
    // expiry measures against.
    file.max_write_time_ms = memtable->sealed_time_ms();
    // The sealed memtable's interval, not the interval current when the file is written. The
    // dangerous variant is the latter: a watermark set after this memtable was sealed covers
    // writes still sitting in the live one, so reporting it would tell the embedder to skip
    // replaying data the engine never stored. Flushes are ordered — one immutable memtable at a
    // time, so seal order is flush order — so by the time this file is in the manifest, every
    // earlier memtable's file already is.
    file.watermark = memtable->watermark();

    VersionEdit edit;
    // A flush does not touch the floor, deliberately — see `WatermarkFloor::lower_to`. Files
    // written during a replay are the youngest data in the store and sit on the tier that just
    // failed, so crediting them would certify a position the next loss could destroy again.
    edit.added.push_back(std::move(file));
    if (Status status = versions_->apply(std::move(edit)); status != Status::Ok) return status;

    if (options_.paranoid_checks) return check_invariants();
    return Status::Ok;
}

const char* invariant_name(Invariant invariant) {
    switch (invariant) {
        case Invariant::None: return "none";
        case Invariant::StoreMissing: return "StoreMissing";
        case Invariant::ObjectMissing: return "ObjectMissing";
        case Invariant::EntryCount: return "EntryCount";
        case Invariant::KeyRange: return "KeyRange";
        case Invariant::LevelOverlap: return "LevelOverlap";
    }
    return "unknown";
}

#ifdef ELYSIUMKV_PARANOID
Status DbImpl::break_invariant_for_test(Invariant which) {
    auto version = versions_->current();
    std::vector<std::vector<FileMetadata>> levels(version->num_levels());
    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        levels[static_cast<size_t>(level)] = version->files_at(level);
    }

    // Every case needs a file to spoil; a caller that has written nothing is
    // asking a question with no answer.
    FileMetadata* victim = nullptr;
    int victim_level = 0;
    for (int level = 0; level < static_cast<int>(levels.size()); ++level) {
        if (!levels[static_cast<size_t>(level)].empty()) {
            victim = &levels[static_cast<size_t>(level)].front();
            victim_level = level;
            break;
        }
    }
    if (victim == nullptr) return Status::Config;

    switch (which) {
        case Invariant::ObjectMissing: {
            // Delete the object out from under a version that still lists it.
            auto store = store_for(victim->store_id);
            if (store == nullptr) return Status::Config;
            return store->remove(sst_object_name(victim->file_number)).get();
        }
        case Invariant::StoreMissing:
            victim->store_id = "a-store-that-was-never-configured";
            break;
        case Invariant::EntryCount:
            victim->num_entries += 1;
            break;
        case Invariant::KeyRange:
            victim->smallest_key = "\x01-a-key-this-file-does-not-contain";
            break;
        case Invariant::LevelOverlap: {
            // Two files covering the same range at a level that must be
            // non-overlapping. Needs a level below L0, so put it there.
            if (victim_level == 0 && levels.size() < 2) return Status::Config;
            const int target = victim_level == 0 ? 1 : victim_level;
            FileMetadata twin = *victim;
            twin.level = target;
            auto& at_target = levels[static_cast<size_t>(target)];
            if (at_target.empty()) {
                FileMetadata original = *victim;
                original.level = target;
                at_target.push_back(original);
            }
            twin.smallest_key = at_target.front().smallest_key;
            twin.largest_key = at_target.front().largest_key;
            twin.file_number = at_target.front().file_number;
            at_target.push_back(twin);
            break;
        }
        case Invariant::None:
            return Status::Config;
    }

    versions_->install_for_test(std::make_shared<const Version>(
        std::move(levels), versions_->next_file_number(), std::map<int, std::string>{}));
    return Status::Ok;
}
#endif

Status DbImpl::check_invariants(Invariant* which) const {
    // Reported through an out-parameter rather than the status, because Status
    // is the engine's error vocabulary and "which invariant" is a different
    // question: every one of these is Corrupt to a caller.
    const auto fail = [&](Invariant invariant) {
        if (which != nullptr) *which = invariant;
        return Status::Corrupt;
    };
    if (which != nullptr) *which = Invariant::None;

    auto version = versions_->current();

    // One list per distinct store, never a get per file — the same shape ARCHITECTURE.md "Open and recovery"
    // requires of open-time verification.
    std::map<std::string, std::vector<std::string>> listings;
    for (const auto& [store_id, store] : tiers_.stores) {
        auto names = store->bulk_view().list("").get();
        if (!names) return names.error();
        listings.emplace(store_id, std::move(*names));
    }

    // Indexed once, for the reason `verify_stores_and_discard` gives: linear per file made this
    // quadratic, and ordering is not something `BlobStore::list` promises.
    std::map<std::string, std::unordered_set<std::string_view>> present;
    for (const auto& [store_id, names] : listings) {
        auto& index = present[store_id];
        index.reserve(names.size());
        for (const std::string& name : names) index.emplace(name);
    }

    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        const auto& files = version->files_at(level);

        for (size_t i = 0; i < files.size(); ++i) {
            const FileMetadata& file = files[i];

            // Every file in the current version exists in its recorded store.
            auto listing = present.find(file.store_id);
            if (listing == present.end()) return fail(Invariant::StoreMissing);
            const std::string name = sst_object_name(file.file_number);
            if (!listing->second.contains(name)) {
                return fail(Invariant::ObjectMissing);
            }

            // The recorded key range matches the contents.
            auto reader = const_cast<DbImpl*>(this)->reader_for(file);
            if (!reader) return reader.error();
            auto it = (*reader)->iterator();

            uint64_t entries = 0;
            std::string first;
            std::string last;
            for (it->seek_to_first(); it->valid(); it->next()) {
                if (entries == 0) first = it->key().to_string();
                last = it->key().to_string();
                ++entries;
            }
            if (it->status() != Status::Ok) return it->status();
            if (entries != file.num_entries) return fail(Invariant::EntryCount);
            if (entries != 0 && (first != file.smallest_key || last != file.largest_key)) {
                return fail(Invariant::KeyRange);
            }

            // L1 and below are non-overlapping by construction (ARCHITECTURE.md "Compaction") — the
            // property the merging iterator's positional recency depends on.
            if (level > 0 && i > 0) {
                if (!(Slice::from(files[i - 1].largest_key) < Slice::from(file.smallest_key))) {
                    return fail(Invariant::LevelOverlap);
                }
            }
        }
    }
    return Status::Ok;
}


// --- read path ----------------------------------------------------------------

EncryptionProvider* DbImpl::provider_for(const std::string& id) const {
    return encryption_.find(id);
}

/// No special case for "not encrypted". The empty id resolves to `NoEncryptionProvider` like
/// any other id resolves to its provider, and that provider returns an identity cipher. One path
/// through the read and write paths, so there is one path to get right and one to test — which is
/// what the reserved id is for.
Result<std::shared_ptr<ObjectCipher>> DbImpl::cipher_for(const FileMetadata& file) const {
    if (file.encryption_provider.empty() && !encryption_.primary.empty() &&
        !encryption_.accept_plaintext) {
        return std::unexpected(Status::Config);
    }
    EncryptionProvider* provider = provider_for(file.encryption_provider);
    if (provider == nullptr) {
        // The bytes are intact; what is missing is the configuration that can read them. Reporting
        // corruption here would send an operator to a restore they do not need.
        return std::unexpected(Status::Config);
    }
    return provider->open(file.file_number, Slice::from(file.encryption_metadata));
}

Result<std::shared_ptr<BlobStore>> DbImpl::decrypting_view(BlobStore& store,
                                                           const FileMetadata& file) const {
    auto cipher = cipher_for(file);
    if (!cipher) return std::unexpected(cipher.error());
    return std::shared_ptr<BlobStore>(std::make_shared<EncryptedObject>(
        store, *cipher, sst_object_name(file.file_number), file.file_bytes));
}

Result<std::shared_ptr<SstReader>> DbImpl::reader_for(const FileMetadata& file) {
    if (auto resident = readers_.get(file.file_number)) return resident;

    BlobStore* store = store_for(file.store_id);
    if (store == nullptr) return std::unexpected(Status::Corrupt);

    auto view = decrypting_view(*store, file);
    if (!view) return std::unexpected(view.error());

    SstReaderOptions reader_options;
    reader_options.block_bytes = options_.block_bytes;
    reader_options.file_number = file.file_number;
    reader_options.block_cache = block_cache_.get();
    reader_options.owned_store = *view;

    auto reader = SstReader::open(*store, sst_object_name(file.file_number), file.file_bytes,
                                  reader_options);
    if (!reader) {
        if (reader.error() == Status::NotFound && read_only_) {
            // Staleness is not corruption, and telling them apart needs no coordination.
            // Objects are write-once and file numbers are never reused, so a missing object means
            // one of two things, and re-reading the manifest pointer says which: if it has advanced
            // past the version holding this file, the writer collected it legitimately and this
            // instance is simply older than the retention window. Reporting that as `Corrupt` would
            // send an operator to a restore for a perfectly healthy store.
            if (manifest_has_advanced()) {
                last_error_ = "file " + sst_object_name(file.file_number) +
                              " was collected by the writer: this read-only instance is older than "
                              "the retention window — refresh() or reopen";
                return std::unexpected(Status::Stale);
            }
            // The manifest still references it, so the object is genuinely gone.
        }
        if (reader.error() == Status::NotFound) {
            // ARCHITECTURE.md "A tier is not a level" — a file vanishing while the store is open cannot be dropped in
            // place — live iterators hold Versions referencing it. Repair running
            // alongside them is not worth the complexity for an event that means
            // the disk is misbehaving. Fail, mark unusable, require a reopen.
            const bool transient = tiers_.store_is_discardable(file.store_id);
            return std::unexpected(fail_terminal(
                transient ? Status::Unusable : Status::Corrupt,
                "file " + sst_object_name(file.file_number) + " vanished from " +
                    (transient ? "transient" : "durable") + " store '" + file.store_id +
                    "' while the store was open"));
        }
        return std::unexpected(reader.error());
    }

    // Insert returns the reader whether or not it stayed resident: a reader larger
    // than the whole cache, or one the shared budget declined, is still a reader.
    return readers_.insert(file.file_number, std::move(*reader));
}

void DbImpl::forget_reader(uint64_t file_number) { readers_.forget(file_number); }

std::vector<FileMetadata> DbImpl::delete_obsolete(const std::vector<FileMetadata>& files) {
    // Grouped by store, one bulk call each. A level's files routinely sit on
    // several tiers, and `remove_many` is per store — so the grouping has to happen
    // here, which is also the only place that can turn a `store_id` into a store.
    // Against S3 this is the difference between one DeleteObjects and one DELETE
    // per file after every compaction.
    std::map<std::string, std::vector<std::string>> by_store;
    for (const FileMetadata& file : files) {
        // Local bookkeeping first and unconditionally: nothing may read a file once
        // no version references it, so dropping its reader and blocks is right even
        // when the object itself cannot be removed yet.
        forget_reader(file.file_number);
        block_cache_->evict_file(file.file_number);

        // Its store is gone — a discarded transient tier. There is nothing to
        // delete and nothing to retry.
        if (store_for(file.store_id) == nullptr) continue;
        by_store[file.store_id].push_back(sst_object_name(file.file_number));
    }

    std::vector<FileMetadata> failed;
    for (const auto& [store_id, names] : by_store) {
        BlobStore* store = store_for(store_id);
        if (store->remove_many(names).get() == Status::Ok) continue;

        // Failure is reported for the batch, not per name, so every file on that
        // store stays pending. `remove` is idempotent, so retrying the ones that did
        // succeed costs a round trip and cannot be wrong — and there is no way to
        // learn which those were. Only the failing store's files are re-pended,
        // which is why grouping happens before deleting rather than after.
        for (const FileMetadata& file : files) {
            if (file.store_id == store_id) failed.push_back(file);
        }
    }
    return failed;
}

/// Whether a read must be refused because a discard has not been replayed yet.
///
/// Reads only. The replay is writes, and refusing those would make the condition permanent.
bool DbImpl::reads_are_blocked() const {
    return requires_recovery_.load(std::memory_order_relaxed) &&
           !options_.allow_reads_before_recovery;
}

Result<Pinned> DbImpl::get(Slice key) {
    if (reads_are_blocked()) return std::unexpected(Status::RecoveryRequired);

    std::shared_ptr<SkiplistMemtable> mem;
    std::shared_ptr<SkiplistMemtable> imm;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        mem = mem_;
        imm = imm_;
    }

    // The memtables must be captured before the Version. A flush publishes its file and only then
    // clears the immutable memtable, so this order sees the record on at least one side of that
    // handoff. Reversing the takes can pair the old Version with the cleared immutable memtable.
    auto version = versions_->current();

    // Below the truncation point is absence, and absence is not an error — the same answer a
    // deleted key gives, reached without touching a single file.
    if (version->truncated(key)) return std::unexpected(Status::NotFound);

    // Newest source first, and at each one the point entry is asked about before the range: a range
    // tombstone shadows everything strictly older and nothing beside it, so a point entry here
    // always wins over a range recorded here, whichever arrived first. The memtable resolved that
    // ordering when the range was recorded, which is why this need not.
    for (const auto& memtable : {mem, imm}) {
        if (memtable == nullptr) continue;
        if (auto entry = memtable->get(key)) {
            if (entry->type == ValueType::Delete) return std::unexpected(Status::NotFound);
            // The keep-alive is the memtable itself: its arena owns the bytes.
            return Pinned(memtable, entry->value, &pins_outstanding_);
        }
        if (memtable->range_deletes(key)) return std::unexpected(Status::NotFound);
    }

    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        // L0 is ordered by descending file number, so the first hit is the most
        // recent; deeper levels are non-overlapping, so there is at most one.
        const std::vector<FileMetadata>& files = version->files_at(level);

        // Binary search where the level allows it, which is what ARCHITECTURE.md - A read
        // describes and what this loop did not do: below L0 the data spans are disjoint and
        // sorted by smallest key, so `lower_bound` on the largest key lands on the only file that
        // can hold it. That is O(log n) against a scan of the level, and for a bottom level of
        // 50,000 files the scan is 50,000 string comparisons per lookup — the one cost a bloom
        // filter cannot remove, because it is paid before any file is opened.
        //
        // Not when the level carries range tombstones. Their spans are neither bounded by the
        // data span nor disjoint across files, so the ordering the search relies on does not hold
        // for the second reason to open a file, and the level is scanned as before.
        size_t begin = 0;
        size_t end = files.size();
        if (level > 0 && !version->carries_ranges(level)) {
            const auto found = std::lower_bound(
                files.begin(), files.end(), key, [](const FileMetadata& file, Slice probe) {
                    return Slice::from(file.largest_key) < probe;
                });
            begin = static_cast<size_t>(found - files.begin());
            end = std::min(files.size(), begin + 1);
        }

        for (size_t index = begin; index < end; ++index) {
            const FileMetadata& file = files[index];
            // Either span can be the reason to open this file. The tombstone span is not bounded by
            // the data span — a file can delete a range it holds no keys in — so a lookup that
            // consulted only the data span would walk straight past the file that answers it.
            const bool may_hold = file_may_contain(file, key);
            const bool may_cover = file.range_may_cover(key);
            if (!may_hold && !may_cover) continue;

            auto reader = reader_for(file);
            if (!reader) return std::unexpected(reader.error());

            if (may_hold) {
                auto found = (*reader)->get(key);
                if (!found) return std::unexpected(classify_read_failure(found.error()));
                if (found->has_value()) {
                    if ((*found)->type == ValueType::Delete) {
                        return std::unexpected(Status::NotFound);
                    }
                    return Pinned((*found)->block, (*found)->value, &pins_outstanding_);
                }
            }
            // No entry here, so the file's own range tombstones decide what every older file holds.
            if (may_cover) {
                auto covered = (*reader)->range_deletes(key);
                if (!covered) return std::unexpected(classify_read_failure(covered.error()));
                if (*covered) return std::unexpected(Status::NotFound);
            }
        }
    }
    return std::unexpected(Status::NotFound);
}

Result<std::vector<uint8_t>> DbImpl::get_copy(Slice key) {
    auto pinned = get(key);
    if (!pinned) return std::unexpected(pinned.error());
    const Slice value = pinned->value();
    return std::vector<uint8_t>(value.data(), value.data() + value.size());
}

std::unique_ptr<Iterator> DbImpl::make_iterator(Slice lower, Slice upper, bool has_upper,
                                                bool reverse) {
    // An iterator has no error return, so the refusal travels as it does for an unreadable file:
    // nothing yielded, and `status()` says why. Silently scanning a store that reads its own older
    // values as current is the outcome worth ruling out.
    if (reads_are_blocked()) {
        return std::make_unique<DbIterator>(
            std::make_unique<ErrorIterator>(Status::RecoveryRequired), versions_->current(),
            std::vector<std::shared_ptr<SkiplistMemtable>>{},
            std::vector<std::shared_ptr<SstReader>>{}, std::string(), std::string(), false,
            reverse);
    }

    std::shared_ptr<SkiplistMemtable> mem;
    std::shared_ptr<SkiplistMemtable> imm;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        mem = mem_;
        imm = imm_;
    }
    auto version = versions_->current();

    // Truncated keys are not skipped entry by entry: the scan simply starts above them. Clamping
    // the lower bound also serves a reverse scan, whose stop condition is that same bound — so one
    // clamp covers both directions and neither pays per entry.
    std::string clamped;
    if (!version->truncation_point().empty() &&
        (lower.empty() || lower < Slice::from(version->truncation_point()))) {
        clamped = version->truncation_point();
        lower = Slice::from(clamped);
    }

    std::vector<std::unique_ptr<InternalIterator>> children;
    std::vector<std::vector<RangeTombstone>> child_ranges;
    std::vector<std::shared_ptr<SkiplistMemtable>> memtables;
    std::vector<std::shared_ptr<SstReader>> readers;

    for (const auto& memtable : {mem, imm}) {
        if (memtable == nullptr) continue;
        memtables.push_back(memtable);
        children.push_back(reverse ? memtable->descending() : memtable->ascending());
        child_ranges.push_back(memtable->range_tombstones());
    }

    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        // Prune by key range before opening anything: this is what keeps a
        // prefix scan from touching files it cannot contain (ARCHITECTURE.md "Absence is an answer, not an error").
        // One child for the whole level, opening one file at a time, where the level allows it:
        // below L0 the files are disjoint and sorted, so at most one can answer any key. That is
        // what keeps a scan from opening every file it *might* need before returning its first
        // entry. Not when the level carries range tombstones — those shadow entries in sibling
        // files, which the merge can only apply with a child per file.
        if (level > 0 && !version->carries_ranges(level)) {
            const auto [begin, end] = version->overlapping_index_range(level, lower, upper);
            if (begin < end) {
                children.push_back(make_concat_iterator(
                    version, level, begin, end,
                    [this](const FileMetadata& file) { return reader_for(file); }));
                child_ranges.emplace_back();
            }
            continue;
        }

        for (const FileMetadata& file : version->overlapping_half_open(level, lower, upper)) {
            auto reader = reader_for(file);
            if (!reader) {
                children.push_back(std::make_unique<ErrorIterator>(reader.error()));
                child_ranges.emplace_back();
                continue;
            }
            readers.push_back(*reader);
            children.push_back((*reader)->iterator());
            auto ranges = (*reader)->range_tombstones();
            if (!ranges) {
                children.back() = std::make_unique<ErrorIterator>(
                    classify_read_failure(ranges.error()));
                child_ranges.emplace_back();
                continue;
            }
            child_ranges.push_back(std::move(*ranges));
        }
    }

    return std::make_unique<DbIterator>(make_merging_iterator(std::move(children),
                                                             std::move(child_ranges)),
                                        std::move(version), std::move(memtables),
                                        std::move(readers), lower.to_string(), upper.to_string(),
                                        has_upper, reverse);
}

std::unique_ptr<Iterator> DbImpl::iterator() {
    return make_iterator(Slice(), Slice(), false, /*reverse=*/false);
}

std::unique_ptr<Iterator> DbImpl::iterator(Slice lower_inclusive) {
    return make_iterator(lower_inclusive, Slice(), /*has_upper=*/false, /*reverse=*/false);
}

std::unique_ptr<Iterator> DbImpl::iterator(Slice lower_inclusive, Slice upper_exclusive) {
    return make_iterator(lower_inclusive, upper_exclusive, true, /*reverse=*/false);
}

std::unique_ptr<Iterator> DbImpl::reverse_iterator() {
    return make_iterator(Slice(), Slice(), false, /*reverse=*/true);
}

std::unique_ptr<Iterator> DbImpl::reverse_iterator(Slice lower_inclusive) {
    return make_iterator(lower_inclusive, Slice(), /*has_upper=*/false, /*reverse=*/true);
}

std::unique_ptr<Iterator> DbImpl::reverse_iterator(Slice lower_inclusive, Slice upper_exclusive) {
    return make_iterator(lower_inclusive, upper_exclusive, true, /*reverse=*/true);
}

std::unique_ptr<Iterator> DbImpl::prefix_iterator(Slice prefix) {
    std::string upper;
    // An all-0xFF (or empty) prefix has no upper bound; the scan simply runs to
    // the end of the keyspace.
    const bool bounded = prefix_upper_bound(prefix, upper);
    return make_iterator(prefix, bounded ? Slice::from(upper) : Slice(), bounded,
                         /*reverse=*/false);
}

std::unique_ptr<Iterator> DbImpl::reverse_prefix_iterator(Slice prefix) {
    std::string upper;
    const bool bounded = prefix_upper_bound(prefix, upper);
    return make_iterator(prefix, bounded ? Slice::from(upper) : Slice(), bounded,
                         /*reverse=*/true);
}

Stats DbImpl::stats() const {
    Stats stats;
    auto version = versions_->current();
    const uint64_t now = options_.clock();

    for (int level = 0; level < static_cast<int>(config_.levels.size()); ++level) {
        const ResolvedLevel& config = config_.levels[static_cast<size_t>(level)];
        LevelStats level_stats;
        level_stats.level = level;
        level_stats.file_count = static_cast<int>(version->file_count(level));
        level_stats.bytes = version->total_bytes(level);

        // ARCHITECTURE.md "Statistics are a buffer, not a struct" and "Inside an SST" — what has not yet been rewritten under the level's current
        // settings. A stale file is perfectly readable — the per-block codec byte
        // and the recorded store_id see to that — what it costs is completion.
        // Free: this loop already runs for the codec check. `num_entries` and `num_tombstones` are
        // written by the SST builder, so these sums are exact about records.
        for (const FileMetadata& file : version->files_at(level)) {
            if (file.compression != config.compression) ++level_stats.files_stale_codec;
            level_stats.entries += file.num_entries;
            level_stats.tombstones += file.num_tombstones;
        }

        // Compaction lag: what you want when diagnosing why a level is over its
        // score. Exposure lives on the tier axis now (ARCHITECTURE.md "Statistics are a buffer, not a struct").
        const uint64_t oldest = version->oldest_write_time_ms(level);
        if (oldest != 0) {
            level_stats.oldest_file_age = Duration(now > oldest ? now - oldest : 0);
        }
        stats.levels.push_back(level_stats);
    }

    // ARCHITECTURE.md "Statistics are a buffer, not a struct" — the tier axis: where files physically live, and what a loss of each
    // store would reach back to.
    //
    // One pass over every file, and the store ids read once. This was a full walk of every
    // level per tier, and the walk compared `file.store_id` against `tier.store->id()` — which
    // returns a `std::string` by value, so it allocated once per file per tier. The watermark
    // below was a second walk, and its `store_is_discardable` allocated per tier per file again.
    // `stats()` is the one call an operator is told to make continuously, times every instance in
    // the process; at ten thousand files and two tiers that was forty thousand allocations a
    // scrape to produce a few dozen integers.
    const int last_tier = tiers_.last();
    std::vector<std::string> tier_store_ids;
    tier_store_ids.reserve(static_cast<size_t>(last_tier) + 1);
    for (int index = 0; index <= last_tier; ++index) {
        tier_store_ids.push_back(tiers_.tiers[static_cast<size_t>(index)].store->id());
    }

    // A store id maps to every tier that names it, not to one. Nothing forbids two tiers
    // sharing a store, and a file on it belongs to both — which is what the per-tier loop did by
    // construction and what a single pass has to be told.
    std::unordered_map<std::string_view, std::vector<int>> tiers_by_store;
    std::unordered_set<std::string_view> discardable_stores;
    for (int index = 0; index <= last_tier; ++index) {
        tiers_by_store[tier_store_ids[static_cast<size_t>(index)]].push_back(index);
    }
    for (const auto& [store_id, claimants] : tiers_by_store) {
        // Discardable only if *every* tier naming it is Transient, resolving the ambiguity toward
        // the non-destructive reading exactly as `ResolvedTiers::store_is_discardable` does.
        const bool all_transient = std::ranges::all_of(claimants, [this](int index) {
            return tiers_.tiers[static_cast<size_t>(index)].durability == Durability::Transient;
        });
        if (all_transient) discardable_stores.emplace(store_id);
    }

    struct TierAccumulator {
        int file_count = 0;
        uint64_t bytes = 0;
        uint64_t oldest_write = 0;
        int pending_migration = 0;
    };
    std::vector<TierAccumulator> per_tier(static_cast<size_t>(last_tier) + 1);

    // The live watermark frontier, accumulated in the same pass. Deliberately not the maximum
    // watermark over current files: that is tier-blind, so a flush to a transient tier would
    // advance it while changing nothing an operator can rely on. This is the same expression
    // recovery uses, evaluated live — the position whose state would survive losing every
    // transient tier.
    std::optional<uint64_t> transient_low;
    bool transient_low_missing = false;
    bool any_transient = false;
    std::optional<uint64_t> high;

    for (const auto& level : version->levels()) {
        for (const FileMetadata& file : level) {
            accumulate_max(high, file.watermark.high);
            if (discardable_stores.contains(file.store_id)) {
                any_transient = true;
                if (file.watermark.low.has_value()) {
                    accumulate_min(transient_low, file.watermark.low);
                } else {
                    transient_low_missing = true;
                }
            }

            const auto claimed = tiers_by_store.find(file.store_id);
            if (claimed == tiers_by_store.end()) continue;  // a store no tier names
            const int placed = placement(tiers_, file.file_number, file.min_write_time_ms, now);
            for (const int index : claimed->second) {
                TierAccumulator& accumulated = per_tier[static_cast<size_t>(index)];
                ++accumulated.file_count;
                accumulated.bytes += file.file_bytes;
                if (accumulated.oldest_write == 0 ||
                    file.min_write_time_ms < accumulated.oldest_write) {
                    accumulated.oldest_write = file.min_write_time_ms;
                }
                if (placed > index) ++accumulated.pending_migration;
            }
        }
    }

    for (int index = 0; index <= last_tier; ++index) {
        const Tier& tier = tiers_.tiers[static_cast<size_t>(index)];
        const TierAccumulator& accumulated = per_tier[static_cast<size_t>(index)];

        TierStats tier_stats;
        tier_stats.tier = index;
        tier_stats.file_count = accumulated.file_count;
        tier_stats.bytes = accumulated.bytes;
        tier_stats.files_pending_migration = accumulated.pending_migration;

        if (tier.max_bytes.has_value() && tier_stats.bytes > *tier.max_bytes &&
            tier_stats.files_pending_migration == 0) {
            // Over capacity: eviction is pending even though nothing has aged out.
            tier_stats.files_pending_migration = tier_stats.file_count;
        }
        // The store that holds the bytes, so a cache chain in front of this tier reports its
        // delegate's traffic rather than its own — see `TierStats::io`.
        tier_stats.io = authoritative_store(*tier.store).counters();

        if (accumulated.oldest_write != 0) {
            tier_stats.oldest_file_age =
                Duration(now > accumulated.oldest_write ? now - accumulated.oldest_write : 0);
            if (tier.durability == Durability::Transient && tier.stall_age.has_value() &&
                tier_stats.oldest_file_age > *tier.stall_age) {
                tier_stats.stalling = true;
            }
        }
        stats.tiers.push_back(tier_stats);
    }

    // The lock covers taking the memtables, not reading them, which is what `get` and
    // `make_iterator` already do. Every figure below is an atomic the arena and the skiplist
    // maintain for exactly this reader, so holding the write path's mutex across them bought
    // nothing — and `stats()` is scraped continuously.
    std::shared_ptr<SkiplistMemtable> mem;
    std::shared_ptr<SkiplistMemtable> imm;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        mem = mem_;
        imm = imm_;
    }

    // Everything not yet in an SST, live and frozen alike: a frozen memtable waiting on a flush is
    // exactly as unwritten as the live one.
    uint64_t oldest_unwritten = 0;
    for (const auto& memtable : {mem, imm}) {
        if (memtable == nullptr) continue;
        stats.memtable_bytes += memtable->approximate_bytes();
        stats.memtable_entries += memtable->num_entries();
        stats.memtable_tombstones += memtable->num_tombstones();
        if (memtable->empty()) continue;
        const uint64_t created = memtable->creation_time_ms();
        if (oldest_unwritten == 0 || created < oldest_unwritten) oldest_unwritten = created;
    }
    stats.memtable_age =
        Duration(oldest_unwritten == 0 || now <= oldest_unwritten ? 0 : now - oldest_unwritten);

    // Accumulated in the pass above; only the verdict is left.
    if (!any_transient) {
        stats.durable_watermark = high;
    } else if (transient_low_missing) {
        // A transient file with no lower bound: losing it would certify nothing, so neither does
        // the gauge. Absent rather than zero, which is a valid position.
        stats.durable_watermark = std::nullopt;
    } else {
        stats.durable_watermark = transient_low;
    }

    stats.requires_recovery = requires_recovery_.load();
    stats.flushes = flushes_.load();
    stats.compactions = compactions_.load();
    stats.compactions_trimmed = trimmed_compactions_.load(std::memory_order_relaxed);
    stats.reencryptions = reencryptions_.load(std::memory_order_relaxed);
    stats.files_pending_reencryption = count_pending_reencryption();
    stats.manifest_payloads_pending_reencryption =
        versions_->manifest_payloads_pending_reencryption();
    stats.compaction_bytes_read = compaction_bytes_read_.load();
    stats.compaction_bytes_written = compaction_bytes_written_.load();
    stats.migrations = migrations_.load();
    stats.migration_bytes = migration_bytes_.load();
    stats.stalled_total = Duration(stalled_total_ms_.load());
    stats.stall_count = stalls_.load();
    stats.background_failures = background_failures_.load();
    stats.block_cache_bytes = block_cache_->approximate_bytes();
    if (options_.memory_budget != nullptr) {
        stats.memory_budget_used = options_.memory_budget->used();
        stats.memory_budget_total = options_.memory_budget->total();
    }
    stats.budget_sheds = budget_sheds_.load(std::memory_order_relaxed);
    stats.reader_cache_bytes = readers_.bytes();
    stats.open_readers = readers_.count();
    stats.reader_cache_hits = readers_.hits();
    stats.reader_cache_misses = readers_.misses();
    stats.block_cache_hits = block_cache_->hits();
    stats.block_cache_misses = block_cache_->misses();
    stats.pins_outstanding = pins_outstanding_.load();
    return stats;
}

// The shedding order (ARCHITECTURE.md "A process-wide memory budget"). The budget is process-wide,
// so an instance over it is usually over because of its neighbours; the response is to give memory
// back rather than to fail.
//
//   1. The block cache — bytes held purely as an optimisation, so the only loss is latency.
//   2. Memtables, by freezing and flushing. Costs I/O and write throughput, and cannot be first
//      because a memtable holds writes that are nowhere else yet.
//   3. Stalling, handled by `throttle_writes` below. Last, being the only step the application sees.
//
// Nothing here fails a write: a budget shapes rather than admits.
/// One place that builds a memtable, so the budget and the clock cannot be attached in
/// two of the three places and forgotten in the third.
std::shared_ptr<SkiplistMemtable> DbImpl::new_memtable() {
    auto memtable = std::make_shared<SkiplistMemtable>();
    memtable->set_memory_budget(options_.memory_budget.get());
    memtable->set_creation_time_ms(now_ms());

    // The interval starts closed at whatever is established now, and the lower bound is what the
    // recovery proof needs: `set_watermark(M)` asserts every write so far is at a position <= M, so
    // a memtable created while M is current holds no write at or below M. Capturing it at creation
    // rather than at seal is what makes that true. A later `set_watermark` moves only the upper
    // bound.
    //
    // Absent when nothing has been established: such a memtable has no lower bound, so losing it
    // certifies nothing — which is why recovery reports `nullopt` rather than zero.
    //
    // Precondition: `mem_mutex_` is held, since this reads `established_watermark_`.
    if (established_watermark_.has_value()) {
        memtable->set_watermark_bounds({*established_watermark_, *established_watermark_});
    }
    return memtable;
}

Status DbImpl::mark_recovery_complete() {
    requires_recovery_.store(false);

    // A reader never discarded anything — `verify_stores_and_discard` refuses to on a read-only
    // instance — so it has no floor to remove and no authority to write one away.
    if (read_only_) return Status::Ok;
    if (!versions_->current()->watermark_floor().has_value()) return Status::Ok;

    // Cleared outright rather than raised to what the replay reached: the embedder declaring the
    // replay done is the only signal that the files no longer over-report. A crash mid-replay
    // therefore repeats the replay rather than half-crediting it.
    VersionEdit edit;
    edit.floor_update = VersionEdit::FloorUpdate::Clear;
    return versions_->apply(std::move(edit));
}

Status DbImpl::set_watermark(uint64_t position) {
    if (read_only_) return Status::Config;
    if (unusable_.load()) return Status::Unusable;

    std::lock_guard<std::mutex> lock(mem_mutex_);
    ELYSIUMKV_LOCK_AUDIT();
    if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;

    // Taking the write path's own lock is what makes "every write completed before this call" a
    // well-defined set, which the recovery rule depends on. Calling this concurrently with a write
    // would make that boundary unsound.
    if (established_watermark_.has_value() && position < *established_watermark_) {
        // A caller bug, refused rather than clamped: clamping would hide a replay that went
        // backwards, and the whole value of the watermark is that it can be trusted.
        return Status::Config;
    }
    established_watermark_ = position;
    if (mem_ != nullptr) {
        if (mem_->empty()) {
            // Empty, so both bounds move. There are no writes for a lower bound to be wrong
            // about, and every write this memtable goes on to accept is at a position above
            // `position` by the premise. Not merely an optimisation: this is the state right
            // after a flush, which is exactly where a changelog consumer commits — the
            // `set_watermark` / `flush` pair — so without it the file about to be written would
            // inherit the *previous* commit's position as its lower bound and every discard would
            // roll back one commit further than the loss requires. It is also what gives the first
            // memtable of a fresh store a lower bound instead of an absent one that would spread
            // through every compaction lineage it joined.
            mem_->set_watermark_bounds({position, position});
        } else {
            // Non-empty: only the upper bound moves. The lower bound was fixed when this memtable
            // was created and says the memtable holds no write at or below it; a later call says
            // nothing about writes already in here, so raising it would be a false claim.
            mem_->set_watermark_high(position);
        }
    }
    return Status::Ok;
}

bool DbImpl::shed_if_over_budget() {
    MemoryBudget* budget = options_.memory_budget.get();
    if (budget == nullptr) return false;

    size_t over = budget->overage();
    if (over == 0) return false;
    budget_sheds_.fetch_add(1, std::memory_order_relaxed);

    // 1. The block cache.
    if (block_cache_ != nullptr) {
        const size_t released = block_cache_->evict_at_least(over);
        if (released >= over) return false;
    }

    over = budget->overage();
    if (over == 0) return false;

    // 2. Flush the memtable — but only if it holds enough to be worth an SST.
    //
    // A flush trades memtable bytes for a file, and the file is permanent: it joins L0 and
    // carries compaction debt for the rest of the store's life. Flushing a memtable holding a
    // handful of entries releases almost nothing and buys all of that.
    //
    // The guard is load-bearing: a budget smaller than one memtable can never be satisfied by
    // flushing, so without it every write would flush, filling L0 with near-empty files and growing
    // compaction work without bound.
    //
    // When nothing is worth flushing the overage is somewhere this step cannot reach — a blob cache,
    // another instance, or a budget set too low — and the next step is the caller's rate.
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        const size_t worth_flushing = options_.memtable_bytes / 4;
        if (mem_ == nullptr || mem_->approximate_bytes() < worth_flushing) {
            return budget->overage() > 0;
        }
    }

    // Not waited on here — this call already runs on the write path, and `throttle_writes` is
    // where waiting belongs.
    (void)maybe_freeze_memtable(/*force=*/true);
    return budget->overage() > 0;
}

void DbImpl::report_stall(const StallLog& log) const {
    if (log.retried_from != Status::Ok) {
        log_event(LogLevel::Warn, LogEvent::BackgroundRetry, "retrying a flush that failed with ",
                  status_name(log.retried_from));
    }
    if (log.gave_up_with != Status::Ok) {
        log_event(LogLevel::Error, LogEvent::BackgroundFailure, "a write is failing on ",
                  status_name(log.gave_up_with));
    }
    if (log.rejected) {
        log_event(LogLevel::Warn, LogEvent::StallEntered,
                  "write rejected: a flush is still in flight");
    } else if (log.stalled_ms != 0) {
        log_event(LogLevel::Warn, LogEvent::StallLeft, "writer blocked ", log.stalled_ms,
                  " ms waiting for a flush");
    }
}

Status DbImpl::await_flush_slot(std::unique_lock<std::mutex>& lock, bool force, StallLog& log) {
    bool retried = false;
    while (imm_ != nullptr && !shutting_down_ && (force || memtable_flush_due(/*force=*/false))) {
        if (bg_error_ != Status::Ok) {
            // Io means "ask again later" (ARCHITECTURE.md "Immutable named objects"), so a failed flush must not
            // leave the instance permanently unable to flush. One retry per
            // call; if it fails again the caller hears about it.
            if (!is_retryable(bg_error_) || retried) {
                log.gave_up_with = bg_error_;
                return bg_error_;
            }
            log.retried_from = bg_error_;
            bg_error_ = Status::Ok;
            retried = true;
            flush_scheduled_.notify_one();
        }
        stalls_.fetch_add(1, std::memory_order_relaxed);
        if (!options_.block_on_stall) {
            log.rejected = true;
            return Status::Stalled;
        }
        const uint64_t stall_began = options_.clock();
        flush_finished_.wait(lock);
        const uint64_t stall_ended = options_.clock();
        if (stall_ended > stall_began) {
            stalled_total_ms_.fetch_add(stall_ended - stall_began, std::memory_order_relaxed);
            log.stalled_ms += stall_ended - stall_began;
        }
    }
    return Status::Ok;
}

Status DbImpl::await_rotation_slot() {
    // Inline mode has no executor to be busy: the writer performs its own rotation in
    // `freeze_and_flush_inline`, so there is no slot to wait for.
    if (inline_mode()) return Status::Ok;

    StallLog log;
    Status result = Status::Ok;
    {
        std::unique_lock<std::mutex> lock(mem_mutex_);
        ELYSIUMKV_LOCK_AUDIT();
        // A write arriving at a fresh memtable is admitted even with a flush in flight: nothing is
        // owed yet. It may end the call over `memtable_bytes`, and the next write is the one that
        // waits, which bounds the overshoot at one write per writer.
        result = await_flush_slot(lock, /*force=*/false, log);
    }
    report_stall(log);
    return result;
}

Status DbImpl::throttle_writes() {
    if (unusable_.load()) return Status::Unusable;
    if (Status status = await_rotation_slot(); status != Status::Ok) return status;

    // Reported once per stalled call rather than per wait iteration, and after the loop so no lock
    // is held. The reason is what an operator needs: which valve closed, not that one did.
    struct Deferred {
        const DbImpl* self;
        bool reported = false;
        bool rejected = false;
        const char* reason = "";
        int level = -1;
        int files = 0;
        uint64_t stalled_ms = 0;
        ~Deferred() try {
            if (!reported) return;
            std::ostringstream where;
            if (level >= 0) where << " (L" << level << " at " << files << " files)";
            if (rejected) {
                self->log_event(LogLevel::Warn, LogEvent::StallEntered, "write rejected on ",
                                reason, where.str());
            } else {
                self->log_event(LogLevel::Warn, LogEvent::StallLeft, "writer blocked ", stalled_ms,
                                " ms on ", reason, where.str());
            }
        } catch (...) {
        }
    } deferred{this};

    // ARCHITECTURE.md "A process-wide memory budget" before the level and tier valves: a process over its memory budget has a
    // problem that no amount of compaction fixes, and the two cheapest remedies are
    // free of the compaction machinery entirely.
    //
    // Stalling on the budget is bounded by progress, not by the budget clearing. A
    // level stall clears when compaction catches up; a budget stall clears only if memory is given
    // back, and a budget set too low for the instances sharing it never gives any. So the writer
    // waits only while the overage is shrinking: backpressure when the system can recover, and a
    // bounded delay when it cannot.
    bool budget_stall = shed_if_over_budget();
    size_t budget_overage = budget_stall ? options_.memory_budget->overage() : 0;

    // Every path that loops must run this, or the budget stall never ends. It lived only
    // in the threaded branch at first, and the inline branch — which `continue`s after
    // compacting — spun on `compact_until_quiet()` for as long as compaction had any work at
    // all. With a budget that cannot be satisfied that is forever, and the L0 churn shedding
    // itself produces guarantees compaction always has work. It presented as a single
    // operation taking over five minutes, 4,000 operations into a differential replay.
    const auto budget_still_holding = [&] {
        if (!budget_stall) return false;
        const size_t now_over = options_.memory_budget->overage();
        // Cleared, or not improving: either way this write stops waiting on the budget. One
        // that is not improving will not improve by waiting longer.
        if (now_over == 0 || now_over >= budget_overage) budget_stall = false;
        budget_overage = now_over;
        return budget_stall;
    };

    while (true) {
        // Not taken unless something needs it. The valve reads file counts and nothing else,
        // and `VersionSet` publishes those as atomics — so the common case, a write that is not
        // throttled at all, costs no lock and no refcount. Inline mode takes it regardless: it has
        // to publish the transient-stall flag itself, and is single-threaded anyway.
        std::shared_ptr<const Version> version;
        const auto version_for = [&]() -> const Version& {
            if (version == nullptr) version = versions_->current();
            return *version;
        };
        const uint64_t now = now_ms();

        bool stop = false;
        bool slowdown = false;
        for (const ResolvedLevel& level : config_.levels) {
            const auto files = static_cast<int>(
                published_level_counts_ ? versions_->published_file_count(level.level)
                                        : version_for().file_count(level.level));
            if (level.stop_at.has_value() && files >= *level.stop_at) stop = true;
            if (level.slowdown_at.has_value() && files >= *level.slowdown_at) slowdown = true;

        }

        // ARCHITECTURE.md "Migration between tiers" — the valve, now on the tier axis: it is what makes the exposure
        // bound a guarantee rather than an expectation, so it is not
        // configurable off.
        //
        // Read, not computed: the maintenance coordinator owns this predicate and publishes the
        // answer, so there is one definition rather than two that can diverge, and the write path
        // costs O(1) instead of O(files). Engaging therefore lags by up to one tick — the same
        // `+ interval` term the exposure window carries. `open` reconciles synchronously, so the
        // flag is never stale on the first write.
        if (inline_mode()) {
            // No coordinator to publish it, so the writer that would otherwise wait for one
            // evaluates it itself — the same asymmetry `flush_interval` already documents.
            publish_transient_stall(version_for(), now);
        }
        if (transient_stalled()) stop = true;

        // 3. Still over after evicting and flushing: the memory is genuinely in use, so
        // the last lever is the caller's rate. Treated exactly like level pressure, so
        // `block_on_stall` and the stall counters behave the same way for both.
        if (budget_stall) stop = true;

        if (!stop) {
            if (slowdown && !inline_mode()) {
                // Throttle rather than block: one memtable of headroom is enough
                // for compaction to catch up if it is only slightly behind.
                schedule_compaction();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return Status::Ok;
        }

        if (inline_mode()) {
            // There is no other thread to wait for: the writer performs the
            // compaction that releases the stall. Same guarantee, by other means.
            const Status status = compact_until_quiet();
            if (status != Status::Ok) return status;
            budget_still_holding();
            auto before = version;
            if (before == nullptr) before = versions_->current();
            if (versions_->current() == before) return Status::Ok;  // nothing left to do
            continue;
        }

        stalls_.fetch_add(1, std::memory_order_relaxed);
        schedule_compaction();
        if (!deferred.reported) {
            deferred.reported = true;
            deferred.reason = budget_stall  ? "the memory budget"
                              : transient_stalled() ? "a transient tier past its stall age"
                                                    : "level file counts";
            for (const ResolvedLevel& level : config_.levels) {
                const auto files = static_cast<int>(version_for().file_count(level.level));
                if (level.stop_at.has_value() && files >= *level.stop_at) {
                    deferred.level = level.level;
                    deferred.files = files;
                    break;
                }
            }
        }
        if (!options_.block_on_stall) {
            deferred.rejected = true;
            return Status::Stalled;
        }

        const uint64_t began = now_ms();
        {
            std::unique_lock<std::mutex> lock(compaction_mutex_);
            ELYSIUMKV_LOCK_AUDIT();
            if (shutting_down_compaction_) return Status::Ok;
            compaction_finished_.wait_for(lock, std::chrono::milliseconds(50));
        }
        const uint64_t ended = now_ms();
        if (ended > began) {
            stalled_total_ms_.fetch_add(ended - began, std::memory_order_relaxed);
            deferred.stalled_ms += ended - began;
        }

        {
            std::lock_guard<std::mutex> lock(mem_mutex_);
            ELYSIUMKV_LOCK_AUDIT();
            if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        }
        if (unusable_.load()) return Status::Unusable;
        if (versions_->fenced()) return Status::Fenced;

        budget_still_holding();
    }
}

// --- durability (ARCHITECTURE.md "A tier is not a level", ARCHITECTURE.md "Open and recovery") ---------------------------------------------------

Status DbImpl::fail_terminal(Status status, std::string detail) {
    last_error_ = std::move(detail);
    unusable_.store(true);
    return status;
}

Result<DbImpl::WrittenObject> DbImpl::write_new_sst(BlobStore& store, Slice bytes,
                                                    bool seal) {
    // ARCHITECTURE.md "Immutable named objects" says it outright: "a failed `put` must not be retried under the same name —
    // allocate a new file number instead; the partial object becomes an orphan and is
    // collected". A taken name is a numbering accident, not a verdict on ownership.
    //
    // Ownership is arbitrated at the manifest, the only place with a compare-and-swap, so a name
    // collision here is a numbering accident and is resolved by renumbering rather than by
    // fencing. Reporting `Fenced` would make a crashed writer's leftover object — sitting at the
    // number recovery hands back out — permanently fatal, colliding again on every reopen.
    //
    // The provider is resolved once rather than per attempt: the retry loop below changes the file
    // number, not the provider. The empty id resolves like any other, to the one that does nothing.
    EncryptionProvider* provider = provider_for(encryption_.primary);
    if (provider == nullptr) return std::unexpected(Status::Config);

    constexpr int kAttempts = 16;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        const uint64_t file_number = versions_->allocate_file_number();
        const std::string name = sst_object_name(file_number);

        // Sealed inside the loop, because the ciphertext depends on the number. The file number
        // is the object id bound into every chunk's authentication, so a renumber is a different
        // object and its bytes have to be produced again. Sealing once outside would write, on the
        // second attempt, ciphertext that authenticates as belonging to a file that does not exist.
        WrittenObject written;
        written.file_number = file_number;
        std::string sealed;
        Slice payload = bytes;
        if (seal) {
            auto created = provider->create(file_number);
            if (!created) return std::unexpected(created.error());
            auto material = EncryptedObject::seal_object(*created->cipher, bytes);
            if (!material) return std::unexpected(material.error());
            sealed = std::move(*material);
            payload = Slice::from(sealed);
            written.encryption_provider = encryption_.primary;
            written.encryption_metadata = std::move(created->metadata);
        }

        const Status status = store.put(name, payload).get();
        if (status == Status::Ok) return written;
        if (status != Status::Unusable) return std::unexpected(status);

        // Refresh the global counter from every tier. A collision is a numbering accident, and a
        // listing jumps past a whole run of crash residue without guessing how long that run is.
        for (const ListResult& names : list_all_stores()) {
            if (!names) {
                return std::unexpected(names.error() == Status::NotFound ? Status::Io
                                                                         : names.error());
            }
            for (const std::string& existing : *names) {
                if (auto number = sst_file_number(existing)) versions_->observe_file_number(*number);
            }
        }
    }

    return std::unexpected(Status::Unusable);
}

/// See `VersionSet::manifest_advanced`. On a path that has already failed, so its cost is moot.
bool DbImpl::manifest_has_advanced() const { return versions_->manifest_advanced(); }

/// Relabels a failed file read on a read-only instance when the cause is that this instance is
/// simply behind.
///
/// A file this version references can go missing for two reasons with opposite remedies: the writer
/// collected it because we are older than its retention window, or it is genuinely gone. The
/// manifest says which — see `VersionSet::manifest_advanced` — and getting it backwards sends an
/// operator to a restore for a healthy store. A writable instance is never behind its own manifest,
/// so this only ever fires for a reader.
Status DbImpl::classify_read_failure(Status status) const {
    if (!read_only_) return status;
    if (status != Status::NotFound && status != Status::Corrupt) return status;
    return manifest_has_advanced() ? Status::Stale : status;
}

Status DbImpl::sweep_orphans() {
    if (read_only_ || !options_.orphan_sweep_interval.has_value()) return Status::Ok;

    // Declared before the guard, so both destruct after it and the sink never runs under a lock.
    DeferredLine fence_line(this);
    DeferredLine reclaimed_line(this);

    // One sweep at a time. In production only the maintenance executor calls this, so the lock is
    // never contended — it is here because `sweep_orphans_for_test` lets a test call it too, and a
    // hook that quietly corrupts `orphan_first_seen_` when used alongside a running engine is a
    // trap rather than a hook.
    std::lock_guard<std::mutex> sweeping(sweep_mutex_);
    ELYSIUMKV_LOCK_AUDIT();

    // Re-read the manifest before deciding anything. This is what turns a repeated observation
    // into a sustained one: a file whose edit committed since the last sweep is referenced now and
    // leaves the candidate set on its own, rather than being argued about. It is also a fence
    // detector — a pointer that moved under us means another writer owns this store.
    //
    // Through `manifest_advanced`, which compares under the manifest mutex. Reading our own
    // generation and then reading the pointer as two separate steps is not the same check: this
    // writer rolls its own generation between them often enough — a sweep overlapping a compaction
    // is the ordinary case — and the two would then differ for a reason that has nothing to do with
    // another writer. That fenced the instance against itself, permanently, and it showed up as a
    // slow CI runner failing a test that passes locally because the sweep never overlapped anything.
    if (versions_->manifest_advanced()) {
        versions_->mark_fenced();
        fence_line.set(LogLevel::Error, LogEvent::Fenced,
                       "fenced: the manifest moved under this writer, so another process owns "
                       "the store");
        return Status::Fenced;
    }

    const std::set<uint64_t> referenced = versions_->referenced_file_numbers();
    const std::set<uint64_t> pending = versions_->pending_file_numbers();
    const uint64_t now = now_ms();
    const auto retention = static_cast<uint64_t>(options_.orphan_retention.count());

    size_t reclaimed = 0;
    std::vector<ListResult> listings = list_all_stores();
    size_t listed = 0;
    for (const auto& [store_id, store] : tiers_.stores) {
        ListResult& names = listings[listed++];
        if (!names) {
            // Failure to look is not evidence of absence, and this is the most destructive
            // possible place to forget that: treating an unreadable store as "everything here is
            // unreferenced" would delete the store.
            return names.error() == Status::NotFound ? Status::Io : names.error();
        }

        std::map<std::string, uint64_t>& seen = orphan_first_seen_[store_id];
        std::map<std::string, uint64_t> still_present;
        std::vector<std::string> collectable;

        for (const std::string& name : *names) {
            const std::optional<uint64_t> number = sst_file_number(name);
            if (!number) continue;                               // not ours to reason about
            if (referenced.count(*number) != 0) continue;        // live
            if (pending.count(*number) != 0) continue;           // has a window of its own

            const auto previous = seen.find(name);
            const uint64_t first_seen = previous == seen.end() ? now : previous->second;
            if (now >= first_seen && now - first_seen >= retention) {
                collectable.push_back(name);
            } else {
                still_present.emplace(name, first_seen);
            }
        }
        // Anything that became referenced, or vanished, drops out by not being carried over.
        seen = std::move(still_present);

        if (!collectable.empty()) {
            (void)store->remove_many(collectable).get();
            reclaimed += collectable.size();
        }
    }
    reclaimed += sweep_stale_generations(now);

    // One line per sweep rather than one per store: the number an operator watches is how much
    // this store is leaking, and a sweep that reclaimed nothing is the ordinary case and silent.
    if (reclaimed != 0) {
        reclaimed_line.set(LogLevel::Info, LogEvent::OrphansReclaimed, "reclaimed ", reclaimed,
                           " object(s) unreferenced for at least ", retention, " ms");
    }
    return Status::Ok;
}

/// Nothing else reclaims a manifest generation. A crash before or after the pointer CAS can leave
/// one on either side of the live generation, and an address collision may make the next roll jump
/// over it. CURRENT is authoritative about exactly one live generation; sustained inequality is
/// what makes every other one collectable after the reader window.
uint64_t DbImpl::sweep_stale_generations(uint64_t now) {
    const uint64_t live = versions_->generation();
    if (live == 0) return 0;

    auto listed = options_.manifest_catalog->list_generations().get();
    std::vector<uint64_t> candidates;
    if (listed) {
        for (const uint64_t generation : *listed) {
            if (generation != live) candidates.push_back(generation);
        }
    } else if (listed.error() == Status::Unsupported) {
        // The fallback, and its limit stated rather than discovered. Without a listing the only
        // way to find a generation is to ask for it, so this probes a short window below the live
        // one. That covers what a crash during a roll leaves — the previous generation — and
        // cannot see a leak from further back.
        constexpr uint64_t kProbeWindow = 4;
        for (uint64_t back = 1; back <= kProbeWindow && back <= live; ++back) {
            const uint64_t generation = live - back;
            if (generation == 0) break;
            if (options_.manifest_catalog->get_snapshot(generation).get().has_value()) {
                candidates.push_back(generation);
            }
        }
        for (uint64_t ahead = 1;
             ahead <= kProbeWindow && live <= std::numeric_limits<uint64_t>::max() - ahead;
             ++ahead) {
            const uint64_t generation = live + ahead;
            if (options_.manifest_catalog->get_snapshot(generation).get().has_value()) {
                candidates.push_back(generation);
            }
        }
    } else {
        return 0;   // failure to look is not evidence of absence
    }

    const auto retention = static_cast<uint64_t>(options_.obsolete_retention.value_or(Duration(0)).count());
    std::map<uint64_t, uint64_t> still_present;
    uint64_t reclaimed = 0;
    for (const uint64_t generation : candidates) {
        const auto previous = stale_generation_first_seen_.find(generation);
        const uint64_t first_seen = previous == stale_generation_first_seen_.end() ? now
                                                                                  : previous->second;
        if (now >= first_seen && now - first_seen >= retention) {
            if (options_.manifest_catalog->delete_generation(generation).get() == Status::Ok) {
                ++reclaimed;
            }
        } else {
            still_present.emplace(generation, first_seen);
        }
    }
    stale_generation_first_seen_ = std::move(still_present);
    return reclaimed;
}

/// Every store's full listing, in `tiers_.stores` order.
///
/// One thread per store, because the seam is not enough. Every `BlobStore` here completes its
/// future synchronously, so issuing the listings before collecting them runs them one after another
/// exactly as a plain loop does. Against object storage each is a round trip, and an instance with a
/// hot tier and a cold one pays both in series for no reason. The calling thread takes the last, so
/// a single-store instance spawns nothing.
std::vector<ListResult> DbImpl::list_all_stores() const {
    std::vector<const std::shared_ptr<BlobStore>*> stores;
    stores.reserve(tiers_.stores.size());
    for (const auto& [store_id, store] : tiers_.stores) stores.push_back(&store);

    // Built in place rather than fill-constructed from one prototype: the fill copies an
    // `expected` whose error arm is active, which gcc 13's `-Wmaybe-uninitialized` reads as a read
    // of the uninitialised vector arm. `Io` and not a default-constructed empty listing, so a slot
    // nothing writes reads as "could not look" rather than "this store is empty" — the difference
    // between failing open and discarding a tier.
    std::vector<ListResult> results;
    results.reserve(stores.size());
    for (size_t i = 0; i < stores.size(); ++i) results.emplace_back(std::unexpected(Status::Io));
    if (stores.empty()) return results;

    std::vector<std::thread> listers;
    listers.reserve(stores.size() - 1);
    for (size_t i = 0; i + 1 < stores.size(); ++i) {
        listers.emplace_back([&results, &stores, i] {
            results[i] = authoritative_store(**stores[i]).bulk_view().list("").get();
        });
    }
    const size_t last = stores.size() - 1;
    results[last] = authoritative_store(**stores[last]).bulk_view().list("").get();
    for (std::thread& lister : listers) lister.join();
    return results;
}

Status DbImpl::verify_stores_and_discard() {
    auto version = versions_->current();

    // One list per distinct store, against bulk_view(), never a get per file.
    // Caches are never consulted: a file present in a cache but absent from its
    // authoritative store counts as missing.
    std::vector<ListResult> results = list_all_stores();

    std::map<std::string, std::vector<std::string>> listings;
    size_t listed = 0;
    for (const auto& [store_id, store] : tiers_.stores) {
        ListResult& names = results[listed++];
        if (!names) {
            // ARCHITECTURE.md "A tier is not a level" — failure to look is not evidence of absence. Fail open, with
            // a retryable error and no manifest write.
            last_error_ = "store '" + store_id + "' could not be listed: " +
                          std::string(status_name(names.error()));
            return names.error() == Status::NotFound ? Status::Io : names.error();
        }

        // Step over what the store already holds. Anything here at or above our counter
        // is either a dead writer's residue — an object whose edit never became durable, so
        // recovery handed its number back — or a live writer's work. We cannot tell those
        // apart, and do not need to: stepping over both is free, and it is what removes the
        // engine's need to delete anything at open.
        for (const std::string& name : *names) {
            if (auto number = sst_file_number(name)) versions_->observe_file_number(*number);
        }
        listings.emplace(store_id, std::move(*names));
    }

    // Indexed, not scanned. Both sizes here are "files in this store", so a linear lookup per
    // file made open quadratic — at 10,000 files that is ~10^8 string comparisons before the store
    // opens, on a path an embedder runs at every rebalance.
    //
    // A set rather than a binary search over the listing: `BlobStore::list` promises nothing about
    // ordering, and an embedder's own store reaches this through the C ABI vtable. Assuming sorted
    // input would turn an unsorted store into "these files are missing", and for a Transient store
    // that discards every file on it.
    std::map<std::string, std::unordered_set<std::string_view>> present;
    for (const auto& [store_id, names] : listings) {
        auto& index = present[store_id];
        index.reserve(names.size());
        // Views into `listings`, which outlives this function.
        for (const std::string& name : names) index.emplace(name);
    }

    std::map<std::string, std::vector<FileMetadata>> missing_by_store;
    for (const auto& level : version->levels()) {
      for (const FileMetadata& file : level) {
        auto listing = present.find(file.store_id);
        if (listing == present.end()) {
            return fail_terminal(Status::Config,
                                 "file " + sst_object_name(file.file_number) +
                                     " names store '" + file.store_id +
                                     "', which is not in this configuration");
        }
        const std::string name = sst_object_name(file.file_number);
        if (!listing->second.contains(name)) {
            missing_by_store[file.store_id].push_back(file);
        }
      }
    }

    for (const auto& [store_id, missing] : missing_by_store) {
        // A store is discardable only if every tier naming it is Transient.
        const bool transient = tiers_.store_is_discardable(store_id);

        if (read_only_) {
            // A reader reports and refuses. The discard is a manifest write, and serving the
            // version unrepaired is worse than refusing: dropping newer files uncovers older values,
            // so reads would return *stale* data presented as current. A reader is the wrong process
            // to improvise with a damaged store — let the writer perform the discard first.
            // Corrupt either way: a reader can repair neither. A durable store missing files is
            // damaged, and a transient one needs the writer to discard and replay, which is not a
            // reader's manifest write to make.
            return fail_terminal(Status::Corrupt,
                                 "read-only open found store '" + store_id +
                                     "' missing files; a writer must discard and repair it first");
        }
        if (!transient) {
            return fail_terminal(Status::Corrupt,
                                 "file " + sst_object_name(missing.front().file_number) +
                                     " is missing from durable store '" + store_id + "'");
        }

        // Loss is per store: drop every file on it, wherever it sits. Tier
        // and level are independent, so those files are scattered across levels
        // rather than forming a contiguous band — which is why whole-store is
        // the only sane granularity here, not merely the preferred one.
        VersionEdit edit;
        uint64_t dropped = 0;
        for (const FileMetadata& file : version->all_files()) {
            if (file.store_id != store_id) continue;
            edit.deleted.push_back({file.level, file.file_number});
            ++dropped;
            // Accumulated here rather than recomputed later, because after the edit is applied
            // these files are in no version and their bounds are gone with them. An earlier
            // draft of this rule said "min over files on a transient tier", which is empty at
            // recovery time — precisely because those files are what was lost.
            recovery_watermark_.observe_discarded(file.watermark);
        }

        // The one number the loss produced, written down in the edit that erases its evidence.
        // After this edit the discarded files are gone from the manifest, so the next open has no
        // lost set to reason from and would fall back to `max(high)` over survivors — which is
        // sound only while nothing has ever been lost. See `Version::watermark_floor`.
        //
        // Recorded even when the loss certifies *nothing* — a discarded file with no lower bound
        // at all. Leaving that unrecorded would be indistinguishable from "no loss has happened",
        // which is exactly the state that sends the next open back to trusting `max(high)`.
        WatermarkFloor floor = versions_->current()->watermark_floor().value_or(
            WatermarkFloor{recovery_watermark_.discarded_lower_bound()});
        floor.lower_to(recovery_watermark_.discarded_lower_bound());
        edit.floor_update = VersionEdit::FloorUpdate::Set;
        edit.watermark_floor = floor;
        if (Status status = versions_->apply(std::move(edit)); status != Status::Ok) return status;

        discarded_stores_.push_back(store_id);
        discarded_files_ += dropped;
        log_event(LogLevel::Warn, LogEvent::StoresDiscarded, "transient store '", store_id,
                  "' did not survive: dropped ", dropped,
                  " file(s); the store is behind until the caller replays");
        // After a discard the store is *wrong*, not merely incomplete: a key whose newer value
        // lived here now reads as its older one — so reads are refused until the embedder replays
        // the gap, unless it has asked for them with `allow_reads_before_recovery`.
        requires_recovery_.store(true);
        version = versions_->current();
    }

    // Nothing is reclaimed here, and there is no flag to turn it on. Deleting on a single
    // instantaneous observation cannot tell a dead writer's residue from a live writer's
    // just-committed file — open takes no lock and performs no compare-and-set — and that is not a
    // defaulting problem, it is the observation being too weak to support the conclusion. The
    // *sustained* observation that can support it lives in `sweep_orphans`, which re-reads the
    // manifest so a file whose edit has since landed leaves the candidate set on its own. The
    // counter advance above is what makes not deleting safe in the meantime.
    return Status::Ok;
}

}  // namespace elysiumkv
