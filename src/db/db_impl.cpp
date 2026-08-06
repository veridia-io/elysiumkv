#include "db/db_impl.hpp"

#include "sst/format.hpp"

#include "compact/merging_iterator.hpp"
#include "compact/picker.hpp"
#include "sst/sst_writer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
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
               std::string upper, bool has_upper)
        : merged_(std::move(merged)),
          version_(std::move(version)),
          memtables_(std::move(memtables)),
          readers_(std::move(readers)),
          lower_(std::move(lower)),
          upper_(std::move(upper)),
          has_upper_(has_upper) {}

    bool next() override {
        if (!started_) {
            started_ = true;
            if (lower_.empty()) {
                merged_->seek_to_first();
            } else {
                merged_->seek(Slice::from(lower_));
            }
        } else if (merged_->valid()) {
            merged_->next();
        }

        // The merge yields one entry per key, newest first; a tombstone here
        // means the key is deleted, so the public iterator skips it entirely.
        while (merged_->valid()) {
            if (has_upper_ && !(merged_->key() < Slice::from(upper_))) {
                // Leaving the range ends the scan — a prefix iterator must never
                // degrade into a full keyspace scan (ARCHITECTURE.md "Absence is an answer, not an error").
                return false;
            }
            if (merged_->type() == ValueType::Put) return true;
            merged_->next();
        }
        return false;
    }

    Slice key() const override { return merged_->key(); }
    Slice value() const override { return merged_->value(); }
    Status status() const override { return merged_->status(); }

private:
    std::unique_ptr<InternalIterator> merged_;
    /// Pinned for the iterator's lifetime; releasing it is what finally allows a
    /// compacted-away file to be unlinked (ARCHITECTURE.md "Versions are immutable snapshots").
    std::shared_ptr<const Version> version_;
    std::vector<std::shared_ptr<SkiplistMemtable>> memtables_;
    std::vector<std::shared_ptr<SstReader>> readers_;
    std::string lower_;
    std::string upper_;
    bool has_upper_ = false;
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
    ops_.push_back({false, key.to_string(), value.to_string()});
    return *this;
}

WriteBatch& WriteBatch::remove(Slice key) {
    ops_.push_back({true, key.to_string(), {}});
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
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        shutting_down_ = true;
    }
    {
        std::lock_guard<std::mutex> lock(compaction_mutex_);
        shutting_down_compaction_ = true;
    }
    // The coordinator goes first: it is the only thing that hands the executors new work, so
    // stopping it before joining them keeps shutdown from racing a dispatch.
    {
        std::lock_guard<std::mutex> lock(maintenance_mutex_);
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

Result<OpenResult> DbImpl::open(const Options& options, bool require_all_durable,
                                bool read_only) {
    auto config = resolve_levels(options.levels);
    if (!config) return std::unexpected(config.error());
    auto tiers = resolve_tiers(options.tiers);
    if (!tiers) return std::unexpected(tiers.error());
    if (options.manifest_catalog == nullptr) return std::unexpected(Status::Config);

    // ARCHITECTURE.md "A tier is not a level" — `open` is guarded rather than merely documented. Adding a Transient
    // tier later must not leave existing call sites compiling and silently
    // serving stale values.
    if (require_all_durable && tiers->any_transient()) return std::unexpected(Status::Config);

    // **A `Transient` tier needs at least two levels.** An L0 file cannot be migrated — that
    // would reorder L0's positional recency — so it leaves its tier by being compacted into L1,
    // and `compact_l0_file_off_its_tier` gives up when there is no L1 to compact into. With one
    // level that is permanent exposure: L0 files can never leave the transient tier, no timer
    // helps, and the stall valve eventually holds every write — a store that is neither durable
    // nor writable. Rejected here, so a silent livelock is a configuration error instead.
    if (tiers->any_transient() && config->last() < 1) return std::unexpected(Status::Config);

    // **The orphan window must be at least the reader window.** An obsolete object is, to the
    // sweep, indistinguishable from an orphan — the edit that removed it is committed, so the
    // current manifest does not reference it, which is the sweep's own test. The pending queue keeps
    // the sweep off objects this instance obsoleted, but **a crash empties that queue**: an object
    // obsoleted before the crash comes back as an orphan afterwards, protected by the orphan window
    // and nothing else. Ordered the other way, the reader window would be silently inert after any
    // restart. Checked rather than documented, like every other bound here.
    if (options.obsolete_retention.has_value() &&
        options.orphan_retention < *options.obsolete_retention) {
        return std::unexpected(Status::Config);
    }

    std::unique_ptr<DbImpl> db(new DbImpl(options, std::move(*config), std::move(*tiers)));
    db->read_only_ = read_only;
    if (Status status = db->recover(); status != Status::Ok) return std::unexpected(status);
    db->start_background();

    OpenResult result;
    result.discarded_stores = db->discarded_stores_;
    result.discarded_files = db->discarded_files_;
    result.requires_recovery = db->requires_recovery_.load();
    result.db = std::move(db);
    return result;
}

const Tier& DbImpl::tier_for(uint64_t min_write_time_ms) const {
    const int index = placement(tiers_, min_write_time_ms, options_.clock());
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
    versions_ = std::make_unique<VersionSet>(
        *options_.manifest_catalog, options_.manifest_edits_per_generation,
        [this](const std::vector<FileMetadata>& files) { return delete_obsolete(files); },
        [this] { return now_ms(); },
        options_.obsolete_retention.value_or(Duration(0)));

    const Status status = versions_->recover();
    if (status == Status::NotFound && read_only_) {
        // **A reader does not create a store.** Finding no manifest means it opened the wrong place
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
        for (const auto& [store_id, store] : tiers_.stores) {
            auto names = authoritative_store(*store).bulk_view().list("").get();
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
    // **A reader runs no background work at all**: no flush, no compaction, no migration, no
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

    // **One synchronous reconcile before the first write can arrive.** A store reopened already
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
        if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        mem_->put(key, value);
    }
    return maybe_freeze_memtable(false);
}

Status DbImpl::remove(Slice key) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    if (Status status = check_entry_size(key, Slice()); status != Status::Ok) return status;
    if (Status status = throttle_writes(); status != Status::Ok) return status;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        mem_->remove(key);
    }
    return maybe_freeze_memtable(false);
}

Status DbImpl::write(WriteBatch& batch) {
    if (read_only_) return Status::Config;   // the C ABI has one handle type; C++ has two
    // Checked in full before anything is applied: ARCHITECTURE.md "Absence is an answer, not an error" says a batch lands as a
    // unit, so discovering an oversized entry halfway through would leave the
    // store holding half of it.
    for (const WriteBatch::Op& op : batch.ops()) {
        const Status status = check_entry_size(Slice::from(op.key),
                                               op.is_delete ? Slice() : Slice::from(op.value));
        if (status != Status::Ok) return status;
    }
    if (Status status = throttle_writes(); status != Status::Ok) return status;
    {
        // Applied as a unit: the freeze decision is taken at batch boundaries
        // only, so a flush never splits a batch across two memtables.
        std::lock_guard<std::mutex> lock(mem_mutex_);
        if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;
        for (const WriteBatch::Op& op : batch.ops()) {
            if (op.is_delete) {
                mem_->remove(Slice::from(op.key));
            } else {
                mem_->put(Slice::from(op.key), Slice::from(op.value));
            }
        }
    }
    return maybe_freeze_memtable(false);
}

bool DbImpl::run_one_flush(Status& status) {
    std::shared_ptr<SkiplistMemtable> pending;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        if (imm_ == nullptr) return false;
        pending = imm_;
    }

    status = flush_memtable(pending);

    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
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
    if (mem_->num_entries() == 0) return false;

    const uint64_t now = now_ms();
    const uint64_t born = mem_->creation_time_ms();
    if (now <= born) return false;   // a clock that went backwards is not evidence of age
    return now - born >= static_cast<uint64_t>(options_.flush_interval->count());
}

Status DbImpl::freeze_and_flush_inline(bool force) {
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        if (imm_ == nullptr) {
            if (!memtable_flush_due(force)) return Status::Ok;
            if (mem_->num_entries() == 0) return Status::Ok;
            imm_ = std::move(mem_);
            mem_ = new_memtable();
        }
        // A frozen memtable left over from a failed flush is retried here: Io
        // means "ask again later", and inline mode has nowhere else to ask.
        if (is_retryable(bg_error_)) bg_error_ = Status::Ok;
    }

    Status status = Status::Ok;
    while (run_one_flush(status)) {
    }
    if (status != Status::Ok) return status;

    // Inline mode: the writer is also the compactor, so the flush is not done
    // until the compaction it just caused is done too.
    return compact_until_quiet();
}

Status DbImpl::maybe_freeze_memtable(bool force) {
    if (inline_mode()) return freeze_and_flush_inline(force);

    std::unique_lock<std::mutex> lock(mem_mutex_);
    if (!memtable_flush_due(force)) return Status::Ok;
    if (mem_->num_entries() == 0 && imm_ == nullptr) return Status::Ok;

    // Backpressure: one memtable may be in flight. Without a WAL there is
    // nowhere else to put writes, so the writer waits.
    bool retried = false;
    while (imm_ != nullptr && !shutting_down_) {
        if (bg_error_ != Status::Ok) {
            // Io means "ask again later" (ARCHITECTURE.md "Immutable named objects"), so a failed flush must not
            // leave the instance permanently unable to flush. One retry per
            // call; if it fails again the caller hears about it.
            if (!is_retryable(bg_error_) || retried) return bg_error_;
            bg_error_ = Status::Ok;
            retried = true;
            flush_scheduled_.notify_one();
        }
        if (!options_.block_on_stall) {
            stalls_.fetch_add(1, std::memory_order_relaxed);
            return Status::Stalled;
        }
        stalls_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t stall_began = options_.clock();
        flush_finished_.wait(lock);
        const uint64_t stall_ended = options_.clock();
        if (stall_ended > stall_began) {
            stalled_total_ms_.fetch_add(stall_ended - stall_began, std::memory_order_relaxed);
        }
    }
    if (mem_->num_entries() == 0) return Status::Ok;

    imm_ = std::move(mem_);
    mem_ = new_memtable();
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
    while (imm_ != nullptr && bg_error_ == Status::Ok && !shutting_down_) {
        flush_finished_.wait(lock);
    }
    return imm_ == nullptr ? Status::Ok : bg_error_;
}

/// The flush executor: memtable flushing and nothing else.
///
/// **It has its own thread for write availability.** One immutable memtable is allowed in
/// flight, so if flushing shared a worker with compaction, a memtable filling while a
/// compaction ran would block the next rotation and stall writes for the length of that
/// compaction — up to `max_compaction_bytes` over throughput.
///
/// It evaluates no predicates. Deciding *whether* a flush is due belongs to the coordinator,
/// which also performs the rotation: a pointer swap under `mem_mutex_` is not long-running work,
/// and doing it there means the age-driven flush is one entry in the shared predicate table
/// instead of this loop's own private timer. `flush_interval` was the first instance of the
/// push-based pattern in this engine, and leaving it on separate machinery is how one of two
/// adjacent loops ends up with a bug the other does not have.
void DbImpl::background_flush_loop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mem_mutex_);
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

/// How often the coordinator evaluates every predicate **regardless of the gate**.
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
    if (options_.orphan_sweep_interval.has_value()) consider(next_sweep_ms_);

    for (const FileMetadata& file : version.all_files()) {
        const int index = tiers_.tier_of_store(file.store_id);
        if (index < 0) continue;
        const Tier& tier = tiers_.tiers[static_cast<size_t>(index)];
        // Placement is monotone in age, so the next time this file's placement can change is
        // when it outgrows the tier it is on. Size mismatches are not time-driven — they are
        // true the moment the file exists, and the epoch covers them.
        if (tier.max_age.has_value()) {
            consider(file.min_write_time_ms + static_cast<uint64_t>(tier.max_age->count()));
        }
        // The stall valve is a published predicate, so its crossing has to open the gate too or
        // the flag would go stale on a store that has stopped being written to.
        if (tier.durability == Durability::Transient && tier.stall_age.has_value()) {
            consider(file.min_write_time_ms + static_cast<uint64_t>(tier.stall_age->count()));
        }
    }
    return earliest;
}

void DbImpl::publish_transient_stall(const Version& version, uint64_t now) {
    if (pinned_transient_stall_.load() >= 0) return;   // negative control; see the header

    bool stalled = false;
    for (const Tier& tier : tiers_.tiers) {
        if (tier.durability != Durability::Transient || !tier.stall_age.has_value()) continue;
        uint64_t oldest = 0;
        for (const FileMetadata& file : version.all_files()) {
            if (file.store_id != tier.store->id()) continue;
            if (oldest == 0 || file.min_write_time_ms < oldest) oldest = file.min_write_time_ms;
        }
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
        // `imm_ != nullptr` means the flush executor is already busy; there is nothing to hand
        // it, and waiting for it here would be the coordinator doing long-running work.
        if (!shutting_down_ && imm_ == nullptr && bg_error_ == Status::Ok && mem_ != nullptr &&
            mem_->num_entries() > 0 && memtable_flush_due(false)) {
            imm_ = std::move(mem_);
            mem_ = new_memtable();
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
        {
            std::unique_lock<std::mutex> lock(maintenance_mutex_);
            if (shutting_down_maintenance_) return;
            // **Always bounded.** Waiting untimed when no tier has a `max_age` was the first
            // draft's shape and it reintroduces the defect: a tier crossing `max_bytes`, a
            // level's score passing 1, a version becoming collectible — none of those has an
            // age bound, so an indefinite wait plus a missed notification is a task that never
            // runs. A periodic wake with the gate closed costs two comparisons.
            maintenance_tick_.wait_for(lock, options_.maintenance_interval);
            if (shutting_down_maintenance_) return;
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

    auto built = build_sst(*source, sst_options);
    if (!built) return built.error();
    if (built->num_entries == 0) return Status::Ok;

    // ARCHITECTURE.md "A tier is not a level" — placement from the memtable's age alone.
    const Tier& tier = tier_for(memtable->creation_time_ms());

    auto file_number = write_new_sst(*tier.store, Slice::from(built->bytes));
    if (!file_number) return file_number.error();

    FileMetadata file;
    file.level = 0;
    file.file_number = *file_number;
    file.store_id = tier.store->id();
    file.smallest_key = built->smallest_key;
    file.largest_key = built->largest_key;
    file.file_bytes = built->bytes.size();
    file.num_entries = built->num_entries;
    file.num_tombstones = built->num_tombstones;
    file.compression = level.compression;
    // A flushed L0 file inherits its memtable's creation time (ARCHITECTURE.md "The manifest is snapshots plus edits"); this is
    // the only place the value originates.
    file.min_write_time_ms = memtable->creation_time_ms();
    // **The sealed memtable's interval, not the interval current when the file is written.** The
    // dangerous variant is the latter: a watermark set after this memtable was sealed covers
    // writes still sitting in the live one, so reporting it would tell the embedder to skip
    // replaying data the engine never stored. Flushes are ordered — one immutable memtable at a
    // time, so seal order is flush order — so by the time this file is in the manifest, every
    // earlier memtable's file already is.
    file.watermark = memtable->watermark();

    VersionEdit edit;
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

    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        const auto& files = version->files_at(level);

        for (size_t i = 0; i < files.size(); ++i) {
            const FileMetadata& file = files[i];

            // Every file in the current version exists in its recorded store.
            auto listing = listings.find(file.store_id);
            if (listing == listings.end()) return fail(Invariant::StoreMissing);
            const std::string name = sst_object_name(file.file_number);
            if (std::find(listing->second.begin(), listing->second.end(), name) ==
                listing->second.end()) {
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

Result<std::shared_ptr<SstReader>> DbImpl::reader_for(const FileMetadata& file) {
    if (auto resident = readers_.get(file.file_number)) return resident;

    BlobStore* store = store_for(file.store_id);
    if (store == nullptr) return std::unexpected(Status::Corrupt);

    SstReaderOptions reader_options;
    reader_options.block_bytes = options_.block_bytes;
    reader_options.file_number = file.file_number;
    reader_options.block_cache = block_cache_.get();

    auto reader = SstReader::open(*store, sst_object_name(file.file_number), file.file_bytes,
                                  reader_options);
    if (!reader) {
        if (reader.error() == Status::NotFound && read_only_) {
            // **Staleness is not corruption, and telling them apart needs no coordination.**
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
    // **Grouped by store, one bulk call each.** A level's files routinely sit on
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

Result<Pinned> DbImpl::get(Slice key) {
    std::shared_ptr<SkiplistMemtable> mem;
    std::shared_ptr<SkiplistMemtable> imm;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        mem = mem_;
        imm = imm_;
    }

    for (const auto& memtable : {mem, imm}) {
        if (memtable == nullptr) continue;
        if (auto entry = memtable->get(key)) {
            if (entry->type == ValueType::Delete) return std::unexpected(Status::NotFound);
            // The keep-alive is the memtable itself: its arena owns the bytes.
            return Pinned(memtable, entry->value, &pins_outstanding_);
        }
    }

    auto version = versions_->current();
    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        // L0 is ordered by descending file number, so the first hit is the most
        // recent; deeper levels are non-overlapping, so there is at most one.
        for (const FileMetadata& file : version->files_at(level)) {
            if (!file_may_contain(file, key)) continue;

            auto reader = reader_for(file);
            if (!reader) return std::unexpected(reader.error());

            auto found = (*reader)->get(key);
            if (!found) return std::unexpected(classify_read_failure(found.error()));
            if (!found->has_value()) continue;
            if ((*found)->type == ValueType::Delete) return std::unexpected(Status::NotFound);
            return Pinned((*found)->block, (*found)->value, &pins_outstanding_);
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

std::unique_ptr<Iterator> DbImpl::make_iterator(Slice lower, Slice upper, bool has_upper) {
    std::shared_ptr<SkiplistMemtable> mem;
    std::shared_ptr<SkiplistMemtable> imm;
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        mem = mem_;
        imm = imm_;
    }
    auto version = versions_->current();

    std::vector<std::unique_ptr<InternalIterator>> children;
    std::vector<std::shared_ptr<SkiplistMemtable>> memtables;
    std::vector<std::shared_ptr<SstReader>> readers;

    for (const auto& memtable : {mem, imm}) {
        if (memtable == nullptr) continue;
        memtables.push_back(memtable);
        children.push_back(memtable->ascending());
    }

    for (int level = 0; level < static_cast<int>(version->num_levels()); ++level) {
        // Prune by key range before opening anything: this is what keeps a
        // prefix scan from touching files it cannot contain (ARCHITECTURE.md "Absence is an answer, not an error").
        for (const FileMetadata& file : version->overlapping_half_open(level, lower, upper)) {
            auto reader = reader_for(file);
            if (!reader) {
                children.push_back(std::make_unique<ErrorIterator>(reader.error()));
                continue;
            }
            readers.push_back(*reader);
            children.push_back((*reader)->iterator());
        }
    }

    return std::make_unique<DbIterator>(make_merging_iterator(std::move(children)),
                                        std::move(version), std::move(memtables),
                                        std::move(readers), lower.to_string(), upper.to_string(),
                                        has_upper);
}

std::unique_ptr<Iterator> DbImpl::iterator() { return make_iterator(Slice(), Slice(), false); }

std::unique_ptr<Iterator> DbImpl::iterator(Slice lower_inclusive) {
    return make_iterator(lower_inclusive, Slice(), /*has_upper=*/false);
}

std::unique_ptr<Iterator> DbImpl::iterator(Slice lower_inclusive, Slice upper_exclusive) {
    return make_iterator(lower_inclusive, upper_exclusive, true);
}

std::unique_ptr<Iterator> DbImpl::prefix_iterator(Slice prefix) {
    std::string upper;
    // An all-0xFF (or empty) prefix has no upper bound; the scan simply runs to
    // the end of the keyspace.
    const bool bounded = prefix_upper_bound(prefix, upper);
    return make_iterator(prefix, bounded ? Slice::from(upper) : Slice(), bounded);
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
    for (int index = 0; index <= tiers_.last(); ++index) {
        const Tier& tier = tiers_.tiers[static_cast<size_t>(index)];
        TierStats tier_stats;
        tier_stats.tier = index;

        uint64_t oldest_write = 0;
        for (const FileMetadata& file : version->all_files()) {
            if (file.store_id != tier.store->id()) continue;
            ++tier_stats.file_count;
            tier_stats.bytes += file.file_bytes;
            if (oldest_write == 0 || file.min_write_time_ms < oldest_write) {
                oldest_write = file.min_write_time_ms;
            }
            if (placement(tiers_, file.min_write_time_ms, now) > index) {
                ++tier_stats.files_pending_migration;
            }
        }
        if (tier.max_bytes.has_value() && tier_stats.bytes > *tier.max_bytes &&
            tier_stats.files_pending_migration == 0) {
            // Over capacity: eviction is pending even though nothing has aged out.
            tier_stats.files_pending_migration = tier_stats.file_count;
        }
        if (oldest_write != 0) {
            tier_stats.oldest_file_age = Duration(now > oldest_write ? now - oldest_write : 0);
            if (tier.durability == Durability::Transient && tier.stall_age.has_value() &&
                tier_stats.oldest_file_age > *tier.stall_age) {
                tier_stats.stalling = true;
            }
        }
        stats.tiers.push_back(tier_stats);
    }

    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        // Everything not yet in an SST, live and frozen alike: a frozen memtable
        // waiting on a flush is exactly as unwritten as the live one.
        uint64_t oldest_write = 0;
        for (const auto& memtable : {mem_, imm_}) {
            if (memtable == nullptr) continue;
            stats.memtable_bytes += memtable->approximate_bytes();
            stats.memtable_entries += memtable->num_entries();
            stats.memtable_tombstones += memtable->num_tombstones();
            if (memtable->num_entries() == 0) continue;
            const uint64_t created = memtable->creation_time_ms();
            if (oldest_write == 0 || created < oldest_write) oldest_write = created;
        }
        stats.memtable_age =
            Duration(oldest_write == 0 || now <= oldest_write ? 0 : now - oldest_write);
    }

    // The **live** frontier, and deliberately not the maximum watermark over current files: that
    // is tier-blind, so a flush to a transient tier would advance it while changing nothing an
    // operator can rely on. This is the same expression recovery uses, evaluated live — the
    // position whose state would survive losing every transient tier.
    {
        std::optional<uint64_t> transient_low;
        bool transient_low_missing = false;
        bool any_transient = false;
        std::optional<uint64_t> high;
        for (const FileMetadata& file : version->all_files()) {
            accumulate_max(high, file.watermark.high);
            if (!tiers_.store_is_discardable(file.store_id)) continue;
            any_transient = true;
            if (file.watermark.low.has_value()) {
                accumulate_min(transient_low, file.watermark.low);
            } else {
                transient_low_missing = true;
            }
        }
        if (!any_transient) {
            stats.durable_watermark = high;
        } else if (transient_low_missing) {
            // A transient file with no lower bound: losing it would certify nothing, so neither
            // does the gauge. Absent rather than zero, which is a valid position.
            stats.durable_watermark = std::nullopt;
        } else {
            stats.durable_watermark = transient_low;
        }
    }

    stats.requires_recovery = requires_recovery_.load();
    stats.flushes = flushes_.load();
    stats.compactions = compactions_.load();
    stats.compaction_bytes_read = compaction_bytes_read_.load();
    stats.compaction_bytes_written = compaction_bytes_written_.load();
    stats.migrations = migrations_.load();
    stats.migration_bytes = migration_bytes_.load();
    stats.stalled_total = Duration(stalled_total_ms_.load());
    stats.stall_count = stalls_.load();
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

// ARCHITECTURE.md "A process-wide memory budget" — **the shedding order, and why it is that order.** The budget is process-wide,
// so an instance over it is usually over because of its neighbours, not itself; the
// response has to be to give memory back rather than to fail.
//
//   1. The block cache. The only consumer whose loss is *purely* latency, and the
//      cheapest to release — bytes held as an optimisation and nothing else.
//   2. Memtables, by freezing and flushing. This costs I/O and it costs write
//      throughput, so it comes second; it cannot be first because a memtable holds
//      writes that are not anywhere else yet.
//   3. Stalling, which is the caller's problem and is handled by `throttle_writes`
//      below. Last, because it is the only one visible to the application.
//
// Nothing here fails a write. A budget is a shaping mechanism, not an admission
// control: refusing a `put` because another instance in the process is using memory
// would be a surprising and unhelpful failure mode.
/// One place that builds a memtable, so the budget and the clock cannot be attached in
/// two of the three places and forgotten in the third.
std::shared_ptr<SkiplistMemtable> DbImpl::new_memtable() {
    auto memtable = std::make_shared<SkiplistMemtable>();
    memtable->set_memory_budget(options_.memory_budget.get());
    memtable->set_creation_time_ms(now_ms());

    // **The watermark interval starts closed at whatever is established right now.** Both bounds,
    // and the lower one is the point: `set_watermark(M)` asserts every write so far is at a
    // position ≤ M, so a memtable created while M is current holds no write at or below M. That
    // is the lower bound the recovery proof needs, and capturing it here — at creation, not at
    // seal — is what makes it true. A later `set_watermark` on this memtable moves only the
    // upper bound; it says nothing about writes already in here.
    //
    // Absent when nothing has been established yet: such a memtable has no lower bound at all,
    // and losing it means nothing can be certified. That is why recovery reports `nullopt`
    // rather than zero, which is a valid position.
    //
    // **Call with `mem_mutex_` held** — it reads `established_watermark_`.
    if (established_watermark_.has_value()) {
        memtable->set_watermark_bounds({*established_watermark_, *established_watermark_});
    }
    return memtable;
}

Status DbImpl::set_watermark(uint64_t position) {
    if (read_only_) return Status::Config;
    if (unusable_.load()) return Status::Unusable;

    std::lock_guard<std::mutex> lock(mem_mutex_);
    if (bg_error_ != Status::Ok && is_terminal(bg_error_)) return bg_error_;

    // Taking the write path's own lock is what makes "every write completed before this call"
    // a well-defined set, and a store has one writer. The proof the recovery rule rests on has
    // no other premise; an embedder able to call this concurrently with a write would make the
    // boundary unsound rather than merely imprecise.
    if (established_watermark_.has_value() && position < *established_watermark_) {
        // A caller bug, refused rather than clamped: clamping would hide a replay that went
        // backwards, and the whole value of the watermark is that it can be trusted.
        return Status::Config;
    }
    established_watermark_ = position;
    if (mem_ != nullptr) {
        if (mem_->num_entries() == 0) {
            // **Empty, so both bounds move.** There are no writes for a lower bound to be wrong
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

    // 2. Flush the memtable — **but only if it holds enough to be worth an SST.**
    //
    // A flush trades memtable bytes for a file, and the file is permanent: it joins L0 and
    // carries compaction debt for the rest of the store's life. Flushing a memtable holding a
    // handful of entries releases almost nothing and buys all of that.
    //
    // It matters because a budget smaller than one memtable can never be satisfied by
    // flushing, so the unguarded version did it on *every write*: L0 filled with thousands of
    // near-empty files, compaction work grew with each one, and the cost per operation climbed
    // without bound. A 5,000-operation differential replay reached a single operation taking
    // over five minutes before the liveness watchdog killed it — a livelock in all but name,
    // and the full-profile run is what surfaced it.
    //
    // When there is nothing worth flushing, the overage is somewhere this step cannot reach —
    // a blob cache, another instance, or a budget simply set too low — and the honest next
    // step is the third one, which is the caller's rate.
    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
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

Status DbImpl::throttle_writes() {
    if (unusable_.load()) return Status::Unusable;

    // ARCHITECTURE.md "A process-wide memory budget" before the level and tier valves: a process over its memory budget has a
    // problem that no amount of compaction fixes, and the two cheapest remedies are
    // free of the compaction machinery entirely.
    //
    // **Stalling on the budget is bounded by progress, not by the budget clearing.** A
    // level stall clears when compaction catches up; a budget stall clears only if memory
    // is given back, and if the budget is simply set too low for the instances sharing it,
    // nothing ever will. Waiting for that is a hang, not backpressure — the Java binding,
    // which has no inline mode to fall back on, wedged on the first attempt at this. So
    // the writer waits only while the overage is *shrinking*: real backpressure when the
    // system can recover, and a bounded delay when it cannot.
    bool budget_stall = shed_if_over_budget();
    size_t budget_overage = budget_stall ? options_.memory_budget->overage() : 0;

    // **Every path that loops must run this**, or the budget stall never ends. It lived only
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
        auto version = versions_->current();
        const uint64_t now = now_ms();

        bool stop = false;
        bool slowdown = false;
        for (const ResolvedLevel& level : config_.levels) {
            const auto files = static_cast<int>(version->file_count(level.level));
            if (level.stop_at.has_value() && files >= *level.stop_at) stop = true;
            if (level.slowdown_at.has_value() && files >= *level.slowdown_at) slowdown = true;

        }

        // ARCHITECTURE.md "Migration between tiers" — the valve, now on the tier axis: it is what makes the exposure
        // bound a guarantee rather than an expectation, so it is not
        // configurable off.
        //
        // **Read, not computed.** The maintenance coordinator owns this predicate and publishes
        // the answer; evaluating it here as well would put the same definition in two places,
        // where it can diverge — which is how a valve ends up engaged by one and not the other.
        // It also makes the write path's cost O(1) instead of O(files). The cost is that
        // engaging lags by up to one tick, which is the same `+ interval` term the exposure
        // window already carries; `open` reconciles synchronously so the flag is never stale on
        // the first write.
        if (inline_mode()) {
            // No coordinator to publish it, so the writer that would otherwise wait for one
            // evaluates it itself — the same asymmetry `flush_interval` already documents.
            publish_transient_stall(*version, now);
        }
        if (transient_stalled_.load()) stop = true;

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
            auto after = versions_->current();
            if (after == version) return Status::Ok;  // nothing left to do
            continue;
        }

        stalls_.fetch_add(1, std::memory_order_relaxed);
        schedule_compaction();
        if (!options_.block_on_stall) return Status::Stalled;

        const uint64_t began = now_ms();
        {
            std::unique_lock<std::mutex> lock(compaction_mutex_);
            if (shutting_down_compaction_) return Status::Ok;
            compaction_finished_.wait_for(lock, std::chrono::milliseconds(50));
        }
        const uint64_t ended = now_ms();
        if (ended > began) stalled_total_ms_.fetch_add(ended - began, std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock(mem_mutex_);
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

Result<uint64_t> DbImpl::write_new_sst(BlobStore& store, Slice bytes) {
    // ARCHITECTURE.md "Immutable named objects" says it outright: "a failed `put` must not be retried under the same name —
    // allocate a new file number instead; the partial object becomes an orphan and is
    // collected". A taken name is a numbering accident, not a verdict on ownership.
    //
    // **Ownership is arbitrated at the manifest, which is the only place with a
    // compare-and-swap.** This used to report `Fenced` here instead, which made a crashed
    // writer's leftover object — sitting at exactly the number recovery hands back out —
    // permanently fatal: every reopen collided on the same name. Open stepping over what the
    // stores hold makes that rare, and renumbering makes it harmless when it happens anyway.
    constexpr int kAttempts = 4;
    for (int attempt = 0; attempt < kAttempts; ++attempt) {
        const uint64_t file_number = versions_->allocate_file_number();
        const std::string name = sst_object_name(file_number);

        const Status status = store.put(name, bytes).get();
        if (status == Status::Ok) return file_number;
        if (status != Status::Unusable) return std::unexpected(status);

        // Taken. Step past it and try the next number.
        versions_->observe_file_number(file_number);
    }

    // Four collisions in a row is not a numbering accident: something is allocating from the
    // same sequence we are, and the manifest will refuse us shortly. Say so here rather than
    // writing objects that can never be installed.
    versions_->mark_fenced();
    last_error_ = "object names are being taken as fast as they are allocated: another writer "
                  "owns this store";
    return std::unexpected(Status::Fenced);
}

void DbImpl::collect_orphans(const std::map<std::string, std::vector<std::string>>& listings) {
    auto version = versions_->current();
    // File numbers are never reused (ARCHITECTURE.md "The manifest is snapshots plus edits"), so a name identifies an object
    // outright — no store qualifier needed.
    std::set<std::string> referenced;
    for (const FileMetadata& file : version->all_files()) {
        referenced.insert(sst_object_name(file.file_number));
    }

    for (const auto& [store_id, names] : listings) {
        BlobStore* store = store_for(store_id);
        if (store == nullptr) continue;

        std::vector<std::string> orphans;
        for (const std::string& name : names) {
            if (!name.ends_with(".sst")) continue;
            if (referenced.count(name) != 0) continue;
            // Nothing references it: the residue of a flush or compaction that
            // died before its edit was durable (ARCHITECTURE.md "Open and recovery").
            orphans.push_back(name);
        }
        // One call per store, like collection after a compaction. A store that
        // crashed mid-compaction can have dozens of these, and open is exactly
        // when a round trip per object is least welcome.
        if (!orphans.empty()) (void)store->remove_many(orphans).get();
    }
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

    // One sweep at a time. In production only the maintenance executor calls this, so the lock is
    // never contended — it is here because `sweep_orphans_for_test` lets a test call it too, and a
    // hook that quietly corrupts `orphan_first_seen_` when used alongside a running engine is a
    // trap rather than a hook.
    std::lock_guard<std::mutex> sweeping(sweep_mutex_);

    // **Re-read the manifest before deciding anything.** This is what turns a repeated observation
    // into a sustained one: a file whose edit committed since the last sweep is referenced now and
    // leaves the candidate set on its own, rather than being argued about. It is also a fence
    // detector — a pointer that moved under us means another writer owns this store.
    //
    // **Through `manifest_advanced`, which compares under the manifest mutex.** Reading our own
    // generation and then reading the pointer as two separate steps is not the same check: this
    // writer rolls its own generation between them often enough — a sweep overlapping a compaction
    // is the ordinary case — and the two would then differ for a reason that has nothing to do with
    // another writer. That fenced the instance against itself, permanently, and it showed up as a
    // slow CI runner failing a test that passes locally because the sweep never overlapped anything.
    if (versions_->manifest_advanced()) {
        versions_->mark_fenced();
        return Status::Fenced;
    }

    const std::set<uint64_t> referenced = versions_->referenced_file_numbers();
    const std::set<uint64_t> pending = versions_->pending_file_numbers();
    const uint64_t now = now_ms();
    const auto retention = static_cast<uint64_t>(options_.orphan_retention.count());

    for (const auto& [store_id, store] : tiers_.stores) {
        auto names = authoritative_store(*store).bulk_view().list("").get();
        if (!names) {
            // **Failure to look is not evidence of absence**, and this is the most destructive
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
        }
    }
    return Status::Ok;
}

Status DbImpl::verify_stores_and_discard() {
    auto version = versions_->current();

    // One list per distinct store, against bulk_view(), never a get per file.
    // Caches are never consulted: a file present in a cache but absent from its
    // authoritative store counts as missing.
    std::map<std::string, std::vector<std::string>> listings;
    for (const auto& [store_id, store] : tiers_.stores) {
        auto names = authoritative_store(*store).bulk_view().list("").get();
        if (!names) {
            // ARCHITECTURE.md "A tier is not a level" — failure to look is not evidence of absence. Fail open, with
            // a retryable error and no manifest write.
            last_error_ = "store '" + store_id + "' could not be listed: " +
                          std::string(status_name(names.error()));
            return names.error() == Status::NotFound ? Status::Io : names.error();
        }

        // **Step over what the store already holds.** Anything here at or above our counter
        // is either a dead writer's residue — an object whose edit never became durable, so
        // recovery handed its number back — or a live writer's work. We cannot tell those
        // apart, and do not need to: stepping over both is free, and it is what removes the
        // engine's need to delete anything at open.
        for (const std::string& name : *names) {
            if (auto number = sst_file_number(name)) versions_->observe_file_number(*number);
        }
        listings.emplace(store_id, std::move(*names));
    }

    std::map<std::string, std::vector<FileMetadata>> missing_by_store;
    for (const FileMetadata& file : version->all_files()) {
        auto listing = listings.find(file.store_id);
        if (listing == listings.end()) {
            return fail_terminal(Status::Config,
                                 "file " + sst_object_name(file.file_number) +
                                     " names store '" + file.store_id +
                                     "', which is not in this configuration");
        }
        const std::string name = sst_object_name(file.file_number);
        if (std::find(listing->second.begin(), listing->second.end(), name) ==
            listing->second.end()) {
            missing_by_store[file.store_id].push_back(file);
        }
    }

    for (const auto& [store_id, missing] : missing_by_store) {
        // A store is discardable only if every tier naming it is Transient.
        const bool transient = tiers_.store_is_discardable(store_id);

        if (read_only_) {
            // **A reader reports and refuses.** The discard is a manifest write, and serving the
            // version unrepaired is worse than refusing: dropping newer files uncovers older values,
            // so reads would return *stale* data presented as current. A reader is the wrong process
            // to improvise with a damaged store — let the writer perform the discard first.
            return fail_terminal(transient ? Status::Corrupt : Status::Corrupt,
                                 "read-only open found store '" + store_id +
                                     "' missing files; a writer must discard and repair it first");
        }
        if (!transient) {
            return fail_terminal(Status::Corrupt,
                                 "file " + sst_object_name(missing.front().file_number) +
                                     " is missing from durable store '" + store_id + "'");
        }

        // Loss is per store: drop **every** file on it, wherever it sits. Tier
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
        if (Status status = versions_->apply(std::move(edit)); status != Status::Ok) return status;

        discarded_stores_.push_back(store_id);
        discarded_files_ += dropped;
        // After a discard the store is *wrong*, not merely incomplete: a key
        // whose newer value lived here now reads as its older one. The engine
        // reports; it does not enforce read blocking.
        requires_recovery_.store(true);
        version = versions_->current();
    }

    // **Nothing is reclaimed here, and there is no flag to turn it on.** Deleting on a single
    // instantaneous observation cannot tell a dead writer's residue from a live writer's
    // just-committed file — open takes no lock and performs no compare-and-set — and that is not a
    // defaulting problem, it is the observation being too weak to support the conclusion. The
    // *sustained* observation that can support it lives in `sweep_orphans`, which re-reads the
    // manifest so a file whose edit has since landed leaves the candidate set on its own. The
    // counter advance above is what makes not deleting safe in the meantime.
    (void)listings;
    return Status::Ok;
}

}  // namespace elysiumkv
