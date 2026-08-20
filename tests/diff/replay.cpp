#include "diff/replay.hpp"

#include "diff/oracle.hpp"
#include "blob/tier.hpp"
#include "db/db_impl.hpp"
#include "support/test_db.hpp"
#include "support/watchdog.hpp"
#include "elysiumkv/memory_budget.hpp"
#include "elysiumkv/db.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>

namespace elysiumkv::test {
namespace {

using Entries = std::vector<std::pair<std::string, std::string>>;

std::string quote(const std::string& text) { return "\"" + text + "\""; }

Entries oracle_entries(const Oracle& oracle) {
    Entries entries;
    for (const auto& entry : oracle.entries()) entries.push_back(entry);
    return entries;
}

/// Names the first difference even when the counts differ. Reporting only "returned
/// 30, expected 31" costs an afternoon: the interesting question is always *which* key,
/// because that identifies the file and the code path that lost it.
std::string describe_mismatch(const Entries& observed, const Entries& expected) {
    const size_t common = std::min(observed.size(), expected.size());
    for (size_t i = 0; i < common; ++i) {
        if (observed[i] == expected[i]) continue;
        std::string out = "scan differs at position " + std::to_string(i) + ": got " +
                          quote(observed[i].first) + " -> " + quote(observed[i].second) +
                          ", expected " + quote(expected[i].first) + " -> " +
                          quote(expected[i].second);
        if (observed.size() != expected.size()) {
            out += " (returned " + std::to_string(observed.size()) + " entries, expected " +
                   std::to_string(expected.size()) + ")";
        }
        return out;
    }
    if (observed.size() == expected.size()) return {};
    // Identical up to the shorter one, so the difference is a missing or extra tail.
    const Entries& longer = observed.size() > expected.size() ? observed : expected;
    const char* which = observed.size() > expected.size() ? "extra" : "missing";
    return std::string("scan is ") + which + " " +
           std::to_string(longer.size() - common) + " entries starting at " +
           quote(longer[common].first) + " -> " + quote(longer[common].second) +
           " (returned " + std::to_string(observed.size()) + ", expected " +
           std::to_string(expected.size()) + ")";
}

/// ARCHITECTURE.md "Benchmarks" — the liveness guard, per operation. Overridable, because a per-operation wall-clock
/// bound is only meaningful when this process decides how fast it runs.
///
/// Under `ctest --parallel` it does not: a dozen replay processes, each with its own flush and
/// compaction threads, oversubscribe the machine, and one operation that triggers a compaction can
/// genuinely take minutes. No value of this bound converges there — 60 s, 300 s and 600 s each
/// abort a different set of configurations, none of them hung — so the long suite is not run in
/// parallel and the override is for anyone who loads the machine deliberately.
///
/// The bound turns a hang into a located failure. A hang is infinite, so a shorter value buys only
/// a faster report while a longer one buys immunity to a slow operation being mistaken for a
/// stopped one. The ctest per-test timeout is the outer backstop, and is larger still.
std::chrono::seconds watchdog_timeout() {
    const char* value = std::getenv("ELYSIUMKV_WATCHDOG_SECONDS");
    const int seconds = value == nullptr ? 300 : std::atoi(value);
    return std::chrono::seconds(seconds > 0 ? seconds : 300);
}

/// One replay: engine, oracle, and the oracle as of the last successful flush —
/// which is what a kill leaves behind (ARCHITECTURE.md "Positional recency" — on crash, all writes since the last
/// SST flush are lost).
class Replayer {
public:
    explicit Replayer(const ReplayConfig& config)
        : config_(config),
          store_(config.split_stores ? 2 : 1),
          watchdog_(watchdog_timeout(), "differential replay") {
        options_ = config.transient_band
                       ? make_transient_options(store_, Duration(60'000), Duration(120'000),
                                                config.memtable_bytes)
                   : config.split_stores
                       ? make_tiered_options(
                             store_,
                             Duration(config.tier_max_age_ms != 0
                                          ? static_cast<int64_t>(config.tier_max_age_ms)
                                          : 50),
                             config.compression, config.memtable_bytes)
                       : make_options(store_, config.compression, config.memtable_bytes);
        // Owned here, so it outlives the close-and-reopen: a budget that reset with the
        // instance would not be a *shared* budget, and the interesting moment is the one
        // where the closed instance's arena comes back off it.
        if (config.tier0_max_bytes != 0 && options_.tiers.size() > 1) {
            options_.tiers[0].max_bytes = config.tier0_max_bytes;
        }
        if (config.budget_bytes != 0) {
            budget_ = std::make_shared<MemoryBudget>(config.budget_bytes);
            options_.memory_budget = budget_;
        }

        // ARCHITECTURE.md "Invariants and sanitizers" — the continuous invariant checks, on for every replay. They cost a
        // scan of the version per edit and they turn "a scan came back one row short,
        // 900 operations ago" into "this edit produced two overlapping files below L0".
        options_.paranoid_checks = std::getenv("ELYSIUMKV_DIFF_NO_PARANOID") == nullptr;

        // Built once, before the first open, so the chain survives the reopen this
        // replay performs at the end: the reopen then reads through a *warm* cache.
        if (config.cached) {
            cache_every_tier(options_, store_.path() / "cache", config.cache_fetch_granularity);
        }
        if (config.max_compaction_bytes != 0) {
            options_.max_compaction_bytes = config.max_compaction_bytes;
        }
        options_.age_jitter = config.jitter;
        options_.tombstone_density_trigger = config.tombstone_density_trigger;
        // Low, because the streams are short: the engine default of 1024 entries would keep the
        // trigger from ever arming and the config would test nothing.
        options_.tombstone_density_min_entries = 16;

        // ARCHITECTURE.md "The differential oracle" — in the gating pass background work runs inline, so the op
        // stream fully determines the execution and a failing seed reproduces.
        options_.background =
            config.threaded ? BackgroundMode::Threaded : BackgroundMode::Inline;

        // …and time advances per operation rather than by the wall clock. A
        // tiered configuration places files by age, so a real clock would make
        // placement a function of machine speed — which is how a slower build
        // produced a different execution, and why a failure would not reproduce.
        // Atomic: in threaded mode the flush and background threads read this
        // while the replay advances it.
        options_.clock = [this] { return now_ms_.load(std::memory_order_relaxed); };
    }

    std::optional<DiffFailure> run(const std::vector<DiffOp>& ops) {
        if (auto failure = open()) return failure;

        for (size_t i = 0; i < ops.size(); ++i) {
            watchdog_.beat(i);
            now_ms_.fetch_add(10, std::memory_order_relaxed);  // deterministic time
            if (auto message = apply(ops[i])) {
                return DiffFailure{i, *message};
            }
        }

        // Final check, then once more after a reopen: the manifest must describe
        // the same store the running instance did.
        if (auto message = check_point_reads()) return DiffFailure{ops.size(), *message};
        if (auto message = check_entry_count()) return DiffFailure{ops.size(), *message};
        if (auto message = check_scan(Slice(), Slice(), false, oracle_entries(oracle_))) {
            return DiffFailure{ops.size(), *message};
        }
        if (Status status = db_->flush(); status != Status::Ok) {
            return DiffFailure{ops.size(), std::string("final flush: ") +
                                               std::string(status_name(status))};
        }
        close_db();
        if (auto failure = open()) return failure;
        if (auto message = check_scan(Slice(), Slice(), false, oracle_entries(oracle_))) {
            return DiffFailure{ops.size(), "after reopen: " + *message};
        }

        // ARCHITECTURE.md "Negative controls" — a configuration that claims to exercise shedding and never sheds
        // exercises nothing, and would sit in the suite looking like coverage. The
        // harness checks its own premise.
        // The same premise check for the density trigger. Every answer is identical whether it
        // fires or not — that is the property under test — so a configuration where it never
        // armed would pass while exercising nothing new.
        if (config_.tombstone_density_trigger > 0.0 &&
            density_compactions_seen_ + engine().density_compactions_for_test() == 0) {
            return DiffFailure{ops.size(),
                               "the tombstone-density trigger never fired, so this configuration "
                               "tested nothing: raise the delete rate or lower the trigger"};
        }

        // And for jitter: a configuration whose files all took the same offset spread nothing.
        if (config_.jitter > 0.0 && distinct_jitter_offsets() < 2) {
            return DiffFailure{ops.size(),
                               "the age jitter gave every file the same offset, so this "
                               "configuration spread nothing: raise the write volume"};
        }

        if (budget_ != nullptr && sheds_seen_ + db_->stats().budget_sheds == 0) {
            return DiffFailure{ops.size(),
                               "the memory budget was never exceeded, so this configuration "
                               "tested nothing: lower budget_bytes or raise the write volume"};
        }

        // The check this list was missing. `TinyCompactionBudget` carried a budget sized
        // against its memtable rather than against the files it produces, so the closure never
        // exceeded it and the trim never ran once — for months, while the config sat here looking
        // like coverage of exactly the path that turned out to have a reordering bug in it.
        if (config_.max_compaction_bytes != 0 &&
            trims_seen_ + db_->stats().compactions_trimmed == 0) {
            return DiffFailure{ops.size(),
                               "max_compaction_bytes never trimmed a compaction, so this "
                               "configuration tested nothing: lower it, or raise the write volume "
                               "until an L0 closure exceeds it"};
        }
        // A tiered configuration that never moves a file tests placement, not migration. The
        // migrator copies an object between stores byte for byte and *renumbers* it, under reads —
        // which is exactly the shape an oracle catches and a unit test has to think to check. This
        // suite ran with `migrations == 0` in every configuration, including the one named for it,
        // until the byte cap replaced the age bound as the driver.
        if (config_.tier0_max_bytes != 0 && migrations_seen_ + db_->stats().migrations == 0) {
            return DiffFailure{ops.size(),
                               "no file was ever migrated between tiers, so this configuration "
                               "tested nothing: lower tier0_max_bytes, or raise the write volume "
                               "until the hot tier exceeds it"};
        }
        // A cache that never misses tests half of a cache. `cache_on_write` populates on every
        // write, so a layer sized above the working set serves everything and the refill path —
        // walk outward, fetch, fill on the way back, and slice the fetched chunk back to what was
        // asked for — never runs. That path is also the only place `cache_fetch_granularity` has
        // any effect, so this check covers both knobs: with a granularity set and misses > 0,
        // every one of those misses went through `plan_fetch` with it.
        if (config_.cached) {
            uint64_t hits = 0;
            uint64_t misses = 0;
            for (const auto& tier : options_.tiers) {
                if (tier.store == nullptr) continue;
                if (CacheBlobStore* cache = tier.store->as_cache()) {
                    hits += cache->hits();
                    misses += cache->misses();
                }
            }
            if (hits == 0 || misses == 0) {
                return DiffFailure{ops.size(),
                                   std::string("the blob cache ") +
                                       (misses == 0 ? "never missed" : "never hit") +
                                       ", so this configuration tested half a cache: resize the "
                                       "layers in wrap_in_cache_chain relative to the working set"};
            }
        }
        return std::nullopt;
    }

private:
    /// The shed counter lives on the instance, and this replay reopens — repeatedly, and
    /// once more at the end. Reading it off the final instance answered zero however much
    /// shedding had happened, which is how the vacuity check below first lied.
    void close_db() {
        if (db_ != nullptr) {
            sheds_seen_ += db_->stats().budget_sheds;
            density_compactions_seen_ += engine().density_compactions_for_test();
            trims_seen_ += db_->stats().compactions_trimmed;
            migrations_seen_ += db_->stats().migrations;
        }
        db_.reset();
    }

    /// A kill, as distinct from a close. The two stopped being the same operation once
    /// destruction began attempting a flush: a clean close now saves the memtable, so a kill has to
    /// say that it does not want that, or `Kind::Kill` would preserve exactly the writes the oracle
    /// is about to declare lost.
    void kill_db() {
        if (db_ != nullptr) db_->abandon_unflushed();
        close_db();
    }

    std::optional<DiffFailure> open() {
        // open_with_result, not open: a transient configuration is refused by the
        // guarded form by design (ARCHITECTURE.md "A tier is not a level").
        auto opened = DB::open_with_result(options_);
        if (!opened) {
            return DiffFailure{0, std::string("open: ") + std::string(status_name(opened.error()))};
        }
        if (opened->requires_recovery) {
            return DiffFailure{0, "open reported requires_recovery with no store loss"};
        }
        db_ = std::move(opened->db);
        return std::nullopt;
    }

    std::optional<std::string> apply(const DiffOp& op) {
        switch (op.kind) {
            case DiffOp::Kind::Put: {
                const Status status = db_->put(Slice::from(op.key), Slice::from(op.value));
                if (below_floor(op.key)) return expect_refused("put", status);
                if (status != Status::Ok) return std::string("put: ") + std::string(status_name(status));
                oracle_.put(op.key, op.value);
                return std::nullopt;
            }
            case DiffOp::Kind::Remove: {
                const Status status = db_->remove(Slice::from(op.key));
                if (below_floor(op.key)) return expect_refused("remove", status);
                if (status != Status::Ok) {
                    return std::string("remove: ") + std::string(status_name(status));
                }
                oracle_.remove(op.key);
                return std::nullopt;
            }
            case DiffOp::Kind::Get: return check_get(op.key);
            case DiffOp::Kind::Batch: {
                WriteBatch batch;
                bool any_below = false;
                for (const auto& [kind, key, value] : op.batch) {
                    // Only a put or a remove under the floor refuses the batch. A range
                    // reaching below it is clamped and applies, so counting it here would make the
                    // replay expect a refusal the engine is right not to give.
                    if (kind != DiffOp::Kind::DeleteRange && below_floor(key)) any_below = true;
                    switch (kind) {
                        case DiffOp::Kind::Remove: batch.remove(Slice::from(key)); break;
                        case DiffOp::Kind::DeleteRange:
                            batch.delete_range(Slice::from(key), Slice::from(value));
                            break;
                        default: batch.put(Slice::from(key), Slice::from(value)); break;
                    }
                }
                const Status status = db_->write(batch);
                // A batch lands whole or not at all, so one key under the floor refuses all of it
                // and the oracle must not see any of them.
                if (any_below) return expect_refused("write", status);
                if (status != Status::Ok) {
                    return std::string("write: ") + std::string(status_name(status));
                }
                // In batch order, which is the whole property under test: a put before a range is
                // covered by it and one after it is not.
                for (const auto& [kind, key, value] : op.batch) {
                    switch (kind) {
                        case DiffOp::Kind::Remove: oracle_.remove(key); break;
                        case DiffOp::Kind::DeleteRange: oracle_.delete_range(key, value); break;
                        default: oracle_.put(key, value); break;
                    }
                }
                return std::nullopt;
            }
            case DiffOp::Kind::ScanAll:
                return check_scan(Slice(), Slice(), false, oracle_entries(oracle_));
            case DiffOp::Kind::ScanRange: {
                Entries expected;
                for (const auto& entry : oracle_.range(op.key, op.upper)) expected.push_back(entry);
                return check_scan(Slice::from(op.key), Slice::from(op.upper), true, expected);
            }
            case DiffOp::Kind::ScanPrefix: {
                Entries expected;
                for (const auto& entry : oracle_.prefix(op.key)) expected.push_back(entry);
                return check_prefix_scan(op.key, expected);
            }
            case DiffOp::Kind::TruncateBelow: {
                const Status status = db_->truncate_below(Slice::from(op.key));
                if (status != Status::Ok) {
                    return std::string("truncate_below: ") + std::string(status_name(status));
                }
                oracle_.truncate_below(op.key);
                if (op.key > floor_) floor_ = op.key;
                // A manifest edit is durable when it returns, so the point survives a kill even
                // though the memtable does not. Applying it to `flushed_` is what says so.
                flushed_.truncate_below(op.key);
                return std::nullopt;
            }
            case DiffOp::Kind::DeleteRange: {
                const Status status =
                    db_->delete_range(Slice::from(op.key), Slice::from(op.upper));
                if (status != Status::Ok) {
                    return std::string("delete_range: ") + std::string(status_name(status));
                }
                // The engine clamps a range reaching below the truncation floor rather than
                // refusing it, and the oracle has already erased everything down there, so the
                // clamp needs no counterpart here.
                oracle_.delete_range(op.key, op.upper);
                // Not applied to `flushed_`, unlike a truncation. A truncation is a manifest
                // edit and durable when it returns; this is a record in the memtable, so a kill
                // before the next flush loses it and every key it covered comes back.
                return std::nullopt;
            }
            case DiffOp::Kind::ReverseScanAll:
                return check_reverse_scan(Slice(), Slice(), false,
                                          reversed(oracle_entries(oracle_)));
            case DiffOp::Kind::ReverseScanRange: {
                Entries expected;
                for (const auto& entry : oracle_.range(op.key, op.upper)) expected.push_back(entry);
                return check_reverse_scan(Slice::from(op.key), Slice::from(op.upper), true,
                                          reversed(expected));
            }
            case DiffOp::Kind::ReverseScanPrefix: {
                Entries expected;
                for (const auto& entry : oracle_.prefix(op.key)) expected.push_back(entry);
                return check_reverse_prefix_scan(op.key, reversed(expected));
            }
            case DiffOp::Kind::Flush: {
                const Status status = db_->flush();
                if (status != Status::Ok) {
                    return std::string("flush: ") + std::string(status_name(status));
                }
                flushed_ = oracle_;
                return std::nullopt;
            }
            case DiffOp::Kind::IterAcrossFlush: return check_iterate_across_flush();
            case DiffOp::Kind::Compact: {
                // Threaded mode has a compaction thread doing this too; calling
                // it here as well is exactly the point of the randomized pass.
                const Status status = engine().compact_until_quiet();
                if (status != Status::Ok) {
                    return std::string("compact: ") + std::string(status_name(status));
                }
                return check_scan(Slice(), Slice(), false, oracle_entries(oracle_));
            }
            case DiffOp::Kind::IterAcrossCompaction: return check_iterate_across_compaction();
            case DiffOp::Kind::ReverseIterAcrossCompaction:
                return check_reverse_iterate_across_compaction();
            case DiffOp::Kind::Reopen: {
                const Status status = db_->flush();
                if (status != Status::Ok) {
                    return std::string("flush before reopen: ") + std::string(status_name(status));
                }
                flushed_ = oracle_;
                close_db();
                if (auto failure = open()) return failure->message;
                return check_scan(Slice(), Slice(), false, oracle_entries(oracle_));
            }
            case DiffOp::Kind::Kill: {
                // ARCHITECTURE.md "Positional recency" — everything since the last successful flush is lost, and
                // restoring it is the embedder's business. Everything before it
                // must come back exactly.
                kill_db();
                oracle_ = flushed_;
                if (auto failure = open()) return failure->message;
                return check_scan(Slice(), Slice(), false, oracle_entries(oracle_));
            }
        }
        return std::nullopt;
    }

    std::optional<std::string> check_get(const std::string& key) {
        auto expected = oracle_.get(key);
        auto found = db_->get(Slice::from(key));
        if (expected.has_value()) {
            if (!found) {
                return "get(" + quote(key) + ") -> " + std::string(status_name(found.error())) +
                       ", expected " + quote(*expected);
            }
            if (found->value().to_string() != *expected) {
                return "get(" + quote(key) + ") -> " + quote(found->value().to_string()) +
                       ", expected " + quote(*expected);
            }
            return std::nullopt;
        }
        if (found) {
            return "get(" + quote(key) + ") -> " + quote(found->value().to_string()) +
                   ", expected absent";
        }
        if (found.error() != Status::NotFound) {
            return "get(" + quote(key) + ") -> " + std::string(status_name(found.error())) +
                   ", expected not_found";
        }
        return std::nullopt;
    }

    std::optional<std::string> check_point_reads() {
        for (const auto& [key, value] : oracle_.entries()) {
            if (auto message = check_get(key)) return message;
        }
        return std::nullopt;
    }

    /// The entry count is an upper bound on distinct live keys, never an estimate of them.
    ///
    /// Every distinct live key occupies at least one record, so `records >= live keys` holds for
    /// every workload — and the oracle knows exactly how many live keys there are. Checked here
    /// rather than only in hand-written cases because the property is about *all* workloads, and the
    /// generator produces update and delete mixes no hand-written case would think of.
    ///
    /// Deliberately not asserted in the other direction. The count legitimately exceeds the oracle,
    /// often by a lot, until compaction has merged the superseded versions and dropped the
    /// tombstones. That slack is the feature, not a defect.
    std::optional<std::string> check_entry_count() {
        const Stats stats = db_->stats();
        uint64_t count = stats.memtable_entries - stats.memtable_tombstones;
        for (const LevelStats& level : stats.levels) count += level.entries - level.tombstones;
        if (count < oracle_.size()) {
            return "entry count " + std::to_string(count) + " is below the oracle's " +
                   std::to_string(oracle_.size()) +
                   " live keys — it is meant to be an upper bound, so either a live key occupies "
                   "no record or a tombstone was subtracted that never counted itself";
        }
        return std::nullopt;
    }

    std::optional<std::string> collect(Iterator& it, Entries& out) {
        while (it.next()) out.emplace_back(it.key().to_string(), it.value().to_string());
        if (it.status() != Status::Ok) {
            return std::string("iterator status: ") + std::string(status_name(it.status()));
        }
        return std::nullopt;
    }

    std::optional<std::string> check_scan(Slice lower, Slice upper, bool bounded,
                                          const Entries& expected) {
        Entries observed;
        auto it = bounded ? db_->iterator(lower, upper) : db_->iterator();
        if (auto message = collect(*it, observed)) return message;
        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return mismatch;
        return std::nullopt;
    }

    /// The truncation floor is permanent, so the oracle has to expect a refusal rather than a
    /// write. Modelled here rather than by generating only legal streams, because "the engine
    /// refuses this" is the behaviour under test.
    bool below_floor(const std::string& key) const { return !floor_.empty() && key < floor_; }

    std::optional<std::string> expect_refused(const char* what, Status status) const {
        if (status == Status::Config) return std::nullopt;
        return std::string(what) + " below the truncation floor should have been refused, got " +
               std::string(status_name(status));
    }

    static Entries reversed(Entries entries) {
        std::reverse(entries.begin(), entries.end());
        return entries;
    }

    std::optional<std::string> check_reverse_scan(Slice lower, Slice upper, bool bounded,
                                                  const Entries& expected) {
        Entries observed;
        auto it = bounded ? db_->reverse_iterator(lower, upper) : db_->reverse_iterator();
        if (auto message = collect(*it, observed)) return message;
        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return "reverse: " + mismatch;
        return std::nullopt;
    }

    std::optional<std::string> check_reverse_prefix_scan(const std::string& prefix,
                                                         const Entries& expected) {
        Entries observed;
        auto it = db_->reverse_prefix_iterator(Slice::from(prefix));
        if (auto message = collect(*it, observed)) return message;
        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return "reverse prefix " + quote(prefix) + ": " + mismatch;
        return std::nullopt;
    }

    std::optional<std::string> check_prefix_scan(const std::string& prefix,
                                                 const Entries& expected) {
        Entries observed;
        auto it = db_->prefix_iterator(Slice::from(prefix));
        if (auto message = collect(*it, observed)) return message;
        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return "prefix " + quote(prefix) + ": " + mismatch;
        return std::nullopt;
    }

    /// ARCHITECTURE.md "The differential oracle" — an iterator advanced across a forced flush. A flush changes where
    /// the data lives, never what it says.
    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    /// How many *different* age offsets the live files were given. One means the window is
    /// there and nothing landed in it.
    size_t distinct_jitter_offsets() {
        auto tiers = resolve_tiers(options_.tiers, options_.age_jitter);
        if (!tiers.has_value()) return 0;
        std::set<uint64_t> offsets;
        auto version = engine().current_version();
        for (const FileMetadata& file : version->all_files()) {
            const int at = tiers->tier_of_store(file.store_id);
            if (at < 0) continue;
            const Tier& tier = tiers->tiers[static_cast<size_t>(at)];
            if (!tier.max_age.has_value()) continue;
            offsets.insert(tier_age_jitter_ms(*tiers, file.file_number, file.min_write_time_ms,
                                              static_cast<uint64_t>(tier.max_age->count())));
        }
        return offsets.size();
    }

    /// ARCHITECTURE.md "Versions are immutable snapshots" — the failure this component exists to prevent: an iterator reading a
    /// file that compaction unlinked mid-scan. Silent, load-dependent, and it
    /// does not reproduce without deliberate effort.
    /// The reverse twin: half a descending scan, then a compaction, then the rest. The iterator
    /// holds the Version it started on, so the files it is mid-way through reading must stay
    /// readable even though compaction has unlinked them.
    std::optional<std::string> check_reverse_iterate_across_compaction() {
        Entries expected = oracle_entries(oracle_);
        std::reverse(expected.begin(), expected.end());
        Entries observed;

        auto it = db_->reverse_iterator();
        const size_t half = expected.size() / 2;
        while (observed.size() < half && it->next()) {
            observed.emplace_back(it->key().to_string(), it->value().to_string());
        }

        if (Status status = db_->flush(); status != Status::Ok) {
            return std::string("flush mid-reverse-iteration: ") + std::string(status_name(status));
        }
        flushed_ = oracle_;
        if (Status status = engine().compact_until_quiet(); status != Status::Ok) {
            return std::string("compact mid-reverse-iteration: ") +
                   std::string(status_name(status));
        }

        if (auto message = collect(*it, observed)) return message;
        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return "reverse across compaction: " + mismatch;
        return std::nullopt;
    }

    std::optional<std::string> check_iterate_across_compaction() {
        const Entries expected = oracle_entries(oracle_);
        Entries observed;

        auto it = db_->iterator();
        const size_t half = expected.size() / 2;
        while (observed.size() < half && it->next()) {
            observed.emplace_back(it->key().to_string(), it->value().to_string());
        }

        if (Status status = db_->flush(); status != Status::Ok) {
            return std::string("flush mid-iteration: ") + std::string(status_name(status));
        }
        flushed_ = oracle_;
        if (Status status = engine().compact_until_quiet(); status != Status::Ok) {
            return std::string("compact mid-iteration: ") + std::string(status_name(status));
        }

        if (auto message = collect(*it, observed)) return message;
        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return "across compaction: " + mismatch;
        return std::nullopt;
    }

    std::optional<std::string> check_iterate_across_flush() {
        const Entries expected = oracle_entries(oracle_);
        Entries observed;

        auto it = db_->iterator();
        const size_t half = expected.size() / 2;
        while (observed.size() < half && it->next()) {
            observed.emplace_back(it->key().to_string(), it->value().to_string());
        }
        const Status status = db_->flush();
        if (status != Status::Ok) {
            return std::string("flush mid-iteration: ") + std::string(status_name(status));
        }
        flushed_ = oracle_;
        if (auto message = collect(*it, observed)) return message;

        const std::string mismatch = describe_mismatch(observed, expected);
        if (!mismatch.empty()) return "across flush: " + mismatch;
        return std::nullopt;
    }

    ReplayConfig config_;
    TestStore store_;
    OperationWatchdog watchdog_;
    Options options_;
    std::shared_ptr<MemoryBudget> budget_;
    uint64_t sheds_seen_ = 0;
    uint64_t density_compactions_seen_ = 0;
    uint64_t trims_seen_ = 0;
    uint64_t migrations_seen_ = 0;
    std::unique_ptr<DB> db_;
    Oracle oracle_;
    Oracle flushed_;
    /// Durable and monotone, so it survives a kill and a reopen exactly as the manifest does.
    std::string floor_;
    std::atomic<uint64_t> now_ms_{1'000'000};
};

}  // namespace

std::optional<DiffFailure> replay(const std::vector<DiffOp>& ops, const ReplayConfig& config) {
    Replayer replayer(config);
    return replayer.run(ops);
}

std::vector<DiffOp> shrink(std::vector<DiffOp> ops, const ReplayConfig& config,
                           int max_replays) {
    return shrink(
        std::move(ops),
        [&config](const std::vector<DiffOp>& candidate) {
            return replay(candidate, config).has_value();
        },
        max_replays);
}

std::vector<DiffOp> shrink(std::vector<DiffOp> ops,
                           const std::function<bool(const std::vector<DiffOp>&)>& still_fails,
                           int max_replays) {
    int replays = 0;
    size_t span = ops.size() / 2;

    while (span >= 1 && replays < max_replays) {
        bool reduced = false;
        for (size_t start = 0; start + span <= ops.size() && replays < max_replays;) {
            std::vector<DiffOp> candidate;
            candidate.reserve(ops.size() - span);
            candidate.insert(candidate.end(), ops.begin(),
                             ops.begin() + static_cast<long>(start));
            candidate.insert(candidate.end(), ops.begin() + static_cast<long>(start + span),
                             ops.end());

            ++replays;
            if (still_fails(candidate)) {
                // Still fails without that span, so the span was not the cause.
                ops = std::move(candidate);
                reduced = true;
                if (span > ops.size()) span = ops.size();
            } else {
                start += span;
            }
        }
        if (!reduced) span /= 2;
    }
    return ops;
}

}  // namespace elysiumkv::test
