#include "diff/replay.hpp"

#include "diff/oracle.hpp"
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

/// **Names the first difference even when the counts differ.** Reporting only "returned
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

/// ARCHITECTURE.md "Benchmarks" — the liveness guard, per operation. **Overridable, because a per-operation wall-clock
/// bound is only meaningful when this process decides how fast it runs.**
///
/// Under `ctest --parallel` it does not: an operation that takes a minute alone takes five in
/// company, and every abort that produced was a false alarm. Three attempts at tuning the
/// number taught that it cannot converge — 60 s, 300 s and 600 s each aborted a different set,
/// none of which had hung — and the eventual answer was to stop running the long suite in
/// parallel at all, which turned out to be faster as well. The override remains for anyone who
/// does load the machine deliberately.
///
/// It exists to turn a hang into a located failure, and a hang is infinite — so the only thing
/// a shorter timeout buys is a faster report, while a longer one buys immunity to a slow
/// operation being mistaken for a stopped one. Sixty seconds was safe when the suite ran one
/// test at a time; under `ctest --parallel` it is not, because a dozen replay processes, each
/// with its own flush and compaction threads, oversubscribe the machine and one operation that
/// triggers a compaction can genuinely take minutes. Seven configurations aborted here the
/// first time the nightly was run in parallel, none of them hung.
///
/// The ctest per-test timeout is the outer backstop, and is larger still.
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
                       ? make_tiered_options(store_, Duration(50), config.compression,
                                             config.memtable_bytes)
                       : make_options(store_, config.compression, config.memtable_bytes);
        // Owned here, so it outlives the close-and-reopen: a budget that reset with the
        // instance would not be a *shared* budget, and the interesting moment is the one
        // where the closed instance's arena comes back off it.
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
        if (config.cached) cache_every_tier(options_, store_.path() / "cache");

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
        if (budget_ != nullptr && sheds_seen_ + db_->stats().budget_sheds == 0) {
            return DiffFailure{ops.size(),
                               "the memory budget was never exceeded, so this configuration "
                               "tested nothing: lower budget_bytes or raise the write volume"};
        }
        return std::nullopt;
    }

private:
    /// The shed counter lives on the instance, and this replay reopens — repeatedly, and
    /// once more at the end. Reading it off the final instance answered zero however much
    /// shedding had happened, which is how the vacuity check below first lied.
    void close_db() {
        if (db_ != nullptr) sheds_seen_ += db_->stats().budget_sheds;
        db_.reset();
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
                if (status != Status::Ok) return std::string("put: ") + std::string(status_name(status));
                oracle_.put(op.key, op.value);
                return std::nullopt;
            }
            case DiffOp::Kind::Remove: {
                const Status status = db_->remove(Slice::from(op.key));
                if (status != Status::Ok) {
                    return std::string("remove: ") + std::string(status_name(status));
                }
                oracle_.remove(op.key);
                return std::nullopt;
            }
            case DiffOp::Kind::Get: return check_get(op.key);
            case DiffOp::Kind::Batch: {
                WriteBatch batch;
                for (const auto& [is_delete, key, value] : op.batch) {
                    if (is_delete) {
                        batch.remove(Slice::from(key));
                        oracle_.remove(key);
                    } else {
                        batch.put(Slice::from(key), Slice::from(value));
                        oracle_.put(key, value);
                    }
                }
                const Status status = db_->write(batch);
                if (status != Status::Ok) {
                    return std::string("write: ") + std::string(status_name(status));
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
                close_db();
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

    /// **The entry count is an upper bound on distinct live keys, never an estimate of them.**
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
    std::unique_ptr<DB> db_;
    Oracle oracle_;
    Oracle flushed_;
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
