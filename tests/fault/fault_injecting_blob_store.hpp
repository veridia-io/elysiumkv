#ifndef ELYSIUMKV_TESTS_FAULT_INJECTING_BLOB_STORE_HPP
#define ELYSIUMKV_TESTS_FAULT_INJECTING_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace elysiumkv::test {

/// ARCHITECTURE.md "Immutable named objects" — **tests only.** A decorator over any store: latency, error injection by
/// name pattern and call index, torn writes, vanished objects. It is not a tier
/// and must never be constructible from Options in a production path.
///
/// Injection is by call *index*, not by wall clock, so every failure a test
/// produces replays exactly from its seed.
class FaultInjectingBlobStore final : public BlobStore {
public:
    enum class Op { Get, Put, Remove, RemoveMany, List };

    struct Rule {
        Op op = Op::Get;
        /// Substring the object name (or list prefix) must contain. Empty matches any.
        std::string name_contains;
        /// Index among calls that match (op, name_contains), zero-based.
        uint64_t first_match = 0;
        /// How many consecutive matching calls to fail; 0 means "unbounded".
        uint64_t match_count = 1;
        Status status = Status::Io;
        /// Put only: write a prefix of the bytes before reporting `status`.
        bool torn_write = false;
        size_t torn_bytes = 0;
    };

    explicit FaultInjectingBlobStore(std::shared_ptr<BlobStore> delegate)
        : delegate_(std::move(delegate)) {}

    void add_rule(Rule rule);
    void clear_rules();

    /// Every operation reports Io: the store cannot answer. This is "failure to
    /// look", which ARCHITECTURE.md - A tier is not a level forbids treating as evidence of loss.
    void set_unreachable(bool unreachable);

    /// Artificial latency applied to every call.
    void set_latency(std::chrono::microseconds latency);

    /// Remove an object behind the engine's back — the store still answers, and
    /// answers truthfully that the object is gone. This is the *only* shape of
    /// evidence a discard may act on.
    Status vanish(std::string_view name);
    /// Whole-store loss: every object disappears, list still succeeds.
    Status vanish_all();

    uint64_t call_count(Op op) const;

    /// The most `list` calls that were ever in flight across **every** store sharing this counter,
    /// at the same instant.
    ///
    /// **A direct observation of concurrency, where the wall clock is only a proxy for it.** The
    /// test this exists for used to time `open` and assert it came in under two latencies; that
    /// holds on an idle machine and fails on a loaded one under a sanitizer, and widening the
    /// bound is no help — serial *is* two latencies, so any bound loose enough to survive the
    /// noise admits the thing being ruled out. A count of simultaneous callers is discrete, and
    /// load makes an overlap more likely rather than less.
    static uint64_t peak_concurrent_lists();
    static void reset_peak_concurrent_lists();

    std::string id() const override { return delegate_->id(); }
    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    /// Counts the bulk call, then runs the base implementation — which loops over
    /// **this** store's `remove`, so `Op::Remove` rules still fire and no existing
    /// case is weakened by the engine having switched to the bulk path.
    std::future<Status> remove_many(const std::vector<std::string>& names) override;
    std::future<ListResult> list(std::string_view prefix) override;
    BlobStore& bulk_view() override { return *this; }

    BlobStore& delegate() { return *delegate_; }

private:
    /// Returns the injected status when a rule fires, plus the rule itself.
    const Rule* match(Op op, std::string_view name);
    /// Counts a call without consulting any rule.
    void record(Op op);

    std::shared_ptr<BlobStore> delegate_;
    mutable std::mutex mutex_;
    std::vector<Rule> rules_;
    std::map<size_t, uint64_t> rule_hits_;  // rule index -> matching calls seen
    std::map<int, uint64_t> calls_;         // op -> total calls
    bool unreachable_ = false;
    std::chrono::microseconds latency_{0};
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_FAULT_INJECTING_BLOB_STORE_HPP
