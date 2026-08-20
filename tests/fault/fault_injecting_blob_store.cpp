#include "fault/fault_injecting_blob_store.hpp"

#include <algorithm>
#include <atomic>
#include <thread>

namespace elysiumkv::test {

void FaultInjectingBlobStore::add_rule(Rule rule) {
    std::lock_guard<std::mutex> lock(mutex_);
    rules_.push_back(std::move(rule));
}

void FaultInjectingBlobStore::clear_rules() {
    std::lock_guard<std::mutex> lock(mutex_);
    rules_.clear();
    rule_hits_.clear();
}

void FaultInjectingBlobStore::set_unreachable(bool unreachable) {
    std::lock_guard<std::mutex> lock(mutex_);
    unreachable_ = unreachable;
}

void FaultInjectingBlobStore::set_latency(std::chrono::microseconds latency) {
    std::lock_guard<std::mutex> lock(mutex_);
    latency_ = latency;
}

uint64_t FaultInjectingBlobStore::call_count(Op op) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = calls_.find(static_cast<int>(op));
    return it == calls_.end() ? 0 : it->second;
}

Status FaultInjectingBlobStore::vanish(std::string_view name) {
    return delegate_->remove(name).get();
}

Status FaultInjectingBlobStore::vanish_all() {
    auto listing = delegate_->list("").get();
    if (!listing) return listing.error();
    for (const std::string& name : *listing) {
        const Status status = delegate_->remove(name).get();
        if (status != Status::Ok) return status;
    }
    return Status::Ok;
}

void FaultInjectingBlobStore::record(Op op) {
    std::lock_guard<std::mutex> lock(mutex_);
    calls_[static_cast<int>(op)]++;
}

const FaultInjectingBlobStore::Rule* FaultInjectingBlobStore::match(Op op, std::string_view name) {
    calls_[static_cast<int>(op)]++;
    if (unreachable_) {
        static const Rule kUnreachable{.op = Op::Get,
                                       .name_contains = {},
                                       .first_match = 0,
                                       .match_count = 0,
                                       .status = Status::Io,
                                       .torn_write = false,
                                       .torn_bytes = 0};
        return &kUnreachable;
    }
    for (size_t i = 0; i < rules_.size(); ++i) {
        const Rule& rule = rules_[i];
        if (rule.op != op) continue;
        if (!rule.name_contains.empty() &&
            name.find(rule.name_contains) == std::string_view::npos) {
            continue;
        }
        const uint64_t seen = rule_hits_[i]++;
        if (seen < rule.first_match) continue;
        if (rule.match_count != 0 && seen >= rule.first_match + rule.match_count) continue;
        return &rule;
    }
    return nullptr;
}

std::future<GetResult> FaultInjectingBlobStore::get(std::string_view name, uint64_t offset,
                                                    size_t len) {
    std::chrono::microseconds latency{0};
    std::optional<Status> injected;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        latency = latency_;
        if (const Rule* rule = match(Op::Get, name)) injected = rule->status;
    }

    // One result and one return. Returning the injected failure from its own statement leaves
    // gcc 13 looking at a move of an `expected` whose value arm it can prove was never written, and
    // `-Wmaybe-uninitialized` fails the build over reading it. An injected failure still skips the
    // latency: a store that rejected the call never made the round trip.
    GetResult result = std::unexpected(Status::Io);
    if (injected.has_value()) {
        result = std::unexpected(*injected);
    } else {
        if (latency.count() > 0) std::this_thread::sleep_for(latency);
        result = delegate_->get(name, offset, len).get();
    }
    note_get(result);
    return make_ready_future(std::move(result));
}

std::future<Status> FaultInjectingBlobStore::put(std::string_view name, Slice bytes) {
    std::chrono::microseconds latency{0};
    const Rule* fired = nullptr;
    Rule copy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        latency = latency_;
        if (const Rule* rule = match(Op::Put, name)) {
            copy = *rule;
            fired = &copy;
        }
    }
    if (latency.count() > 0) std::this_thread::sleep_for(latency);

    if (fired != nullptr) {
        if (fired->torn_write) {
            // The object becomes visible holding a prefix of the bytes — what a
            // half-completed multipart upload leaves behind. CRC catches it.
            const size_t n = std::min(fired->torn_bytes, bytes.size());
            (void)delegate_->put(name, Slice(bytes.data(), n)).get();
        }
        note_put(fired->status, bytes.size());
        return make_ready_future(fired->status);
    }
    const Status status = delegate_->put(name, bytes).get();
    note_put(status, bytes.size());
    return make_ready_future(status);
}

std::future<Status> FaultInjectingBlobStore::remove(std::string_view name) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (const Rule* rule = match(Op::Remove, name)) {
            note_remove(rule->status);
            return make_ready_future(rule->status);
        }
    }
    const Status status = delegate_->remove(name).get();
    note_remove(status);
    return make_ready_future(status);
}

std::future<Status> FaultInjectingBlobStore::remove_many(const std::vector<std::string>& names) {
    record(Op::RemoveMany);
    // Not counted here: the base loops over this store's `remove`, which counts each one.
    return BlobStore::remove_many(names);
}

namespace {

/// Process-wide, because the property is that *different stores* list at the same time — a counter
/// per store could only ever reach one.
std::atomic<uint64_t> g_lists_in_flight{0};
std::atomic<uint64_t> g_peak_lists{0};

struct ListInFlight {
    ListInFlight() {
        const uint64_t now = g_lists_in_flight.fetch_add(1, std::memory_order_acq_rel) + 1;
        uint64_t seen = g_peak_lists.load(std::memory_order_relaxed);
        while (now > seen &&
               !g_peak_lists.compare_exchange_weak(seen, now, std::memory_order_relaxed)) {
        }
    }
    ~ListInFlight() { g_lists_in_flight.fetch_sub(1, std::memory_order_acq_rel); }
    ListInFlight(const ListInFlight&) = delete;
    ListInFlight& operator=(const ListInFlight&) = delete;
};

}  // namespace

uint64_t FaultInjectingBlobStore::peak_concurrent_lists() {
    return g_peak_lists.load(std::memory_order_relaxed);
}

void FaultInjectingBlobStore::reset_peak_concurrent_lists() {
    g_peak_lists.store(0, std::memory_order_relaxed);
}

std::future<ListResult> FaultInjectingBlobStore::list(std::string_view prefix) {
    std::chrono::microseconds latency{0};
    std::optional<Status> injected;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        latency = latency_;
        if (const Rule* rule = match(Op::List, prefix)) injected = rule->status;
    }

    // One result and one return, for the reason `get` above gives.
    ListResult result = std::unexpected(Status::Io);
    if (injected.has_value()) {
        result = std::unexpected(*injected);
    } else {
        // A listing is a round trip like any other, and the one open pays per store. Counted
        // across the sleep, so a caller that overlaps another is visible as an overlap.
        const ListInFlight in_flight;
        if (latency.count() > 0) std::this_thread::sleep_for(latency);
        result = delegate_->list(prefix).get();
    }
    note_list(result);
    return make_ready_future(std::move(result));
}

}  // namespace elysiumkv::test
