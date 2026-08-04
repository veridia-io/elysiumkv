#include "fault/fault_injecting_blob_store.hpp"

#include <algorithm>
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
    {
        std::lock_guard<std::mutex> lock(mutex_);
        latency = latency_;
        if (const Rule* rule = match(Op::Get, name)) {
            return make_ready_future(GetResult(std::unexpected(rule->status)));
        }
    }
    if (latency.count() > 0) std::this_thread::sleep_for(latency);
    return make_ready_future(delegate_->get(name, offset, len).get());
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
        return make_ready_future(fired->status);
    }
    return make_ready_future(delegate_->put(name, bytes).get());
}

std::future<Status> FaultInjectingBlobStore::remove(std::string_view name) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (const Rule* rule = match(Op::Remove, name)) {
            return make_ready_future(rule->status);
        }
    }
    return make_ready_future(delegate_->remove(name).get());
}

std::future<Status> FaultInjectingBlobStore::remove_many(const std::vector<std::string>& names) {
    record(Op::RemoveMany);
    return BlobStore::remove_many(names);
}

std::future<ListResult> FaultInjectingBlobStore::list(std::string_view prefix) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (const Rule* rule = match(Op::List, prefix)) {
            return make_ready_future(ListResult(std::unexpected(rule->status)));
        }
    }
    return make_ready_future(delegate_->list(prefix).get());
}

}  // namespace elysiumkv::test
