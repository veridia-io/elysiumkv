#ifndef ELYSIUMKV_BLOB_SINGLE_FLIGHT_HPP
#define ELYSIUMKV_BLOB_SINGLE_FLIGHT_HPP

#include "elysiumkv/blob_store.hpp"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace elysiumkv {

/// Collapses concurrent identical fetches into one.
///
/// A cache miss is the one place a request count multiplies by reader count. Ten threads
/// arriving on a cold chunk each miss, each fetch it, and each write it — ten round trips and ten
/// writes for one chunk of bytes. Against object storage that is ten times the bill, and on a cold
/// start with a shared cache it is the difference between one fetch per chunk and one per reader.
///
/// Keyed by the whole fetch plan, not by the request. Readers wanting different windows of one
/// chunk round to the same plan and share its result; a reader whose request is larger than the
/// chunk rounds to a different plan and correctly does not.
class SingleFlight {
public:
    /// Runs `fetch` if no identical one is in progress, otherwise waits for the one that is and
    /// returns its result. `fetch` runs with no lock of this class held.
    GetResult run(std::string_view name, uint64_t offset, size_t len,
                  const std::function<GetResult()>& fetch) {
        const Key key{std::string(name), offset, len};

        std::shared_ptr<Call> call;
        bool leader = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = calls_.find(key);
            if (it != calls_.end()) {
                call = it->second;
            } else {
                call = std::make_shared<Call>();
                calls_.emplace(key, call);
                leader = true;
            }
        }

        if (!leader) {
            std::unique_lock<std::mutex> lock(call->mutex);
            call->ready.wait(lock, [&call] { return call->done; });
            return call->result;
        }

        // Completed on every path, including an exception. A leader that left without
        // publishing would leave its followers waiting forever, which is worse than any error it
        // could have reported.
        struct Completion {
            SingleFlight* self;
            const Key* key;
            const std::shared_ptr<Call>* call;
            ~Completion() {
                {
                    std::lock_guard<std::mutex> lock((*call)->mutex);
                    (*call)->done = true;
                }
                (*call)->ready.notify_all();
                std::lock_guard<std::mutex> lock(self->mutex_);
                self->calls_.erase(*key);
            }
        } completion{this, &key, &call};

        GetResult result = fetch();
        {
            std::lock_guard<std::mutex> lock(call->mutex);
            call->result = result;
        }
        return result;
    }

private:
    struct Key {
        std::string name;
        uint64_t offset;
        size_t len;
        bool operator==(const Key&) const = default;
    };
    struct KeyHash {
        size_t operator()(const Key& key) const {
            uint64_t h = std::hash<std::string>{}(key.name);
            h = h * 0x9E3779B97F4A7C15ull + key.offset;
            h = h * 0x9E3779B97F4A7C15ull + key.len;
            h ^= h >> 31;
            return static_cast<size_t>(h);
        }
    };
    struct Call {
        std::mutex mutex;
        std::condition_variable ready;
        bool done = false;
        /// Defaults to the error a leader that never got as far as fetching would owe its
        /// followers; overwritten by every path that does.
        GetResult result{std::unexpected(Status::Io)};
    };

    std::mutex mutex_;
    std::unordered_map<Key, std::shared_ptr<Call>, KeyHash> calls_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_SINGLE_FLIGHT_HPP
