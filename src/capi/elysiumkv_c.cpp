#include "elysiumkv/elysiumkv.h"

// Generated into the build tree by `configure_file` (see the top-level CMakeLists);
// carries ELYSIUMKV_VERSION_STRING. Private — not part of the installed headers.
#include "elysiumkv/elysiumkv_version.hpp"

#include "elysiumkv/memory_budget.hpp"
#include "cache/sharded_lru.hpp"
#include "elysiumkv/db.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/disk_cache_blob_store.hpp"
#include "elysiumkv/disk_blob_store.hpp"
#include "elysiumkv/memory_cache_blob_store.hpp"

// The remote implementations are an optional component (ARCHITECTURE.md "Dependencies and artifacts"), but the ABI shape
// is not: the constructors below exist in every build and report
// ELYSIUMKV_CONFIG when this is off. They live in *this* translation unit rather
// than a companion one on purpose — `last_error` is a thread_local with internal
// linkage here, so a second TU would get a second slot and a failure reported by
// one would be invisible to `elysiumkv_last_error()` reading the other.
#ifdef ELYSIUMKV_WITH_AWS
#  include "elysiumkv/dynamo_manifest_catalog.hpp"
#  include "elysiumkv/s3_blob_store.hpp"
#  include "elysiumkv/s3_manifest_catalog.hpp"
#endif

#include <atomic>
#include <chrono>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace elysiumkv {
namespace {

/// ARCHITECTURE.md "The ABI boundary" — thread-local, so a failure on one thread cannot be read as another's.
std::string& last_error_slot() {
    static thread_local std::string message;
    return message;
}

void set_last_error(std::string message) { last_error_slot() = std::move(message); }

elysiumkv_status to_c(Status status) {
    switch (status) {
        case Status::Ok: return ELYSIUMKV_OK;
        case Status::NotFound: return ELYSIUMKV_NOT_FOUND;
        case Status::Corrupt: return ELYSIUMKV_CORRUPT;
        case Status::Unusable: return ELYSIUMKV_UNUSABLE;
        case Status::Fenced: return ELYSIUMKV_FENCED;
        case Status::Config: return ELYSIUMKV_CONFIG;
        case Status::Io: return ELYSIUMKV_IO;
        case Status::Stalled: return ELYSIUMKV_STALLED;
        case Status::Unsupported: return ELYSIUMKV_UNSUPPORTED;
        case Status::Stale: return ELYSIUMKV_STALE;
    }
    return ELYSIUMKV_UNUSABLE;
}

/// Every entry point is wrapped in this: **a C++ exception escaping the ABI is
/// undefined behaviour** (ARCHITECTURE.md "The ABI boundary"), so the boundary converts anything that escapes
/// into a status and a message. `Status::Unusable` rather than `Io`, because an
/// exception is not a "try again later" — the instance is in a state the engine
/// does not model.
template <typename Body>
elysiumkv_status guard(Body&& body) noexcept {
    try {
        return body();
    } catch (const std::exception& error) {
        set_last_error(std::string("unexpected exception: ") + error.what());
        return ELYSIUMKV_UNUSABLE;
    } catch (...) {
        set_last_error("unexpected exception");
        return ELYSIUMKV_UNUSABLE;
    }
}

/// For the void- and value-returning entry points, where there is no status to
/// carry the failure.
template <typename Body>
auto guard_value(Body&& body, decltype(body()) fallback) noexcept -> decltype(body()) {
    try {
        return body();
    } catch (const std::exception& error) {
        set_last_error(std::string("unexpected exception: ") + error.what());
        return fallback;
    } catch (...) {
        set_last_error("unexpected exception");
        return fallback;
    }
}

elysiumkv_status fail(Status status, std::string message) {
    set_last_error(std::move(message));
    return to_c(status);
}

Slice as_slice(const uint8_t* data, size_t len) {
    return data == nullptr ? Slice() : Slice(data, len);
}

/// A store supplied by a binding (the vtable seam). The engine sees an ordinary
/// `BlobStore`; the callbacks see plain C.
class VtableBlobStore final : public BlobStore {
public:
    explicit VtableBlobStore(const elysiumkv_blob_store_vtable& vtable)
        : vtable_(vtable),
          max_object_bytes_(vtable.max_object_bytes != 0 ? vtable.max_object_bytes
                                                         : (64u << 20)) {}

    std::string id() const override { return vtable_.id(vtable_.context); }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override {
        const std::string name_z(name);
        const size_t want = len == kReadToEnd ? max_object_bytes_ : len;

        Buffer buffer(want);
        size_t produced = 0;
        const elysiumkv_status status =
            vtable_.get(vtable_.context, name_z.c_str(), offset, want, buffer.data(), &produced);
        if (status != ELYSIUMKV_OK) return make_ready_future(GetResult(std::unexpected(from_c(status))));
        if (produced > want) return make_ready_future(GetResult(std::unexpected(Status::Corrupt)));

        buffer.resize(produced);
        return make_ready_future(GetResult(std::move(buffer)));
    }

    std::future<Status> put(std::string_view name, Slice bytes) override {
        const std::string name_z(name);
        return make_ready_future(
            from_c(vtable_.put(vtable_.context, name_z.c_str(), bytes.data(), bytes.size())));
    }

    std::future<Status> remove(std::string_view name) override {
        const std::string name_z(name);
        return make_ready_future(from_c(vtable_.remove(vtable_.context, name_z.c_str())));
    }

    /// Only overridden when the vtable supplies it; otherwise the base class's
    /// loop over `remove` is exactly right, and better than a wrapper that
    /// pretends to batch.
    std::future<Status> remove_many(const std::vector<std::string>& names) override {
        if (vtable_.remove_many == nullptr) return BlobStore::remove_many(names);
        if (names.empty()) return make_ready_future(Status::Ok);

        // The callback takes `const char* const*`, so the C strings have to exist
        // as an array. Held by the vector of strings beside it.
        std::vector<std::string> owned(names.begin(), names.end());
        std::vector<const char*> pointers;
        pointers.reserve(owned.size());
        for (const std::string& name : owned) pointers.push_back(name.c_str());

        return make_ready_future(
            from_c(vtable_.remove_many(vtable_.context, pointers.data(), pointers.size())));
    }

    std::future<ListResult> list(std::string_view prefix) override {
        const std::string prefix_z(prefix);
        std::vector<std::string> names;
        const elysiumkv_status status = vtable_.list(
            vtable_.context, prefix_z.c_str(),
            [](void* context, const char* name) {
                static_cast<std::vector<std::string>*>(context)->emplace_back(name);
            },
            &names);
        if (status != ELYSIUMKV_OK) {
            return make_ready_future(ListResult(std::unexpected(from_c(status))));
        }
        return make_ready_future(ListResult(std::move(names)));
    }

private:
    static Status from_c(elysiumkv_status status) {
        switch (status) {
            case ELYSIUMKV_OK: return Status::Ok;
            case ELYSIUMKV_NOT_FOUND: return Status::NotFound;
            case ELYSIUMKV_CORRUPT: return Status::Corrupt;
            case ELYSIUMKV_UNUSABLE: return Status::Unusable;
            case ELYSIUMKV_FENCED: return Status::Fenced;
            case ELYSIUMKV_CONFIG: return Status::Config;
            case ELYSIUMKV_IO: return Status::Io;
            case ELYSIUMKV_STALLED: return Status::Stalled;
            case ELYSIUMKV_UNSUPPORTED: return Status::Unsupported;
            case ELYSIUMKV_STALE: return Status::Stale;
        }
        // An unknown code is emphatically not absence: treat it as "could not
        // determine", which is the non-destructive reading (ARCHITECTURE.md - Immutable named objects).
        return Status::Io;
    }

    elysiumkv_blob_store_vtable vtable_;
    size_t max_object_bytes_;
};

}  // namespace
}  // namespace elysiumkv

using namespace elysiumkv;

/// The options object the C side builds up. Held by value so the engine's
/// `Options` never crosses the boundary.
struct elysiumkv_options {
    Options options;
    size_t block_cache_bytes = 8ull << 20;
};

struct elysiumkv_batch {
    WriteBatch batch;
};

struct elysiumkv_db {
    /// Exactly one of these is set. **The C ABI cannot express the C++ type split**, where passing
    /// a read-only handle somewhere that writes is a compile error, so the distinction lives here
    /// and the write entry points refuse on a handle that has no `db`.
    std::unique_ptr<DB> db;
    std::unique_ptr<ReadOnlyDB> reader;

    /// The read surface, whichever kind of handle this is.
    ReadOnlyDB* reads() const { return db != nullptr ? db.get() : reader.get(); }
    /// The write surface, or null on a read-only handle.
    DB* writes() const { return db.get(); }
    std::shared_ptr<BlockCache> block_cache;
    std::vector<std::string> discarded_stores;
    uint64_t discarded_files = 0;

    /// ARCHITECTURE.md "The ABI boundary" — the pin registry. A handle rather than a pointer, so a binding
    /// cannot fabricate one by arithmetic, and so a double unpin is a lookup
    /// miss rather than a double free.
    std::mutex pins_mutex;
    std::map<uint64_t, Pinned> pins;
    uint64_t next_pin = 1;

    /// Live iterator handles. The caller owns those objects and will free them
    /// on its own schedule — including, when a binding gets it wrong, after the
    /// DB is closed. Closing therefore *detaches* them: their engine state is
    /// released while the engine is still alive, and the empty shell is left for
    /// the caller to destroy safely.
    std::mutex iters_mutex;
    std::set<elysiumkv_iter*> iters;
};

struct elysiumkv_iter {
    std::unique_ptr<Iterator> iterator;
    elysiumkv_db* owner = nullptr;
    /// Set when a batch filled up with the iterator already positioned on an
    /// entry that did not fit. The entry is not stored — the iterator is still
    /// sitting on it, so the next call re-reads it rather than holding slices
    /// that only stay valid until the next advance.
    bool pending_entry = false;
};

extern "C" {

const char* elysiumkv_last_error(void) { return last_error_slot().c_str(); }

/// Generated from `ELYSIUMKV_VERSION`, which the publish workflow sets from the
/// release tag. Not a literal here: that would be a second place to remember at
/// release time, and the one that gets forgotten — leaving a jar built from a new
/// tag whose native library answers with the old number, to the very question
/// asked in order to identify what was loaded.
const char* elysiumkv_version(void) { return ELYSIUMKV_VERSION_STRING; }

uint32_t elysiumkv_features(void) {
#ifdef ELYSIUMKV_WITH_AWS
    return ELYSIUMKV_FEATURE_AWS;
#else
    return 0;
#endif
}

// --- configuration -----------------------------------------------------------

elysiumkv_options* elysiumkv_options_create(void) {
    return guard_value([] { return new elysiumkv_options(); }, nullptr);
}

void elysiumkv_options_destroy(elysiumkv_options* options) { delete options; }

elysiumkv_status elysiumkv_options_add_tier(elysiumkv_options* options, void* store,
                                        elysiumkv_durability durability, int64_t max_age_ms,
                                        int64_t max_bytes, int64_t stall_age_ms) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr || store == nullptr) {
            return fail(Status::Config, "elysiumkv_options_add_tier: null options or store");
        }
        Tier tier;
        tier.store = *static_cast<std::shared_ptr<BlobStore>*>(store);
        tier.durability =
            durability == ELYSIUMKV_TRANSIENT ? Durability::Transient : Durability::Durable;
        if (max_age_ms > 0) tier.max_age = Duration(max_age_ms);
        if (max_bytes > 0) tier.max_bytes = static_cast<size_t>(max_bytes);
        if (stall_age_ms > 0) tier.stall_age = Duration(stall_age_ms);
        options->options.tiers.push_back(std::move(tier));
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_options_set_level(elysiumkv_options* options, int level,
                                         elysiumkv_compression compression, int64_t max_bytes,
                                         int max_files, int slowdown_at, int stop_at,
                                         size_t target_file_bytes) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr || level < 0) {
            return fail(Status::Config, "elysiumkv_options_set_level: null options or bad level");
        }
        LevelOptions entry;
        entry.compression = static_cast<Compression>(compression);
        if (max_bytes > 0) entry.max_bytes = static_cast<size_t>(max_bytes);
        if (max_files > 0) entry.max_files = max_files;
        if (slowdown_at > 0) entry.slowdown_at = slowdown_at;
        if (stop_at > 0) entry.stop_at = stop_at;
        if (target_file_bytes > 0) entry.target_file_bytes = target_file_bytes;
        options->options.levels[level] = std::move(entry);
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_options_configure(elysiumkv_options* options, void* manifest_catalog,
                                         void* memory_budget, size_t memtable_bytes,
                                         size_t block_bytes,
                                         size_t block_cache_bytes, size_t reader_cache_bytes,
                                         int bloom_bits_per_key,
                                         size_t max_compaction_bytes,
                                         int manifest_edits_per_generation, int paranoid_checks,
                                         int block_on_stall, uint64_t flush_interval_ms,
                                         uint64_t maintenance_interval_ms,
                                         uint64_t obsolete_retention_ms,
                                         uint64_t orphan_retention_ms,
                                         uint64_t orphan_sweep_interval_ms) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr) {
            return fail(Status::Config, "elysiumkv_options_configure: null options");
        }
        if (manifest_catalog != nullptr) {
            options->options.manifest_catalog =
                *static_cast<std::shared_ptr<ManifestCatalog>*>(manifest_catalog);
        }
        if (memory_budget != nullptr) {
            options->options.memory_budget =
                *static_cast<std::shared_ptr<MemoryBudget>*>(memory_budget);
        }
        if (memtable_bytes > 0) options->options.memtable_bytes = memtable_bytes;
        if (block_bytes > 0) options->options.block_bytes = block_bytes;
        if (block_cache_bytes > 0) options->block_cache_bytes = block_cache_bytes;
        if (reader_cache_bytes > 0) options->options.reader_cache_bytes = reader_cache_bytes;
        if (bloom_bits_per_key > 0) options->options.bloom_bits_per_key = bloom_bits_per_key;
        if (max_compaction_bytes > 0) options->options.max_compaction_bytes = max_compaction_bytes;
        if (manifest_edits_per_generation > 0) {
            options->options.manifest_edits_per_generation = manifest_edits_per_generation;
        }
        if (paranoid_checks >= 0) options->options.paranoid_checks = paranoid_checks > 0;
        if (block_on_stall >= 0) options->options.block_on_stall = block_on_stall > 0;
        if (flush_interval_ms > 0) {
            options->options.flush_interval = std::chrono::milliseconds(flush_interval_ms);
        }
        if (maintenance_interval_ms > 0) {
            options->options.maintenance_interval =
                std::chrono::milliseconds(maintenance_interval_ms);
        }
        if (obsolete_retention_ms > 0) {
            options->options.obsolete_retention = std::chrono::milliseconds(obsolete_retention_ms);
        }
        if (orphan_retention_ms > 0) {
            options->options.orphan_retention = std::chrono::milliseconds(orphan_retention_ms);
        }
        if (orphan_sweep_interval_ms > 0) {
            options->options.orphan_sweep_interval =
                std::chrono::milliseconds(orphan_sweep_interval_ms);
        }
        return ELYSIUMKV_OK;
    });
}

// --- seams -------------------------------------------------------------------

void* elysiumkv_disk_blob_store_create(const char* root_directory, const char* store_id) {
    return guard_value(
        [&]() -> void* {
            if (root_directory == nullptr) return nullptr;
            auto store = std::make_shared<DiskBlobStore>(
                root_directory, store_id == nullptr ? std::string() : std::string(store_id));
            return new std::shared_ptr<BlobStore>(std::move(store));
        },
        nullptr);
}

void elysiumkv_blob_store_destroy(void* store) {
    delete static_cast<std::shared_ptr<BlobStore>*>(store);
}

void* elysiumkv_blob_store_from_vtable(const elysiumkv_blob_store_vtable* vtable) {
    return guard_value(
        [&]() -> void* {
            if (vtable == nullptr || vtable->id == nullptr || vtable->get == nullptr ||
                vtable->put == nullptr || vtable->remove == nullptr || vtable->list == nullptr) {
                set_last_error("elysiumkv_blob_store_from_vtable: incomplete vtable");
                return nullptr;
            }
            return new std::shared_ptr<BlobStore>(std::make_shared<VtableBlobStore>(*vtable));
        },
        nullptr);
}

void* elysiumkv_disk_manifest_catalog_create(const char* directory) {
    return guard_value(
        [&]() -> void* {
            if (directory == nullptr) return nullptr;
            auto catalog = std::make_shared<DiskManifestCatalog>(directory);
            return new std::shared_ptr<ManifestCatalog>(std::move(catalog));
        },
        nullptr);
}

void elysiumkv_manifest_catalog_destroy(void* catalog) {
    delete static_cast<std::shared_ptr<ManifestCatalog>*>(catalog);
}

// --- the shared memory budget -------------------------------------------------

void* elysiumkv_memory_budget_create(size_t total_bytes) {
    return guard_value(
        [&]() -> void* {
            if (total_bytes == 0) {
                set_last_error("elysiumkv_memory_budget_create: total_bytes must be non-zero");
                return nullptr;
            }
            return new std::shared_ptr<MemoryBudget>(std::make_shared<MemoryBudget>(total_bytes));
        },
        nullptr);
}

void elysiumkv_memory_budget_destroy(void* budget) {
    delete static_cast<std::shared_ptr<MemoryBudget>*>(budget);
}

size_t elysiumkv_memory_budget_used(const void* budget) {
    if (budget == nullptr) return 0;
    return (*static_cast<const std::shared_ptr<MemoryBudget>*>(budget))->used();
}

// --- cache layers -------------------------------------------------------------

extern "C++" {
namespace {

/// Both cache constructors need the same three things checked, and a cache built
/// over a null delegate would be a store that reads from nothing.
Result<std::shared_ptr<BlobStore>> cache_delegate(void* delegate) {
    if (delegate == nullptr) return std::unexpected(Status::Config);
    auto* held = static_cast<std::shared_ptr<BlobStore>*>(delegate);
    if (*held == nullptr) return std::unexpected(Status::Config);
    return *held;
}

}  // namespace
}  // extern "C++"

elysiumkv_status elysiumkv_options_configure_compaction(elysiumkv_options* options,
                                                double tombstone_density_trigger,
                                                uint64_t tombstone_density_min_entries) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr) {
            return fail(Status::Config, "elysiumkv_options_configure_compaction: null options");
        }
        // A negative or absurd fraction is a mistake rather than an intent: a trigger above one can
        // never be reached, since a file cannot hold more tombstones than entries, so it would read
        // as "off" while looking configured.
        if (tombstone_density_trigger < 0.0 || tombstone_density_trigger > 1.0) {
            return fail(Status::Config,
                        "elysiumkv_options_configure_compaction: tombstone_density_trigger must be "
                        "a fraction between 0 and 1");
        }
        options->options.tombstone_density_trigger = tombstone_density_trigger;
        if (tombstone_density_min_entries > 0) {
            options->options.tombstone_density_min_entries = tombstone_density_min_entries;
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_options_configure_jitter(elysiumkv_options* options, double age_jitter,
                                                    double flush_interval_jitter) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr) {
            return fail(Status::Config, "elysiumkv_options_configure_jitter: null options");
        }
        // Written to reject NaN as well, which would otherwise pass every comparison and reach
        // the engine as a window of unpredictable width.
        if (!(age_jitter >= 0.0) || age_jitter > 1.0 ||
            !(flush_interval_jitter >= 0.0) || flush_interval_jitter > 1.0) {
            return fail(Status::Config,
                        "elysiumkv_options_configure_jitter: both jitters must be a fraction "
                        "between 0 and 1");
        }
        options->options.age_jitter = age_jitter;
        options->options.flush_interval_jitter = flush_interval_jitter;
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_options_set_logger(elysiumkv_options* options,
                                        const elysiumkv_logger_vtable* vtable, int min_level) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr) return fail(Status::Config, "elysiumkv_options_set_logger: null options");
        if (min_level < static_cast<int>(LogLevel::Debug) ||
            min_level > static_cast<int>(LogLevel::Off)) {
            return fail(Status::Config, "elysiumkv_options_set_logger: min_level is out of range");
        }
        options->options.min_log_level = static_cast<LogLevel>(min_level);
        // A null vtable is how a caller turns logging off, so it is not an error.
        if (vtable == nullptr || vtable->write == nullptr) {
            options->options.logger.reset();
            return ELYSIUMKV_OK;
        }
        // A trampoline rather than a cast between function pointer types: the enums are `int`
        // underneath, but calling through a differently-typed pointer is undefined even so. The
        // holder owns the caller's vtable by value, so the caller may let theirs go out of scope.
        struct Holder {
            Logger logger;
            elysiumkv_logger_vtable vtable;
        };
        auto holder = std::make_shared<Holder>();
        holder->vtable = *vtable;
        holder->logger.context = holder.get();
        holder->logger.write = [](void* context, LogLevel level, LogEvent event,
                                  const char* message, size_t len) {
            auto* self = static_cast<Holder*>(context);
            self->vtable.write(self->vtable.context, static_cast<int>(level),
                               static_cast<int>(event), message, len);
        };
        // Aliasing: keeps the holder alive, hands the engine the `Logger` inside it.
        options->options.logger = std::shared_ptr<Logger>(holder, &holder->logger);
        return ELYSIUMKV_OK;
    });
}

static_assert(static_cast<int>(LogLevel::Off) == ELYSIUMKV_LOG_OFF);
static_assert(static_cast<int>(LogEvent::OrphansReclaimed) == ELYSIUMKV_EVENT_ORPHANS_RECLAIMED);
static_assert(static_cast<int>(LogEvent::BackgroundRetry) == ELYSIUMKV_EVENT_BACKGROUND_RETRY);

elysiumkv_status elysiumkv_disk_cache_blob_store_create(void* delegate, const char* directory,
                                                    size_t max_cache_bytes, int cache_on_write,
                                                    void** out) {
    return elysiumkv_disk_cache_blob_store_create_chunked(delegate, directory, max_cache_bytes,
                                                      cache_on_write, /*fetch_granularity=*/0, out);
}

elysiumkv_status elysiumkv_disk_cache_blob_store_create_chunked(void* delegate,
                                                        const char* directory,
                                                        size_t max_cache_bytes, int cache_on_write,
                                                        size_t fetch_granularity, void** out) {
    return guard([&]() -> elysiumkv_status {
        if (out == nullptr) {
            return fail(Status::Config, "elysiumkv_disk_cache_blob_store_create: out is null");
        }
        *out = nullptr;

        auto below = cache_delegate(delegate);
        if (!below) {
            return fail(Status::Config,
                        "elysiumkv_disk_cache_blob_store_create: delegate is required");
        }
        if (directory == nullptr || *directory == '\0' || max_cache_bytes == 0) {
            return fail(Status::Config,
                        "elysiumkv_disk_cache_blob_store_create: directory and a non-zero "
                        "max_cache_bytes are required");
        }

        auto cache = std::make_shared<DiskCacheBlobStore>(*below, directory, max_cache_bytes,
                                                          cache_on_write != 0, fetch_granularity);
        *out = new std::shared_ptr<BlobStore>(std::move(cache));
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_blob_cache_stats(void* store, uint64_t* hits, uint64_t* misses) {
    return guard([&]() -> elysiumkv_status {
        if (store == nullptr || hits == nullptr || misses == nullptr) {
            return fail(Status::Config, "elysiumkv_blob_cache_stats: store, hits and misses are required");
        }
        auto* handle = static_cast<std::shared_ptr<BlobStore>*>(store);
        // as_cache rather than a dynamic_cast: this builds without RTTI.
        CacheBlobStore* cache = handle->get()->as_cache();
        if (cache == nullptr) {
            return fail(Status::Config, "elysiumkv_blob_cache_stats: not a caching blob store");
        }
        *hits = cache->hits();
        *misses = cache->misses();
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_memory_cache_blob_store_create(void* delegate, void* budget,
                                                     size_t max_cache_bytes, int cache_on_write,
                                                     void** out) {
    return elysiumkv_memory_cache_blob_store_create_chunked(delegate, budget, max_cache_bytes,
                                                        cache_on_write, /*fetch_granularity=*/0,
                                                        out);
}

elysiumkv_status elysiumkv_memory_cache_blob_store_create_chunked(void* delegate, void* budget,
                                                          size_t max_cache_bytes,
                                                          int cache_on_write,
                                                          size_t fetch_granularity, void** out) {
    return guard([&]() -> elysiumkv_status {
        if (out == nullptr) {
            return fail(Status::Config, "elysiumkv_memory_cache_blob_store_create: out is null");
        }
        *out = nullptr;

        auto below = cache_delegate(delegate);
        if (!below) {
            return fail(Status::Config,
                        "elysiumkv_memory_cache_blob_store_create: delegate is required");
        }
        if (max_cache_bytes == 0) {
            return fail(Status::Config,
                        "elysiumkv_memory_cache_blob_store_create: max_cache_bytes is required");
        }

        std::shared_ptr<MemoryBudget> shared;
        if (budget != nullptr) shared = *static_cast<std::shared_ptr<MemoryBudget>*>(budget);
        auto cache = std::make_shared<MemoryCacheBlobStore>(*below, std::move(shared),
                                                            max_cache_bytes, cache_on_write != 0,
                                                            fetch_granularity);
        *out = new std::shared_ptr<BlobStore>(std::move(cache));
        return ELYSIUMKV_OK;
    });
}

// --- remote seams -------------------------------------------------------------

#ifndef ELYSIUMKV_WITH_AWS

// **CONFIG, not UNUSABLE.** Asking for S3 from a build that has no S3 is a
// mistake in how the process was assembled, and the message has to name the
// build option — otherwise the failure reads as "S3 is broken" rather than "this
// library does not contain it", and the next hour goes into the wrong place.
namespace {
elysiumkv_status no_aws(const char* what) {
    return fail(Status::Config,
                std::string(what) + ": this library was built without ELYSIUMKV_BUILD_AWS, so the "
                                    "S3 and DynamoDB implementations are not compiled in "
                                    "(elysiumkv_features() reports ELYSIUMKV_FEATURE_AWS when they are)");
}
}  // namespace

elysiumkv_status elysiumkv_s3_blob_store_create(const char*, const char*, const char*, const char*,
                                            const char*, const char*, int64_t, int64_t,
                                            const char*, void** out) {
    if (out != nullptr) *out = nullptr;
    return no_aws("elysiumkv_s3_blob_store_create");
}

elysiumkv_status elysiumkv_s3_manifest_catalog_create(const char*, const char*, const char*,
                                                  const char*, const char*, const char*, int64_t,
                                                  int64_t, void** out) {
    if (out != nullptr) *out = nullptr;
    return no_aws("elysiumkv_s3_manifest_catalog_create");
}

elysiumkv_status elysiumkv_dynamo_manifest_catalog_create(const char*, const char*, const char*,
                                                      const char*, const char*, const char*,
                                                      int64_t, int, void** out) {
    if (out != nullptr) *out = nullptr;
    return no_aws("elysiumkv_dynamo_manifest_catalog_create");
}

#else

// An anonymous namespace inside a linkage-specification does not escape it:
// these helpers would get C language linkage and be rejected for returning
// `std::string` and `std::expected`. Everything else in this file that needs a
// helper happens to return a C-compatible type, which is why the block below is
// the first place it comes up.
extern "C++" {
namespace {

/// NULL is "unset", which for a credential means the SDK's own chain and for a
/// prefix means the bucket root. An empty C string means the same thing, so the
/// two spellings cannot diverge.
std::string text(const char* value) { return value == nullptr ? std::string() : value; }

/// A negative timeout would become an enormous unsigned duration, so it is
/// rejected rather than silently turned into "never time out".
Result<std::chrono::milliseconds> duration(int64_t millis, std::chrono::milliseconds fallback) {
    if (millis < 0) return std::unexpected(Status::Config);
    if (millis == 0) return fallback;
    return std::chrono::milliseconds(millis);
}

Result<S3Options> s3_options(const char* bucket, const char* prefix, const char* region,
                             const char* endpoint, const char* access_key, const char* secret_key,
                             int64_t point_timeout_ms, int64_t bulk_timeout_ms) {
    S3Options options;
    if (bucket == nullptr || *bucket == '\0') return std::unexpected(Status::Config);
    options.bucket = bucket;
    options.prefix = text(prefix);
    if (region != nullptr && *region != '\0') options.region = region;
    options.endpoint = text(endpoint);
    options.access_key = text(access_key);
    options.secret_key = text(secret_key);

    auto point = duration(point_timeout_ms, options.point_timeout);
    if (!point) return std::unexpected(point.error());
    auto bulk = duration(bulk_timeout_ms, options.bulk_timeout);
    if (!bulk) return std::unexpected(bulk.error());
    options.point_timeout = *point;
    options.bulk_timeout = *bulk;
    return options;
}

}  // namespace
}  // extern "C++"

elysiumkv_status elysiumkv_s3_blob_store_create(const char* bucket, const char* prefix,
                                            const char* region, const char* endpoint,
                                            const char* access_key, const char* secret_key,
                                            int64_t point_timeout_ms, int64_t bulk_timeout_ms,
                                            const char* store_id, void** out) {
    return guard([&]() -> elysiumkv_status {
        if (out == nullptr) return fail(Status::Config, "elysiumkv_s3_blob_store_create: out is null");
        *out = nullptr;

        auto options = s3_options(bucket, prefix, region, endpoint, access_key, secret_key,
                                  point_timeout_ms, bulk_timeout_ms);
        if (!options) {
            return fail(options.error(),
                        "elysiumkv_s3_blob_store_create: bucket must be non-empty and timeouts "
                        "non-negative");
        }
        options->id = text(store_id);

        auto store = S3BlobStore::open(*options);
        if (!store) return fail(store.error(), "elysiumkv_s3_blob_store_create failed");
        *out = new std::shared_ptr<BlobStore>(std::move(*store));
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_s3_manifest_catalog_create(const char* bucket, const char* prefix,
                                                  const char* region, const char* endpoint,
                                                  const char* access_key, const char* secret_key,
                                                  int64_t point_timeout_ms, int64_t bulk_timeout_ms,
                                                  void** out) {
    return guard([&]() -> elysiumkv_status {
        if (out == nullptr) {
            return fail(Status::Config, "elysiumkv_s3_manifest_catalog_create: out is null");
        }
        *out = nullptr;

        auto options = s3_options(bucket, prefix, region, endpoint, access_key, secret_key,
                                  point_timeout_ms, bulk_timeout_ms);
        if (!options) {
            return fail(options.error(),
                        "elysiumkv_s3_manifest_catalog_create: bucket must be non-empty and timeouts "
                        "non-negative");
        }

        auto catalog = S3ManifestCatalog::open(*options);
        if (!catalog) return fail(catalog.error(), "elysiumkv_s3_manifest_catalog_create failed");
        *out = new std::shared_ptr<ManifestCatalog>(std::move(*catalog));
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_dynamo_manifest_catalog_create(const char* table, const char* store_id,
                                                      const char* region, const char* endpoint,
                                                      const char* access_key,
                                                      const char* secret_key, int64_t timeout_ms,
                                                      int create_table_if_missing, void** out) {
    return guard([&]() -> elysiumkv_status {
        if (out == nullptr) {
            return fail(Status::Config, "elysiumkv_dynamo_manifest_catalog_create: out is null");
        }
        *out = nullptr;

        DynamoOptions options;
        if (table == nullptr || *table == '\0' || store_id == nullptr || *store_id == '\0') {
            return fail(Status::Config,
                        "elysiumkv_dynamo_manifest_catalog_create: table and store_id are required");
        }
        options.table = table;
        options.store_id = store_id;
        if (region != nullptr && *region != '\0') options.region = region;
        options.endpoint = text(endpoint);
        options.access_key = text(access_key);
        options.secret_key = text(secret_key);
        options.create_table_if_missing = create_table_if_missing != 0;

        auto timeout = duration(timeout_ms, options.timeout);
        if (!timeout) {
            return fail(Status::Config,
                        "elysiumkv_dynamo_manifest_catalog_create: timeout must be non-negative");
        }
        options.timeout = *timeout;

        // The one call here that touches the network, and the reason these
        // constructors return a status: an unreachable DynamoDB is ELYSIUMKV_IO and
        // worth retrying, a wrong table name is ELYSIUMKV_CONFIG and is not.
        auto catalog = DynamoManifestCatalog::open(options);
        if (!catalog) {
            return fail(catalog.error(), "elysiumkv_dynamo_manifest_catalog_create failed");
        }
        *out = new std::shared_ptr<ManifestCatalog>(std::move(*catalog));
        return ELYSIUMKV_OK;
    });
}

#endif  // ELYSIUMKV_WITH_AWS

// --- open and close -----------------------------------------------------------

namespace {

elysiumkv_status open_common(const elysiumkv_options* options, elysiumkv_db** out, bool guarded,
                           const char** discarded_stores, size_t* n_stores,
                           uint64_t* discarded_files, bool* requires_recovery) {
    if (options == nullptr || out == nullptr) {
        return fail(Status::Config, "open: null options or out parameter");
    }

    auto handle = std::make_unique<elysiumkv_db>();
    Options copy = options->options;
    handle->block_cache = std::make_shared<ShardedLruBlockCache>(options->block_cache_bytes);
    copy.block_cache = handle->block_cache;

    auto opened = guarded ? DB::open(copy).transform([](std::unique_ptr<DB> db) {
        OpenResult result;
        result.db = std::move(db);
        return result;
    }) : DB::open_with_result(copy);

    if (!opened) {
        return fail(opened.error(), std::string("open failed: ") +
                                       std::string(status_name(opened.error())));
    }

    handle->db = std::move(opened->db);
    handle->discarded_stores = std::move(opened->discarded_stores);
    handle->discarded_files = opened->discarded_files;

    if (n_stores != nullptr) {
        const size_t capacity = *n_stores;
        *n_stores = handle->discarded_stores.size();
        if (discarded_stores != nullptr) {
            for (size_t i = 0; i < handle->discarded_stores.size() && i < capacity; ++i) {
                discarded_stores[i] = handle->discarded_stores[i].c_str();
            }
        }
    }
    if (discarded_files != nullptr) *discarded_files = handle->discarded_files;
    if (requires_recovery != nullptr) *requires_recovery = opened->requires_recovery;

    *out = handle.release();
    return ELYSIUMKV_OK;
}

}  // namespace

elysiumkv_status elysiumkv_open_read_only(const elysiumkv_options* options, elysiumkv_db** out) {
    return guard([&]() -> elysiumkv_status {
        if (options == nullptr || out == nullptr) {
            return fail(Status::Config, "elysiumkv_open_read_only: null options or out");
        }
        Options read_options = options->options;
        // The sweep deletes objects, and a reader has authority to delete nothing. Forced here
        // rather than trusted to the caller, because the setting is about what a *writer* may do.
        read_options.orphan_sweep_interval.reset();

        auto handle = std::make_unique<elysiumkv_db>();
        handle->block_cache = std::make_shared<ShardedLruBlockCache>(options->block_cache_bytes);
        read_options.block_cache = handle->block_cache;

        auto opened = DB::open_read_only(read_options);
        if (!opened) {
            return fail(opened.error(), std::string("read-only open failed: ") +
                                            std::string(status_name(opened.error())));
        }
        handle->reader = std::move(*opened);
        *out = handle.release();
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_refresh(elysiumkv_db* db) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_refresh: null db");
        return to_c(db->reads()->refresh());
    });
}

elysiumkv_status elysiumkv_open(const elysiumkv_options* options, elysiumkv_db** out) {
    return guard([&] {
        return open_common(options, out, /*guarded=*/true, nullptr, nullptr, nullptr, nullptr);
    });
}

elysiumkv_status elysiumkv_open_with_result(const elysiumkv_options* options, elysiumkv_db** out,
                                        const char** discarded_stores, size_t* n_stores,
                                        uint64_t* discarded_files, bool* requires_recovery) {
    return guard([&] {
        return open_common(options, out, /*guarded=*/false, discarded_stores, n_stores,
                           discarded_files, requires_recovery);
    });
}

namespace {

uint64_t close_db(elysiumkv_db* db, bool flush_first) {
    if (db == nullptr) return 0;
    // Before the handle bookkeeping, because it is the engine that owns the decision and the
    // destructor is where the attempt happens. See `DB::abandon_unflushed`.
    if (!flush_first && db->db != nullptr) db->db->abandon_unflushed();

    uint64_t outstanding = 0;
    {
        std::lock_guard<std::mutex> lock(db->pins_mutex);
        outstanding = db->pins.size();
    }
    {
        std::lock_guard<std::mutex> lock(db->iters_mutex);
        outstanding += db->iters.size();
        // Detach rather than free: the handle belongs to the caller, and
        // a destroy after close must not be a use-after-free.
        for (elysiumkv_iter* iter : db->iters) {
            iter->iterator.reset();
            iter->owner = nullptr;
        }
        db->iters.clear();
    }

    // ARCHITECTURE.md "The ABI boundary" — a leaked pin holds a block-cache entry forever, so this is a
    // first-class invariant rather than a diagnostic. The pins are
    // released here regardless — leaking the memory too would help
    // nobody — but the count is reported so a binding's tests can fail.
    if (outstanding != 0) {
        set_last_error("close: " + std::to_string(outstanding) +
                       " pin(s) or iterator(s) still outstanding at close");
    }
    delete db;
    return outstanding;
}

}  // namespace

uint64_t elysiumkv_close(elysiumkv_db* db) {
    return guard_value([&]() -> uint64_t { return close_db(db, /*flush_first=*/true); }, 0);
}

uint64_t elysiumkv_close_without_flush(elysiumkv_db* db) {
    return guard_value([&]() -> uint64_t { return close_db(db, /*flush_first=*/false); }, 0);
}

// --- reads --------------------------------------------------------------------

elysiumkv_status elysiumkv_get(elysiumkv_db* db, const uint8_t* key, size_t key_len,
                           const uint8_t** value, size_t* value_len, uint64_t* pin) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || value == nullptr || value_len == nullptr || pin == nullptr) {
            return fail(Status::Config, "elysiumkv_get: null argument");
        }
        auto found = db->reads()->get(as_slice(key, key_len));
        if (!found) {
            return fail(found.error(), std::string("get: ") + std::string(status_name(found.error())));
        }

        std::lock_guard<std::mutex> lock(db->pins_mutex);
        const uint64_t handle = db->next_pin++;
        *value = found->value().data();
        *value_len = found->value().size();
        db->pins.emplace(handle, std::move(*found));
        *pin = handle;
        return ELYSIUMKV_OK;
    });
}

void elysiumkv_unpin(elysiumkv_db* db, uint64_t pin) {
    if (db == nullptr) return;
    Pinned released;
    {
        std::lock_guard<std::mutex> lock(db->pins_mutex);
        auto it = db->pins.find(pin);
        // A double unpin is a lookup miss, not a double free — which is the
        // point of handing out handles rather than pointers.
        if (it == db->pins.end()) return;
        released = std::move(it->second);
        db->pins.erase(it);
    }
}

elysiumkv_status elysiumkv_get_copy(elysiumkv_db* db, const uint8_t* key, size_t key_len, uint8_t* value,
                                size_t capacity, size_t* value_len) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || value_len == nullptr) {
            return fail(Status::Config, "elysiumkv_get_copy: null argument");
        }
        auto found = db->reads()->get(as_slice(key, key_len));
        if (!found) {
            return fail(found.error(),
                        std::string("get_copy: ") + std::string(status_name(found.error())));
        }
        const Slice slice = found->value();
        *value_len = slice.size();
        if (value != nullptr && capacity > 0) {
            const size_t to_copy = slice.size() < capacity ? slice.size() : capacity;
            std::memcpy(value, slice.data(), to_copy);
        }
        return ELYSIUMKV_OK;
    });
}

uint64_t elysiumkv_pins_outstanding(const elysiumkv_db* db) {
    if (db == nullptr) return 0;
    auto* mutable_db = const_cast<elysiumkv_db*>(db);
    std::lock_guard<std::mutex> lock(mutable_db->pins_mutex);
    return mutable_db->pins.size();
}

// --- writes -------------------------------------------------------------------

elysiumkv_status elysiumkv_put(elysiumkv_db* db, const uint8_t* key, size_t key_len,
                           const uint8_t* value, size_t value_len) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_put: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_put: handle is read-only");
        }
        const Status status = db->writes()->put(as_slice(key, key_len), as_slice(value, value_len));
        if (status != Status::Ok) {
            return fail(status, std::string("put: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_delete_range(elysiumkv_db* db, const uint8_t* lower, size_t lower_len,
                                        const uint8_t* upper, size_t upper_len) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_delete_range: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_delete_range: handle is read-only");
        }
        const Status status =
            db->writes()->delete_range(as_slice(lower, lower_len), as_slice(upper, upper_len));
        if (status != Status::Ok) {
            return fail(status, std::string("delete_range: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_range_is_erased(elysiumkv_db* db, const uint8_t* lower,
                                           size_t lower_len, const uint8_t* upper,
                                           size_t upper_len, int* erased) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || erased == nullptr) {
            return fail(Status::Config, "elysiumkv_range_is_erased: db and erased are required");
        }
        // Reads only, so a read-only handle answers it too — which is the handle an auditor is
        // most likely to be holding.
        auto result = db->reads()->range_is_erased(as_slice(lower, lower_len),
                                                   as_slice(upper, upper_len));
        if (!result) {
            return fail(result.error(),
                        std::string("range_is_erased: ") + std::string(status_name(result.error())));
        }
        *erased = *result ? 1 : 0;
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_truncate_below(elysiumkv_db* db, const uint8_t* key, size_t key_len) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_truncate_below: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_truncate_below: handle is read-only");
        }
        const Status status = db->writes()->truncate_below(as_slice(key, key_len));
        if (status != Status::Ok) {
            return fail(status, std::string("truncate_below: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_delete(elysiumkv_db* db, const uint8_t* key, size_t key_len) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_delete: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_remove: handle is read-only");
        }
        const Status status = db->writes()->remove(as_slice(key, key_len));
        if (status != Status::Ok) {
            return fail(status, std::string("delete: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_batch* elysiumkv_batch_create(void) {
    return guard_value([] { return new elysiumkv_batch(); }, nullptr);
}

void elysiumkv_batch_destroy(elysiumkv_batch* batch) { delete batch; }

void elysiumkv_batch_put(elysiumkv_batch* batch, const uint8_t* key, size_t key_len,
                       const uint8_t* value, size_t value_len) {
    if (batch == nullptr) return;
    (void)guard([&]() -> elysiumkv_status {
        batch->batch.put(as_slice(key, key_len), as_slice(value, value_len));
        return ELYSIUMKV_OK;
    });
}

void elysiumkv_batch_delete(elysiumkv_batch* batch, const uint8_t* key, size_t key_len) {
    if (batch == nullptr) return;
    (void)guard([&]() -> elysiumkv_status {
        batch->batch.remove(as_slice(key, key_len));
        return ELYSIUMKV_OK;
    });
}

void elysiumkv_batch_delete_range(elysiumkv_batch* batch, const uint8_t* lower, size_t lower_len,
                                  const uint8_t* upper, size_t upper_len) {
    if (batch == nullptr) return;
    (void)guard([&]() -> elysiumkv_status {
        batch->batch.delete_range(as_slice(lower, lower_len), as_slice(upper, upper_len));
        return ELYSIUMKV_OK;
    });
}

size_t elysiumkv_batch_size(const elysiumkv_batch* batch) {
    return batch == nullptr ? 0 : batch->batch.size();
}

elysiumkv_status elysiumkv_write(elysiumkv_db* db, elysiumkv_batch* batch) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || batch == nullptr) {
            return fail(Status::Config, "elysiumkv_write: null db or batch");
        }
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_write: handle is read-only");
        }
        const Status status = db->writes()->write(batch->batch);
        if (status != Status::Ok) {
            return fail(status, std::string("write: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_flush(elysiumkv_db* db) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_flush: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_flush: handle is read-only");
        }
        const Status status = db->writes()->flush();
        if (status != Status::Ok) {
            return fail(status, std::string("flush: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_compact_level(elysiumkv_db* db, int level) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_compact_level: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_compact_level: handle is read-only");
        }
        const Status status = db->writes()->compact_level(level);
        if (status != Status::Ok) {
            return fail(status,
                        std::string("compact_level: ") + std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

// --- iteration ----------------------------------------------------------------

elysiumkv_status elysiumkv_iter_create(elysiumkv_db* db, const uint8_t* lo, size_t lo_len,
                                   const uint8_t* hi, size_t hi_len, elysiumkv_iter** out) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || out == nullptr) {
            return fail(Status::Config, "elysiumkv_iter_create: null argument");
        }
        auto handle = std::make_unique<elysiumkv_iter>();
        handle->owner = db;
        // Each bound is independent. Folding them together — "unbounded only if
        // both are null" — made `[lo, end)` ask for `[lo, "")` instead, which is
        // the empty range: the scan returned nothing and looked like a store with
        // no data rather than a mistake.
        if (hi == nullptr) {
            handle->iterator = lo == nullptr ? db->reads()->iterator()
                                             : db->reads()->iterator(as_slice(lo, lo_len));
        } else {
            handle->iterator = db->reads()->iterator(as_slice(lo, lo_len), as_slice(hi, hi_len));
        }
        {
            std::lock_guard<std::mutex> lock(db->iters_mutex);
            db->iters.insert(handle.get());
        }
        *out = handle.release();
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_iter_prefix(elysiumkv_db* db, const uint8_t* prefix, size_t prefix_len,
                                   elysiumkv_iter** out) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || out == nullptr) {
            return fail(Status::Config, "elysiumkv_iter_prefix: null argument");
        }
        auto handle = std::make_unique<elysiumkv_iter>();
        handle->owner = db;
        handle->iterator = db->reads()->prefix_iterator(as_slice(prefix, prefix_len));
        {
            std::lock_guard<std::mutex> lock(db->iters_mutex);
            db->iters.insert(handle.get());
        }
        *out = handle.release();
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_iter_create_reverse(elysiumkv_db* db, const uint8_t* lo, size_t lo_len,
                                       const uint8_t* hi, size_t hi_len, elysiumkv_iter** out) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || out == nullptr) {
            return fail(Status::Config, "elysiumkv_iter_create_reverse: null argument");
        }
        auto handle = std::make_unique<elysiumkv_iter>();
        handle->owner = db;
        // Each bound stays independent, for the same reason as the forward call: folding them
        // together turns "from lo to the end" into the empty range.
        if (hi == nullptr) {
            handle->iterator = lo == nullptr
                                       ? db->reads()->reverse_iterator()
                                       : db->reads()->reverse_iterator(as_slice(lo, lo_len));
        } else {
            handle->iterator =
                    db->reads()->reverse_iterator(as_slice(lo, lo_len), as_slice(hi, hi_len));
        }
        {
            std::lock_guard<std::mutex> lock(db->iters_mutex);
            db->iters.insert(handle.get());
        }
        *out = handle.release();
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_iter_prefix_reverse(elysiumkv_db* db, const uint8_t* prefix,
                                       size_t prefix_len, elysiumkv_iter** out) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || out == nullptr) {
            return fail(Status::Config, "elysiumkv_iter_prefix_reverse: null argument");
        }
        auto handle = std::make_unique<elysiumkv_iter>();
        handle->owner = db;
        handle->iterator = db->reads()->reverse_prefix_iterator(as_slice(prefix, prefix_len));
        {
            std::lock_guard<std::mutex> lock(db->iters_mutex);
            db->iters.insert(handle.get());
        }
        *out = handle.release();
        return ELYSIUMKV_OK;
    });
}

bool elysiumkv_iter_next(elysiumkv_iter* iter) {
    return guard_value(
        [&]() -> bool {
            // A detached iterator — its DB was closed underneath it — is simply
            // exhausted rather than a crash.
            if (iter == nullptr || iter->iterator == nullptr) return false;
            return iter->iterator->next();
        },
        false);
}

/* Read live from the iterator rather than from a copy taken during next(). The
 * copy bought nothing — the slices are valid exactly as long as the iterator
 * stays put — and it made correctness after close() depend on those fields
 * having been blanked, which is a coupling nobody would guess at. */
void elysiumkv_iter_key(elysiumkv_iter* iter, const uint8_t** key, size_t* key_len) {
    if (iter == nullptr || key == nullptr || key_len == nullptr) return;
    const Slice slice = iter->iterator == nullptr ? Slice() : iter->iterator->key();
    *key = slice.data();
    *key_len = slice.size();
}

void elysiumkv_iter_value(elysiumkv_iter* iter, const uint8_t** value, size_t* value_len) {
    if (iter == nullptr || value == nullptr || value_len == nullptr) return;
    const Slice slice = iter->iterator == nullptr ? Slice() : iter->iterator->value();
    *value = slice.data();
    *value_len = slice.size();
}

elysiumkv_status elysiumkv_iter_next_batch(elysiumkv_iter* iter, uint8_t* buf, size_t cap,
                                       size_t* out_count, size_t* out_bytes) {
    return guard([&]() -> elysiumkv_status {
        if (iter == nullptr || out_count == nullptr || out_bytes == nullptr) {
            return fail(Status::Config, "elysiumkv_iter_next_batch: null argument");
        }
        *out_count = 0;
        *out_bytes = 0;
        if (iter->iterator == nullptr) return ELYSIUMKV_OK;  // detached by close()

        const auto put_u32 = [](uint8_t* at, uint32_t value) {
            for (int i = 0; i < 4; ++i) at[i] = static_cast<uint8_t>(value >> (8 * i));
        };

        size_t used = 0;
        while (true) {
            // A previous call left the iterator positioned on an entry it could
            // not fit. Emitting it now is what keeps a batched scan from being a
            // silently short one.
            if (!iter->pending_entry && !iter->iterator->next()) break;
            iter->pending_entry = false;

            const Slice key = iter->iterator->key();
            const Slice value = iter->iterator->value();
            const size_t needed = 8 + key.size() + value.size();

            if (used + needed > cap) {
                iter->pending_entry = true;
                // Nothing fitted at all: tell the caller how large a buffer the
                // next entry needs, rather than looking like exhaustion.
                if (used == 0) *out_bytes = needed;
                break;
            }

            put_u32(buf + used, static_cast<uint32_t>(key.size()));
            std::memcpy(buf + used + 4, key.data(), key.size());
            put_u32(buf + used + 4 + key.size(), static_cast<uint32_t>(value.size()));
            std::memcpy(buf + used + 8 + key.size(), value.data(), value.size());
            used += needed;
            ++*out_count;
        }
        if (*out_count > 0) *out_bytes = used;
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_iter_status(elysiumkv_iter* iter) {
    if (iter == nullptr || iter->iterator == nullptr) return ELYSIUMKV_CONFIG;
    return to_c(iter->iterator->status());
}

void elysiumkv_iter_destroy(elysiumkv_iter* iter) {
    if (iter == nullptr) return;
    if (iter->owner != nullptr) {
        std::lock_guard<std::mutex> lock(iter->owner->iters_mutex);
        iter->owner->iters.erase(iter);
    }
    delete iter;
}

// --- statistics ----------------------------------------------------------------

/* ARCHITECTURE.md "The ABI boundary" — one call, one `stats()`, serialized. The old shape — an accessor per
 * field — meant a binding assembling a snapshot observed a different instant per
 * call, so `compactions` did not describe the same engine state as the level
 * counts next to it. The layout is documented in elysiumkv.h; the encoding here is
 * byte-at-a-time little-endian so it neither depends on the host's byte order
 * nor assumes the caller's buffer is aligned. */
namespace {

constexpr uint32_t kStatsFormatVersion = 1;
// 32 fixed + 22 u64 scalars + the watermark presence byte and its padding. **No version bump:**
// the header declares its own length, so a decoder that starts records at `header_bytes` skips
// what it does not recognise — which is the property that made the previous seven appended
// scalars a non-event too.
constexpr uint32_t kStatsHeaderBytes = 240;
constexpr uint32_t kStatsLevelRecordBytes = 48;
constexpr uint32_t kStatsTierRecordBytes = 32;

/// Appends little-endian into a buffer it never overruns: once `full` is set the
/// writer only counts, which is what makes the size query and the real write the
/// same code path rather than two that can disagree.
class StatsWriter {
public:
    StatsWriter(uint8_t* buf, size_t cap) : buf_(buf), cap_(cap) {}

    void u8(uint8_t value) {
        if (buf_ != nullptr && written_ < cap_) buf_[written_] = value;
        ++written_;
    }
    void u32(uint32_t value) {
        for (int shift = 0; shift < 32; shift += 8) u8(static_cast<uint8_t>(value >> shift));
    }
    void i32(int32_t value) { u32(static_cast<uint32_t>(value)); }
    void u64(uint64_t value) {
        for (int shift = 0; shift < 64; shift += 8) u8(static_cast<uint8_t>(value >> shift));
    }
    void pad(size_t count) {
        for (size_t i = 0; i < count; ++i) u8(0);
    }

    size_t written() const { return written_; }

private:
    uint8_t* buf_;
    size_t cap_;
    size_t written_ = 0;
};

uint64_t millis(Duration duration) { return static_cast<uint64_t>(duration.count()); }

void encode_stats(const Stats& stats, StatsWriter& out) {
    out.u32(kStatsFormatVersion);
    out.u32(kStatsHeaderBytes);
    out.u32(kStatsLevelRecordBytes);
    out.u32(kStatsTierRecordBytes);
    out.u32(static_cast<uint32_t>(stats.levels.size()));
    out.u32(static_cast<uint32_t>(stats.tiers.size()));
    out.u8(stats.requires_recovery ? 1u : 0u);
    out.pad(7);
    out.u64(stats.memtable_bytes);
    out.u64(millis(stats.memtable_age));
    out.u64(stats.compactions);
    out.u64(stats.compaction_bytes_read);
    out.u64(stats.compaction_bytes_written);
    out.u64(stats.migrations);
    out.u64(stats.migration_bytes);
    out.u64(millis(stats.stalled_total));
    out.u64(stats.stall_count);
    out.u64(stats.block_cache_hits);
    out.u64(stats.block_cache_misses);
    out.u64(stats.block_cache_bytes);
    out.u64(stats.pins_outstanding);
    // Appended, which is why the header carries its own length: a decoder locates the
    // level records by `header_bytes`, so one built against an older layout skips
    // these rather than mis-reading everything after them.
    out.u64(stats.reader_cache_hits);
    out.u64(stats.reader_cache_misses);
    out.u64(stats.reader_cache_bytes);
    out.u64(stats.open_readers);
    out.u64(stats.memory_budget_used);
    out.u64(stats.memory_budget_total);
    out.u64(stats.budget_sheds);
    out.u64(stats.flushes);
    // The live frontier, and its presence byte beside it because zero is a valid position.
    out.u64(stats.durable_watermark.value_or(0));
    out.u8(stats.durable_watermark.has_value() ? 1u : 0u);
    out.pad(7);
    out.u64(stats.memtable_entries);
    out.u64(stats.memtable_tombstones);
    out.u64(stats.background_failures);

    for (const LevelStats& level : stats.levels) {
        out.i32(level.level);
        out.i32(level.file_count);
        out.u64(level.bytes);
        out.u64(millis(level.oldest_file_age));
        out.i32(level.files_stale_codec);
        out.u8(level.age_triggered ? 1u : 0u);
        out.u8(level.stalling ? 1u : 0u);
        out.pad(2);
        // Appended, which is why the record declares its own length: a decoder that steps by
        // `level_record_bytes` skips these rather than mis-reading the record after them.
        out.u64(level.entries);
        out.u64(level.tombstones);
    }
    for (const TierStats& tier : stats.tiers) {
        out.i32(tier.tier);
        out.i32(tier.file_count);
        out.u64(tier.bytes);
        out.u64(millis(tier.oldest_file_age));
        out.i32(tier.files_pending_migration);
        out.u8(tier.stalling ? 1u : 0u);
        out.pad(3);
    }
}

}  // namespace

elysiumkv_status elysiumkv_stats_snapshot(const elysiumkv_db* db, uint8_t* buf, size_t cap,
                                      size_t* out_bytes) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || out_bytes == nullptr) {
            return fail(Status::Config, "elysiumkv_stats_snapshot: null db or out_bytes");
        }
        const Stats stats = db->reads()->stats();

        StatsWriter measure(nullptr, 0);
        encode_stats(stats, measure);
        *out_bytes = measure.written();
        if (buf == nullptr || cap < *out_bytes) return ELYSIUMKV_OK;

        StatsWriter writer(buf, cap);
        encode_stats(stats, writer);
        return ELYSIUMKV_OK;
    });
}

elysiumkv_status elysiumkv_mark_recovery_complete(elysiumkv_db* db) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) {
            return fail(Status::Config, "elysiumkv_mark_recovery_complete: null db");
        }
        const Status status = db->reads()->mark_recovery_complete();
        if (status != Status::Ok) {
            return fail(status, std::string("mark_recovery_complete: ") +
                                    std::string(status_name(status)));
        }
        return ELYSIUMKV_OK;
    });
}

// --- watermark ----------------------------------------------------------------

elysiumkv_status elysiumkv_set_watermark(elysiumkv_db* db, uint64_t position) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr) return fail(Status::Config, "elysiumkv_set_watermark: null db");
        if (db->writes() == nullptr) {
            return fail(Status::Config, "elysiumkv_set_watermark: handle is read-only");
        }
        const Status status = db->writes()->set_watermark(position);
        if (status == Status::Config) {
            return fail(status, "elysiumkv_set_watermark: watermarks must be non-decreasing");
        }
        return to_c(status);
    });
}

elysiumkv_status elysiumkv_watermark(elysiumkv_db* db, uint64_t* out, bool* present) {
    return guard([&]() -> elysiumkv_status {
        if (db == nullptr || out == nullptr || present == nullptr) {
            return fail(Status::Config, "elysiumkv_watermark: null db, out or present");
        }
        const std::optional<uint64_t> watermark = db->reads()->recovered_watermark();
        *present = watermark.has_value();
        // Left untouched when absent: a caller that ignores `present` must not read a plausible
        // zero out of `out`, because zero is a valid position and the two are not the same
        // answer.
        if (watermark.has_value()) *out = *watermark;
        return ELYSIUMKV_OK;
    });
}

}  // extern "C"
