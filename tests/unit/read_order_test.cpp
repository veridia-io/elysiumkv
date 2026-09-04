#include "db/db_impl.hpp"
#include "support/test_db.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace elysiumkv::test {
namespace {

class ControlledCatalog final : public ManifestCatalog {
public:
    explicit ControlledCatalog(std::shared_ptr<ManifestCatalog> delegate)
        : delegate_(std::move(delegate)) {}

    Result<std::optional<Entry>> read() override { return delegate_->read(); }
    Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                 uint64_t generation) override {
        return delegate_->compare_and_set(std::move(expected), generation);
    }
    std::future<Status> put_snapshot(uint64_t generation, Slice bytes) override {
        return delegate_->put_snapshot(generation, bytes);
    }
    std::future<GetResult> get_snapshot(uint64_t generation) override {
        return delegate_->get_snapshot(generation);
    }
    std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) override {
        if (fail_next_edit_.exchange(false)) {
            return make_ready_future(Status::Io);
        }
        if (slow_edits_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(2));
        {
            std::unique_lock<std::mutex> lock(gate_);
            if (holding_) {
                entered_ = true;
                released_.wait(lock, [this] { return !holding_; });
            }
        }
        return delegate_->put_edit(generation, seq, bytes);
    }
    std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) override {
        return delegate_->get_edit(generation, seq);
    }
    std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) override {
        return delegate_->list_edits(generation);
    }
    std::future<Status> delete_generation(uint64_t generation) override {
        return delegate_->delete_generation(generation);
    }
    std::future<Result<std::vector<uint64_t>>> list_generations() override {
        return delegate_->list_generations();
    }

    void fail_next_edit() { fail_next_edit_.store(true); }
    void slow_edits() { slow_edits_.store(true); }

    /// Holds every edit inside `put_edit`, which is what keeps a flush in flight for as long as
    /// the test needs one there.
    void hold_edits() {
        std::lock_guard<std::mutex> lock(gate_);
        holding_ = true;
    }
    void release_edits() {
        {
            std::lock_guard<std::mutex> lock(gate_);
            holding_ = false;
        }
        released_.notify_all();
    }
    bool an_edit_is_held() {
        std::lock_guard<std::mutex> lock(gate_);
        return entered_;
    }

private:
    std::shared_ptr<ManifestCatalog> delegate_;
    std::atomic<bool> fail_next_edit_{false};
    std::atomic<bool> slow_edits_{false};
    std::mutex gate_;
    std::condition_variable released_;
    bool holding_ = false;
    bool entered_ = false;
};

bool settles(const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::yield();
    }
    return predicate();
}

/// A store whose flush thread is parked inside `put_edit`, with one memtable already frozen: the
/// state where the write path owes a rotation it cannot perform.
struct StalledFlush {
    TestStore store;
    std::shared_ptr<ControlledCatalog> catalog;
    std::unique_ptr<DB> db;
    // One write per memtable, and comfortably above the 4 KiB block a fresh arena charges for its
    // head node — a `memtable_bytes` below that is over budget before the first write lands.
    std::string value = std::string(128u << 10, 'v');

    ~StalledFlush() {
        // Released before the DB is torn down: a flush thread parked in the catalog never joins.
        catalog->release_edits();
    }
};

std::unique_ptr<StalledFlush> park_a_flush(bool block_on_stall) {
    auto parked = std::make_unique<StalledFlush>();
    parked->catalog = std::make_shared<ControlledCatalog>(parked->store.catalog());
    Options options = make_options(parked->store, Compression::None, /*memtable_bytes=*/64u << 10);
    options.manifest_catalog = parked->catalog;
    options.background = BackgroundMode::Threaded;
    options.block_on_stall = block_on_stall;
    options.maintenance_interval = Duration(20);

    auto opened = DB::open(options);
    if (!opened) return nullptr;
    parked->db = std::move(*opened);

    parked->catalog->hold_edits();
    // Over `memtable_bytes`, so this write seals its memtable and hands it to the flush thread.
    if (parked->db->put(Slice::from(std::string_view("a")), Slice::from(parked->value)) !=
        Status::Ok) {
        return nullptr;
    }
    if (!settles([&] { return parked->catalog->an_edit_is_held(); })) return nullptr;
    return parked;
}

TEST(ReadOrderTest, AGetConcurrentWithFlushNeverFallsBetweenTheTwoSnapshots) {
    TestStore store;
    auto catalog = std::make_shared<ControlledCatalog>(store.catalog());
    catalog->slow_edits();
    Options options = make_options(store, Compression::None, 8u << 20);
    options.manifest_catalog = catalog;
    options.background = BackgroundMode::Threaded;
    options.maintenance_interval = Duration(20);

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);
    ASSERT_EQ(db->put(Slice::from(std::string_view("key")),
                      Slice::from(std::string_view("value"))),
              Status::Ok);

    std::atomic<bool> stop{false};
    std::atomic<bool> missed{false};
    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                auto found = db->get(Slice::from(std::string_view("key")));
                if (!found || found->value().to_string() != "value") {
                    missed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }

    for (int round = 0; round < 200 && !missed.load(std::memory_order_relaxed); ++round) {
        ASSERT_EQ(db->put(Slice::from(std::string_view("key")),
                          Slice::from(std::string_view("value"))),
                  Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    stop.store(true, std::memory_order_relaxed);
    for (auto& reader : readers) reader.join();
    EXPECT_FALSE(missed.load(std::memory_order_relaxed));
}

TEST(ReadOrderTest, AQuietStoreRetriesARetryableFlushFailure) {
    TestStore store;
    auto catalog = std::make_shared<ControlledCatalog>(store.catalog());
    Options options = make_options(store, Compression::None, 8u << 20);
    options.manifest_catalog = catalog;
    options.background = BackgroundMode::Threaded;
    options.maintenance_interval = Duration(20);

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);
    ASSERT_EQ(db->put(Slice::from(std::string_view("key")),
                      Slice::from(std::string_view("value"))),
              Status::Ok);

    catalog->fail_next_edit();
    const Status flush_status = db->flush();
    EXPECT_TRUE(flush_status == Status::Io || flush_status == Status::Ok)
        << status_name(flush_status);
    EXPECT_TRUE(settles([&] { return db->stats().background_failures == 1; }))
        << "the injected flush failure was never observed";
    EXPECT_TRUE(settles([&] { return db->stats().flushes == 1; }))
        << "the frozen memtable was never retried after the store went quiet";

    auto found = db->get(Slice::from(std::string_view("key")));
    ASSERT_TRUE(found.has_value()) << status_name(found.error());
    EXPECT_EQ(found->value().to_string(), "value");
}

// `Stalled` means the write is not stored. A rotation is owed and the flush executor is busy, so
// the valve has to decide before the entry reaches the memtable — deciding after it has landed
// leaves the store holding a key its caller was told was refused.
TEST(StallTest, ARefusedWriteIsNotStored) {
    auto parked = park_a_flush(/*block_on_stall=*/false);
    ASSERT_NE(parked, nullptr);
    DB& db = *parked->db;

    // Admitted: the memtable it lands in is fresh, so nothing was owed when this arrived. It ends
    // the call over `memtable_bytes` and cannot be rotated, which is the next write's problem, not
    // a reason to lie about this one.
    EXPECT_EQ(db.put(Slice::from(std::string_view("b")), Slice::from(parked->value)), Status::Ok)
        << "nothing was owed when this write arrived";
    auto landed = db.get(Slice::from(std::string_view("b")));
    ASSERT_TRUE(landed.has_value()) << status_name(landed.error());
    EXPECT_EQ(landed->value().size(), parked->value.size());

    // Refused: a rotation is owed now and the flush executor still holds the one slot.
    EXPECT_EQ(db.put(Slice::from(std::string_view("c")), Slice::from(parked->value)),
              Status::Stalled)
        << "a rotation is owed and the flush executor is busy";
    EXPECT_EQ(db.get(Slice::from(std::string_view("c"))).error(), Status::NotFound)
        << "the write was refused, so the key must not be readable";
    EXPECT_GT(db.stats().stall_count, 0u);
}

// The control for the valve above: with blocking allowed the same write must wait for the slot and
// then land, not be refused. One valve, two configured answers.
TEST(StallTest, AWriteThatWouldStallBlocksAndLandsWhenBlockingIsAllowed) {
    auto parked = park_a_flush(/*block_on_stall=*/true);
    ASSERT_NE(parked, nullptr);
    DB& db = *parked->db;

    EXPECT_EQ(db.put(Slice::from(std::string_view("b")), Slice::from(parked->value)), Status::Ok);

    std::atomic<Status> result{Status::Unusable};
    std::thread writer([&] {
        result.store(db.put(Slice::from(std::string_view("c")), Slice::from(parked->value)),
                     std::memory_order_relaxed);
    });
    const bool entered_the_valve = settles([&] { return db.stats().stall_count > 0; });
    parked->catalog->release_edits();
    writer.join();

    EXPECT_TRUE(entered_the_valve) << "the write never entered the rotation valve";
    EXPECT_EQ(result.load(std::memory_order_relaxed), Status::Ok)
        << "blocking was allowed, so this write waits for the slot rather than being refused";

    for (const auto& key : {"a", "b", "c"}) {
        auto found = db.get(Slice::from(std::string_view(key)));
        EXPECT_TRUE(found.has_value()) << key << ": " << status_name(found.error());
    }
}

}  // namespace
}  // namespace elysiumkv::test
