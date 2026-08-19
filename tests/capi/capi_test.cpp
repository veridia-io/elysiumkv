#include "elysiumkv/elysiumkv.h"

#include "stats_decoder.hpp"
#include "support/temp_dir.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

/// Compiled as C99 in its own translation unit — see elysiumkv_c_smoke.c.
extern "C" int elysiumkv_c_smoke(const char* store_directory, const char* catalog_directory);

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "The ABI boundary" — the ABI every binding targets. These tests go through `elysiumkv.h` only:
/// no engine headers, no C++ types across the boundary.
class CApiTest : public ::testing::Test {
protected:
    void SetUp() override {
        // ARCHITECTURE.md "A process-wide memory budget" — a budget generous enough never to shed, attached to every case here.
        // The point is coverage of the *plumbing*: a budget that reaches the engine
        // through the ABI and is charged by it. Shedding is tested where it can be
        // forced.
        budget_ = elysiumkv_memory_budget_create(256u << 20);
        ASSERT_NE(budget_, nullptr);

        store_dir_ = (dir_.path() / "store").string();
        std::filesystem::create_directories(store_dir_);
        catalog_dir_ = dir_.path().string();

        store_ = elysiumkv_disk_blob_store_create(store_dir_.c_str(), "store-0");
        ASSERT_NE(store_, nullptr);
        catalog_ = elysiumkv_disk_manifest_catalog_create(catalog_dir_.c_str());
        ASSERT_NE(catalog_, nullptr);
    }

    void TearDown() override {
        if (budget_ != nullptr) elysiumkv_memory_budget_destroy(budget_);
        if (catalog_ != nullptr) elysiumkv_manifest_catalog_destroy(catalog_);
        if (store_ != nullptr) elysiumkv_blob_store_destroy(store_);
        if (second_store_ != nullptr) elysiumkv_blob_store_destroy(second_store_);
    }

    void* budget_ = nullptr;

    elysiumkv_options* make_options(bool transient_tier = false) {
        elysiumkv_options* options = elysiumkv_options_create();
        EXPECT_EQ(elysiumkv_options_configure(options, catalog_, budget_,
                                            /*memtable_bytes=*/64u << 10,
                                            /*block_bytes=*/1024, 0, 0, 0, 0, 0, 0, -1, -1, -1,
                                            /*flush_interval_ms=*/0,
                                            /*maintenance_interval_ms=*/0,
                                            /*obsolete_retention_ms=*/0,
                                            /*orphan_retention_ms=*/0,
                                            /*orphan_sweep_interval_ms=*/0),
                  ELYSIUMKV_OK);

        if (transient_tier) {
            const std::string cold = (dir_.path() / "cold").string();
            std::filesystem::create_directories(cold);
            second_store_ = elysiumkv_disk_blob_store_create(cold.c_str(), "store-1");
            EXPECT_EQ(elysiumkv_options_add_tier(options, store_, ELYSIUMKV_TRANSIENT, 60'000, 0, 120'000),
                      ELYSIUMKV_OK);
            EXPECT_EQ(elysiumkv_options_add_tier(options, second_store_, ELYSIUMKV_DURABLE, 0, 0, 0),
                      ELYSIUMKV_OK);
        } else {
            EXPECT_EQ(elysiumkv_options_add_tier(options, store_, ELYSIUMKV_DURABLE, 0, 0, 0),
                      ELYSIUMKV_OK);
        }
        EXPECT_EQ(elysiumkv_options_set_level(options, 0, ELYSIUMKV_COMPRESSION_NONE, 0, 4, 8, 12, 0),
                  ELYSIUMKV_OK);
        EXPECT_EQ(elysiumkv_options_set_level(options, 1, ELYSIUMKV_COMPRESSION_ZSTD, 4u << 20, 0, 0, 0,
                                            0),
                  ELYSIUMKV_OK);
        EXPECT_EQ(elysiumkv_options_set_level(options, 2, ELYSIUMKV_COMPRESSION_ZSTD, 0, 0, 0, 0, 0),
                  ELYSIUMKV_OK);
        return options;
    }

    elysiumkv_db* open() {
        elysiumkv_options* options = make_options();
        elysiumkv_db* db = nullptr;
        EXPECT_EQ(elysiumkv_open(options, &db), ELYSIUMKV_OK) << elysiumkv_last_error();
        elysiumkv_options_destroy(options);
        return db;
    }

    /// Every binding needs this shape: ask for the size, then decode.
    static DecodedStats snapshot(const elysiumkv_db* db) {
        size_t needed = 0;
        EXPECT_EQ(elysiumkv_stats_snapshot(db, nullptr, 0, &needed), ELYSIUMKV_OK);
        EXPECT_GT(needed, 0u);
        std::vector<uint8_t> buffer(needed);
        size_t written = 0;
        EXPECT_EQ(elysiumkv_stats_snapshot(db, buffer.data(), buffer.size(), &written), ELYSIUMKV_OK);
        EXPECT_EQ(written, needed);
        return decode_stats(buffer.data(), written);
    }

    static elysiumkv_status put(elysiumkv_db* db, const std::string& key, const std::string& value) {
        return elysiumkv_put(db, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                           reinterpret_cast<const uint8_t*>(value.data()), value.size());
    }

    TempDir dir_;
    std::string store_dir_;
    std::string catalog_dir_;
    void* store_ = nullptr;
    void* second_store_ = nullptr;
    void* catalog_ = nullptr;
};

/// The header is C99: this passes only because elysiumkv_c_smoke.c compiled as C.
TEST_F(CApiTest, TheHeaderIsUsableFromC) {
    EXPECT_EQ(elysiumkv_c_smoke(store_dir_.c_str(), catalog_dir_.c_str()), 0);
}

TEST_F(CApiTest, VersionIsReported) {
    ASSERT_NE(elysiumkv_version(), nullptr);
    const std::string version = elysiumkv_version();
    EXPECT_FALSE(version.empty()) << "a binding has to be able to check what it loaded";

    // The value is substituted into a generated header from `ELYSIUMKV_VERSION`, so the way this
    // breaks is not emptiness — it is a *literal* "@ELYSIUMKV_VERSION@" reaching the ABI when the
    // placeholder name or `@ONLY` is wrong. Asserting the shape catches that; asserting an exact
    // number would put the version back in a second place, which is what was removed.
    //
    // A pre-release suffix is allowed and deliberately not parsed: "1.0.1-rc1" is a legitimate
    // release tag, and the numeric prefix is the part with a required shape.
    const std::string numeric = version.substr(0, version.find('-'));
    std::vector<std::string> parts;
    for (size_t start = 0; start <= numeric.size();) {
        const size_t dot = numeric.find('.', start);
        const size_t end = dot == std::string::npos ? numeric.size() : dot;
        parts.push_back(numeric.substr(start, end - start));
        if (dot == std::string::npos) break;
        start = end + 1;
    }
    ASSERT_EQ(parts.size(), 3u) << "expected major.minor.patch, got \"" << version << "\"";
    for (const std::string& part : parts) {
        EXPECT_FALSE(part.empty()) << "empty component in \"" << version << "\"";
        EXPECT_EQ(part.find_first_not_of("0123456789"), std::string::npos)
            << "non-numeric component \"" << part << "\" in \"" << version << "\"";
    }
}

// ARCHITECTURE.md "The ABI boundary" — **pin accounting is a first-class invariant with a debug-build leak check
// at close.** A leaked pin holds a block-cache entry forever.
TEST_F(CApiTest, ClosingWithAPinOutstandingReportsTheLeak) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);

    const uint8_t* value = nullptr;
    size_t value_len = 0;
    uint64_t pin = 0;
    ASSERT_EQ(elysiumkv_get(db, reinterpret_cast<const uint8_t*>("k"), 1, &value, &value_len, &pin),
              ELYSIUMKV_OK);
    EXPECT_EQ(elysiumkv_pins_outstanding(db), 1u);

    // Deliberately not unpinned.
    EXPECT_EQ(elysiumkv_close(db), 1u) << "close must report what was left held";
    EXPECT_NE(std::string(elysiumkv_last_error()).find("outstanding"), std::string::npos)
        << elysiumkv_last_error();
}

// Closing detaches a live iterator rather than leaving it pointing at a freed
// engine: the handle belongs to the caller, who will destroy it on their own
// schedule — including, when a binding gets it wrong, after the close.
TEST_F(CApiTest, ClosingWithAnIteratorOutstandingReportsItAndDetachesIt) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);

    elysiumkv_iter* iter = nullptr;
    ASSERT_EQ(elysiumkv_iter_create(db, nullptr, 0, nullptr, 0, &iter), ELYSIUMKV_OK);
    ASSERT_TRUE(elysiumkv_iter_next(iter)) << "live before the close";

    EXPECT_EQ(elysiumkv_close(db), 1u) << "close reports what was left outstanding";

    // Detached: exhausted rather than a crash, and safe to destroy.
    EXPECT_FALSE(elysiumkv_iter_next(iter));
    EXPECT_EQ(elysiumkv_iter_status(iter), ELYSIUMKV_CONFIG);
    elysiumkv_iter_destroy(iter);
}

TEST_F(CApiTest, ACleanCloseReportsNothing) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);

    const uint8_t* value = nullptr;
    size_t value_len = 0;
    uint64_t pin = 0;
    ASSERT_EQ(elysiumkv_get(db, reinterpret_cast<const uint8_t*>("k"), 1, &value, &value_len, &pin),
              ELYSIUMKV_OK);
    elysiumkv_unpin(db, pin);

    elysiumkv_iter* iter = nullptr;
    ASSERT_EQ(elysiumkv_iter_create(db, nullptr, 0, nullptr, 0, &iter), ELYSIUMKV_OK);
    elysiumkv_iter_destroy(iter);

    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// **A clean close attempts a flush**, because there is no write-ahead log and a memtable dropped on
// the way out is lost for no reason. The C ABI inherits this from the engine's destructor.
TEST_F(CApiTest, CloseAttemptsAFlush) {
    elysiumkv_db* db = open();
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);
    EXPECT_EQ(elysiumkv_close(db), 0u);

    db = open();
    const uint8_t* value = nullptr;
    size_t len = 0;
    uint64_t pin = 0;
    EXPECT_EQ(elysiumkv_get(db, reinterpret_cast<const uint8_t*>("k"), 1, &value, &len, &pin),
              ELYSIUMKV_OK)
        << "a clean close discarded the memtable";
    elysiumkv_unpin(db, pin);
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// The control, and the reason the case above is not vacuous: told not to flush, the same sequence
// loses the write exactly as a crash would.
TEST_F(CApiTest, CloseWithoutFlushDiscardsTheMemtable) {
    elysiumkv_db* db = open();
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);
    EXPECT_EQ(elysiumkv_close_without_flush(db), 0u);

    db = open();
    const uint8_t* value = nullptr;
    size_t len = 0;
    uint64_t pin = 0;
    EXPECT_EQ(elysiumkv_get(db, reinterpret_cast<const uint8_t*>("k"), 1, &value, &len, &pin),
              ELYSIUMKV_NOT_FOUND);
    EXPECT_EQ(elysiumkv_close(db), 0u);
}


// The pinned bytes must stay readable while the pin is held, whatever the cache
// does in the meantime — that is the whole point of the protocol.
TEST_F(CApiTest, PinnedBytesSurviveCachePressure) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);

    for (int i = 0; i < 2000; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), std::string(200, 'v')), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    const std::string key = "key:7";
    const uint8_t* value = nullptr;
    size_t value_len = 0;
    uint64_t pin = 0;
    ASSERT_EQ(elysiumkv_get(db, reinterpret_cast<const uint8_t*>(key.data()), key.size(), &value,
                          &value_len, &pin),
              ELYSIUMKV_OK);
    const std::string held(reinterpret_cast<const char*>(value), value_len);

    // Read everything else, evicting whatever the cache likes.
    for (int i = 0; i < 2000; ++i) {
        const std::string other = "key:" + std::to_string(i);
        uint8_t scratch[512];
        size_t len = 0;
        (void)elysiumkv_get_copy(db, reinterpret_cast<const uint8_t*>(other.data()), other.size(),
                               scratch, sizeof(scratch), &len);
    }

    EXPECT_EQ(std::string(reinterpret_cast<const char*>(value), value_len), held)
        << "a pinned value must not move or change under the caller";
    elysiumkv_unpin(db, pin);
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

TEST_F(CApiTest, GetCopyReportsTheFullLengthEvenWhenTruncated) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    const std::string value(500, 'x');
    ASSERT_EQ(put(db, "k", value), ELYSIUMKV_OK);

    uint8_t small[16];
    size_t len = 0;
    ASSERT_EQ(elysiumkv_get_copy(db, reinterpret_cast<const uint8_t*>("k"), 1, small, sizeof(small),
                               &len),
              ELYSIUMKV_OK);
    EXPECT_EQ(len, 500u) << "the caller has to be able to size a second buffer";
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(small), sizeof(small)),
              std::string(16, 'x'));

    // A null buffer is a length query.
    len = 0;
    ASSERT_EQ(elysiumkv_get_copy(db, reinterpret_cast<const uint8_t*>("k"), 1, nullptr, 0, &len),
              ELYSIUMKV_OK);
    EXPECT_EQ(len, 500u);
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// ARCHITECTURE.md "The ABI boundary" — `ELYSIUMKV_IO` is the retryable class, and the ABI must never invite a
// binding to read a failure as absence.
TEST_F(CApiTest, StatusCodesAreDistinctAndCarryDetail) {
    elysiumkv_options* options = elysiumkv_options_create();
    ASSERT_EQ(elysiumkv_options_configure(options, catalog_, nullptr, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, 0, 0, 0, 0, 0), ELYSIUMKV_OK);
    // A transient last tier: rejected at open (ARCHITECTURE.md "A tier is not a level").
    ASSERT_EQ(elysiumkv_options_add_tier(options, store_, ELYSIUMKV_TRANSIENT, 60'000, 0, 120'000),
              ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_set_level(options, 0, ELYSIUMKV_COMPRESSION_NONE, 0, 4, 0, 0, 0),
              ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_set_level(options, 1, ELYSIUMKV_COMPRESSION_NONE, 0, 0, 0, 0, 0),
              ELYSIUMKV_OK);

    elysiumkv_db* db = nullptr;
    EXPECT_EQ(elysiumkv_open_with_result(options, &db, nullptr, nullptr, nullptr, nullptr),
              ELYSIUMKV_CONFIG);
    EXPECT_EQ(db, nullptr);
    EXPECT_FALSE(std::string(elysiumkv_last_error()).empty());
    elysiumkv_options_destroy(options);
}

// ARCHITECTURE.md "A tier is not a level" — the guarded open refuses a transient configuration outright, and the
// reporting form accepts it.
TEST_F(CApiTest, GuardedOpenRefusesATransientTier) {
    elysiumkv_options* options = make_options(/*transient_tier=*/true);
    elysiumkv_db* db = nullptr;
    EXPECT_EQ(elysiumkv_open(options, &db), ELYSIUMKV_CONFIG);
    EXPECT_EQ(db, nullptr);

    const char* discarded[4] = {nullptr, nullptr, nullptr, nullptr};
    size_t n_stores = 4;
    uint64_t discarded_files = 1;
    bool requires_recovery = true;
    ASSERT_EQ(elysiumkv_open_with_result(options, &db, discarded, &n_stores, &discarded_files,
                                       &requires_recovery),
              ELYSIUMKV_OK)
        << elysiumkv_last_error();
    EXPECT_EQ(n_stores, 0u);
    EXPECT_EQ(discarded_files, 0u);
    EXPECT_FALSE(requires_recovery);
    EXPECT_EQ(snapshot(db).tiers.size(), 2u);
    EXPECT_EQ(elysiumkv_close(db), 0u);
    elysiumkv_options_destroy(options);
}

TEST_F(CApiTest, StatsReachBothAxes) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 500; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), std::string(100, 'v')), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    const DecodedStats stats = snapshot(db);
    EXPECT_EQ(stats.format_version, 1u);
    EXPECT_EQ(stats.levels.size(), 3u);
    EXPECT_EQ(stats.tiers.size(), 1u);
    EXPECT_GT(stats.tiers[0].file_count, 0);
    EXPECT_GT(stats.tiers[0].bytes, 0u);
    EXPECT_EQ(stats.tiers[0].files_pending_migration, 0);
    EXPECT_FALSE(stats.tiers[0].stalling);
    EXPECT_EQ(stats.level_bytes_total(), stats.tier_bytes_total())
        << "the two axes describe the same files";
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// ARCHITECTURE.md "The ABI boundary" — the reason the snapshot is one call. The old shape had an accessor per
// field, each taking its own `stats()`, so a caller assembling a picture of the
// engine sampled a different instant per field.
//
// Every file sits in exactly one level and exactly one tier, so these two totals
// are the same number seen along two axes. That identity is what the old design
// could not offer: measured against this same workload, two snapshots taken 20ms
// apart disagreed in 40 rounds out of 40. Back-to-back calls usually agree,
// which is what makes the torn design so comfortable to ship — the window is
// small, not absent, and a binding assembling fourteen calls is preemptible in
// every gap. The assertion below is on the invariant rather than on the race,
// because a test that depends on losing a race belongs nowhere near a gate.
TEST_F(CApiTest, TheSnapshotIsOneInstantWhileCompactionRuns) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);

    // Enough writing to keep the background thread compacting throughout.
    int consistent = 0;
    for (int round = 0; round < 40; ++round) {
        for (int i = 0; i < 400; ++i) {
            ASSERT_EQ(put(db, "key:" + std::to_string(round * 400 + i), std::string(120, 'v')),
                      ELYSIUMKV_OK);
        }
        const DecodedStats stats = snapshot(db);
        ASSERT_EQ(stats.level_bytes_total(), stats.tier_bytes_total())
            << "torn snapshot at round " << round;
        ++consistent;
    }

    EXPECT_EQ(consistent, 40);
    const DecodedStats final_stats = snapshot(db);
    EXPECT_GT(final_stats.level_bytes_total(), 0u) << "the check must not be vacuous";
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

/// Pins the stats buffer layout FORMAT.md declares, against a real snapshot. The self-describing
/// header is only useful if a decoder can trust where the records begin, so the sizes the header
/// reports are asserted against the buffer's actual length rather than against the constants.
// The flush counter, which is what makes `flush_interval` diagnosable: too short an interval
// produces many small L0 files and therefore more compaction, and flush rate is the first place
// that shows up. It is also the only way to confirm the interval fires at all on a quiet
// partition — `memtable_age` is a gauge read at scrape time, so a flush between two scrapes leaves
// no trace in it, and a counter cannot be derived from a gauge.
TEST_F(CApiTest, TheFlushCounterAdvancesAcrossAFlush) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    EXPECT_EQ(snapshot(db).flushes, 0u) << "nothing has been flushed yet";

    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), std::string(64, 'v')), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    const uint64_t after_one = snapshot(db).flushes;
    EXPECT_GE(after_one, 1u);

    for (int i = 200; i < 400; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), std::string(64, 'v')), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    EXPECT_GT(snapshot(db).flushes, after_one) << "a counter, not a gauge";

    // An empty memtable is not flushed, so the counter must not move for a no-op.
    const uint64_t settled = snapshot(db).flushes;
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    EXPECT_EQ(snapshot(db).flushes, settled)
        << "counting a rotation that produced no file would make the rate unreadable";
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// The watermark travels through the same buffer, and absence has to survive the trip: zero is a
// valid position, so an exporter needs the presence byte to know whether to publish the series.
TEST_F(CApiTest, TheStatsBufferCarriesTheLiveWatermarkAndItsPresence) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    EXPECT_FALSE(snapshot(db).watermark_present) << "no watermark has been set";

    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_set_watermark(db, 0), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    DecodedStats stats = snapshot(db);
    EXPECT_TRUE(stats.watermark_present) << "zero is a position, not the absence of one";
    EXPECT_EQ(stats.durable_watermark, 0u);

    ASSERT_EQ(put(db, "k2", "v"), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_set_watermark(db, 4242), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    stats = snapshot(db);
    EXPECT_TRUE(stats.watermark_present);
    EXPECT_EQ(stats.durable_watermark, 4242u);

    // Non-decreasing, and refused rather than clamped.
    EXPECT_EQ(elysiumkv_set_watermark(db, 4241), ELYSIUMKV_CONFIG);

    // The getter is a different quantity: it describes the state recovered at open and must not
    // have moved.
    uint64_t recovered = 12345;
    bool present = true;
    ASSERT_EQ(elysiumkv_watermark(db, &recovered, &present), ELYSIUMKV_OK);
    EXPECT_FALSE(present) << "this store was opened before any watermark existed";
    EXPECT_EQ(recovered, 12345u) << "out is untouched when absent, so a plausible zero cannot leak";
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

/* The C ABI cannot express the C++ type split, so the refusal is a status — and it has to be
 * checked, because a binding that got a read-only handle where it expected a writable one would
 * otherwise silently drop writes.
 */
TEST_F(CApiTest, AReadOnlyHandleRefusesEveryWriteAndStillReads) {
    // Populate, then close, so the reader opens a store that exists.
    {
        elysiumkv_db* writer = open();
        ASSERT_NE(writer, nullptr);
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(put(writer, "key:" + std::to_string(i), "v"), ELYSIUMKV_OK);
        }
        ASSERT_EQ(elysiumkv_flush(writer), ELYSIUMKV_OK);
        ASSERT_EQ(elysiumkv_close(writer), 0u);
    }

    elysiumkv_options* options = make_options();
    elysiumkv_db* reader = nullptr;
    ASSERT_EQ(elysiumkv_open_read_only(options, &reader), ELYSIUMKV_OK) << elysiumkv_last_error();
    elysiumkv_options_destroy(options);
    ASSERT_NE(reader, nullptr);

    // Reads work.
    uint8_t buffer[64];
    size_t len = 0;
    const std::string key = "key:7";
    ASSERT_EQ(elysiumkv_get_copy(reader, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                               buffer, sizeof(buffer), &len),
              ELYSIUMKV_OK);
    EXPECT_EQ(len, 1u);

    // Every write refuses, and says why rather than silently doing nothing.
    EXPECT_EQ(put(reader, "k", "v"), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_delete(reader, reinterpret_cast<const uint8_t*>(key.data()), key.size()),
              ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_flush(reader), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_compact_level(reader, 0), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_set_watermark(reader, 1), ELYSIUMKV_CONFIG);

    // And refresh is available on both kinds of handle.
    EXPECT_EQ(elysiumkv_refresh(reader), ELYSIUMKV_OK);
    EXPECT_EQ(elysiumkv_close(reader), 0u);
}

/* Encryption reaching a binding: a key manager written in C, and the engine's own construction
 * keyed by it.
 *
 * **The assertion is on the stored bytes, not on the round trip.** A round trip passes against a
 * build that registered the provider and then encrypted nothing, which is exactly the failure a C
 * seam invites — the callbacks are reached, so it looks wired.
 */
namespace {

/// The envelope is the key. Enough to prove the seam; a KMS would add a network and prove nothing
/// further about the boundary.
extern "C" elysiumkv_status c_new_data_key(void* context, uint8_t* key_out, size_t key_cap,
                                       uint8_t* envelope_out, size_t envelope_cap,
                                       size_t* envelope_len) {
    auto* counter = static_cast<int*>(context);
    if (key_cap < 32 || envelope_cap < 32) return ELYSIUMKV_CONFIG;
    for (size_t i = 0; i < 32; ++i) {
        key_out[i] = static_cast<uint8_t>(*counter * 17 + static_cast<int>(i));
        envelope_out[i] = key_out[i];
    }
    *envelope_len = 32;
    ++*counter;
    return ELYSIUMKV_OK;
}

extern "C" elysiumkv_status c_open_data_key(void*, const uint8_t* envelope, size_t envelope_len,
                                        uint8_t* key_out, size_t key_cap) {
    if (envelope_len != 32 || key_cap < 32) return ELYSIUMKV_CORRUPT;
    for (size_t i = 0; i < 32; ++i) key_out[i] = envelope[i];
    return ELYSIUMKV_OK;
}

}  // namespace

TEST_F(CApiTest, AKeyManagerSuppliedThroughTheAbiEncryptsTheStore) {
    int keys_issued = 0;
    elysiumkv_encryption_key_manager manager{};
    manager.context = &keys_issued;
    manager.new_data_key = c_new_data_key;
    manager.open_data_key = c_open_data_key;

    elysiumkv_options* options = make_options();
    ASSERT_EQ(elysiumkv_options_add_aes256_gcm_encryption(options, "c-kms", &manager, 0), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_set_primary_encryption_provider(options, "c-kms"), ELYSIUMKV_OK);

    elysiumkv_db* db = nullptr;
    ASSERT_EQ(elysiumkv_open(options, &db), ELYSIUMKV_OK);
    elysiumkv_options_destroy(options);
    ASSERT_NE(db, nullptr);

    const std::string canary = "CANARY-THROUGH-THE-C-ABI-0123456789";
    for (int i = 0; i < 64; ++i) {
        const std::string key = "k" + std::to_string(i);
        ASSERT_EQ(put(db, key.c_str(), canary.c_str()), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    EXPECT_GT(keys_issued, 0) << "the binding's manager was actually asked for a key";

    // Read back through the ABI.
    const uint8_t* value = nullptr;
    size_t len = 0;
    uint64_t pin = 0;
    ASSERT_EQ(elysiumkv_get(db, reinterpret_cast<const uint8_t*>("k7"), 2, &value, &len, &pin),
              ELYSIUMKV_OK);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(value), len), canary);
    elysiumkv_unpin(db, pin);
    elysiumkv_close(db);

    // And the bytes on disk are not the canary.
    for (const auto& entry : std::filesystem::directory_iterator(store_dir_)) {
        if (!entry.is_regular_file()) continue;
        std::ifstream in(entry.path(), std::ios::binary);
        const std::string contents((std::istreambuf_iterator<char>(in)),
                                   std::istreambuf_iterator<char>());
        EXPECT_EQ(contents.find(canary), std::string::npos) << entry.path();
    }
}

TEST_F(CApiTest, TheBuiltInStaticKeyManagerEncryptsTheStore) {
    std::array<uint8_t, 32> master{};
    for (size_t i = 0; i < master.size(); ++i) master[i] = static_cast<uint8_t>(i);

    elysiumkv_options* options = make_options();
    ASSERT_EQ(elysiumkv_options_add_aes256_gcm_encryption_with_static_key(
                  options, "static", master.data(), master.size(), 0),
              ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_set_primary_encryption_provider(options, "static"), ELYSIUMKV_OK);

    elysiumkv_db* db = nullptr;
    ASSERT_EQ(elysiumkv_open(options, &db), ELYSIUMKV_OK);
    elysiumkv_options_destroy(options);

    const std::string canary = "CANARY-UNDER-A-STATIC-KEY-01234567";
    for (int i = 0; i < 64; ++i) {
        const std::string key = "k" + std::to_string(i);
        ASSERT_EQ(put(db, key.c_str(), canary.c_str()), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    elysiumkv_close(db);

    for (const auto& entry : std::filesystem::directory_iterator(store_dir_)) {
        if (!entry.is_regular_file()) continue;
        std::ifstream in(entry.path(), std::ios::binary);
        const std::string contents((std::istreambuf_iterator<char>(in)),
                                   std::istreambuf_iterator<char>());
        EXPECT_EQ(contents.find(canary), std::string::npos) << entry.path();
    }
}

TEST_F(CApiTest, TheBuiltInManagersRefuseUnusableConfiguration) {
    std::array<uint8_t, 32> master{};
    elysiumkv_options* options = elysiumkv_options_create();

    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption_with_static_key(
                  options, "", master.data(), master.size(), 0),
              ELYSIUMKV_OK)
        << "the empty id belongs to the passthrough";
    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption_with_static_key(options, "short",
                                                                         master.data(), 31, 0),
              ELYSIUMKV_OK)
        << "a 31-byte master key is not an AES-256 key";
    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption_with_static_key(options, "null", nullptr,
                                                                         32, 0),
              ELYSIUMKV_OK);

    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption_with_kms(options, "kms", nullptr, nullptr,
                                                                  nullptr, nullptr, nullptr, 0, 0),
              ELYSIUMKV_OK)
        << "a KMS manager without a key id is not a configuration";

    // Without the AWS build this is refused for a different reason, and the message has to name the
    // build option either way — the same contract the remote constructors keep.
    if ((elysiumkv_features() & ELYSIUMKV_FEATURE_AWS) == 0) {
        EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption_with_kms(
                      options, "kms", "alias/whatever", nullptr, nullptr, nullptr, nullptr, 0, 0),
                  ELYSIUMKV_OK);
        EXPECT_NE(std::string(elysiumkv_last_error()).find("ELYSIUMKV_BUILD_AWS"), std::string::npos)
            << elysiumkv_last_error();
    }
    elysiumkv_options_destroy(options);
}

TEST_F(CApiTest, TheReservedProviderIdIsRefusedThroughTheAbi) {
    elysiumkv_encryption_key_manager manager{};
    int counter = 0;
    manager.context = &counter;
    manager.new_data_key = c_new_data_key;
    manager.open_data_key = c_open_data_key;

    elysiumkv_options* options = elysiumkv_options_create();
    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption(options, "", &manager, 0), ELYSIUMKV_OK);
    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption(options, nullptr, &manager, 0), ELYSIUMKV_OK);

    // An incomplete vtable is refused where it is registered, not where it is first used.
    elysiumkv_encryption_key_manager partial{};
    partial.new_data_key = c_new_data_key;
    EXPECT_NE(elysiumkv_options_add_aes256_gcm_encryption(options, "partial", &partial, 0),
              ELYSIUMKV_OK);

    elysiumkv_encryption_provider empty{};
    EXPECT_NE(elysiumkv_options_add_encryption_provider(options, "custom", &empty), ELYSIUMKV_OK);
    elysiumkv_options_destroy(options);
}

TEST_F(CApiTest, StatsBufferMatchesTheDocumentedLayout) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);

    size_t needed = 0;
    ASSERT_EQ(elysiumkv_stats_snapshot(db, nullptr, 0, &needed), ELYSIUMKV_OK);
    std::vector<uint8_t> buffer(needed, 0);
    size_t written = 0;
    ASSERT_EQ(elysiumkv_stats_snapshot(db, buffer.data(), buffer.size(), &written), ELYSIUMKV_OK);
    ASSERT_EQ(written, needed);

    auto u32 = [&buffer](size_t offset) {
        return static_cast<uint32_t>(buffer[offset]) |
               (static_cast<uint32_t>(buffer[offset + 1]) << 8) |
               (static_cast<uint32_t>(buffer[offset + 2]) << 16) |
               (static_cast<uint32_t>(buffer[offset + 3]) << 24);
    };

    EXPECT_EQ(u32(0), 1u) << "format_version";
    const uint32_t header_bytes = u32(4);
    const uint32_t level_record_bytes = u32(8);
    const uint32_t tier_record_bytes = u32(12);
    const uint32_t level_count = u32(16);
    const uint32_t tier_count = u32(20);

    EXPECT_EQ(header_bytes, 240u);
    EXPECT_EQ(level_record_bytes, 48u);
    // 32 original fields plus the store's seven I/O counters. The header carries the width, so a
    // decoder written against 32 steps correctly over the wider record — which is the whole reason
    // the width is in the header.
    EXPECT_EQ(tier_record_bytes, 88u);
    EXPECT_LE(buffer[24], 1u) << "requires_recovery is a 0/1 byte";
    for (size_t i = 25; i < 32; ++i) EXPECT_EQ(buffer[i], 0u) << "header padding at " << i;

    // The two appended fields, at the offsets FORMAT.md §7 fixes so that whichever feature
    // landed second would not move the first.
    EXPECT_LE(buffer[208], 1u) << "watermark_present is a 0/1 byte";
    EXPECT_EQ(buffer[208], 0u) << "no watermark has been set on this store";
    for (size_t i = 209; i < 216; ++i) EXPECT_EQ(buffer[i], 0u) << "watermark padding at " << i;

    // **The whole point of the self-describing header**: records are located by the declared
    // sizes, so those sizes must account for the buffer exactly.
    EXPECT_EQ(needed, header_bytes + level_count * level_record_bytes +
                          tier_count * tier_record_bytes)
        << "a decoder locating records by the declared sizes would run off the end";

    // Level record padding, at the documented offsets within the first record.
    ASSERT_GT(level_count, 0u);
    const size_t level0 = header_bytes;
    EXPECT_LE(buffer[level0 + 28], 1u) << "age_triggered";
    EXPECT_LE(buffer[level0 + 29], 1u) << "stalling";
    EXPECT_EQ(buffer[level0 + 30], 0u);
    EXPECT_EQ(buffer[level0 + 31], 0u);

    // Tier record padding likewise.
    ASSERT_GT(tier_count, 0u);
    const size_t tier0 = header_bytes + level_count * level_record_bytes;
    EXPECT_LE(buffer[tier0 + 28], 1u) << "stalling";
    for (size_t i = 29; i < 32; ++i) EXPECT_EQ(buffer[tier0 + i], 0u);

    // The I/O counters, appended after that padding. A put has happened, so the store has
    // been written to — a zero here would mean the fields were reserved and never filled.
    auto u64 = [&buffer](size_t offset) {
        uint64_t value = 0;
        for (size_t i = 0; i < 8; ++i) {
            value |= static_cast<uint64_t>(buffer[offset + i]) << (8 * i);
        }
        return value;
    };
    EXPECT_GT(u64(tier0 + 32) + u64(tier0 + 40) + u64(tier0 + 48) + u64(tier0 + 56), 0u)
        << "gets/puts/removes/lists";
    EXPECT_EQ(u64(tier0 + 80), 0u) << "errors, on a store nothing has gone wrong with";

    elysiumkv_close(db);
}

TEST_F(CApiTest, StatsSnapshotReportsItsSizeAndRefusesToTruncate) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    ASSERT_EQ(put(db, "k", "v"), ELYSIUMKV_OK);

    size_t needed = 0;
    ASSERT_EQ(elysiumkv_stats_snapshot(db, nullptr, 0, &needed), ELYSIUMKV_OK);
    ASSERT_GT(needed, 136u) << "header plus three levels and a tier";

    // A buffer one byte short writes nothing rather than half a snapshot.
    std::vector<uint8_t> undersized(needed - 1, 0xAB);
    size_t reported = 0;
    EXPECT_EQ(elysiumkv_stats_snapshot(db, undersized.data(), undersized.size(), &reported),
              ELYSIUMKV_OK);
    EXPECT_EQ(reported, needed);
    EXPECT_EQ(undersized[0], 0xAB) << "nothing was written";

    // A larger buffer is fine, and reports the same length.
    std::vector<uint8_t> oversized(needed + 64, 0);
    EXPECT_EQ(elysiumkv_stats_snapshot(db, oversized.data(), oversized.size(), &reported),
              ELYSIUMKV_OK);
    EXPECT_EQ(reported, needed);
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// The rule that keeps the format extensible: locate records by the declared
// sizes. A decoder that hardcodes them breaks the first time a field is added.
TEST_F(CApiTest, TheDecoderFollowsTheDeclaredRecordSizes) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), "v"), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    size_t needed = 0;
    ASSERT_EQ(elysiumkv_stats_snapshot(db, nullptr, 0, &needed), ELYSIUMKV_OK);
    std::vector<uint8_t> buffer(needed);
    size_t written = 0;
    ASSERT_EQ(elysiumkv_stats_snapshot(db, buffer.data(), buffer.size(), &written), ELYSIUMKV_OK);
    const DecodedStats actual = decode_stats(buffer.data(), written);

    // Rewrite it as a future version would: same fields, wider records and a
    // longer header. The decoder must read exactly the same values back.
    // Read from the buffer, not written down here. A test about following declared
    // sizes must not hardcode them: this said 136 and broke the moment the header
    // legitimately grew, which is the failure it exists to rule out.
    const auto read_u32_at = [&](size_t offset) {
        uint32_t value = 0;
        for (int i = 0; i < 4; ++i) {
            value |= static_cast<uint32_t>(buffer[offset + static_cast<size_t>(i)]) << (8 * i);
        }
        return value;
    };
    const uint32_t header = read_u32_at(4), level_bytes = read_u32_at(8),
                   tier_bytes = read_u32_at(12);
    const uint32_t grown_header = header + 16, grown_level = level_bytes + 8,
                   grown_tier = tier_bytes + 8;
    std::vector<uint8_t> grown(grown_header + actual.levels.size() * grown_level +
                               actual.tiers.size() * grown_tier);
    std::memcpy(grown.data(), buffer.data(), header);
    const auto put_u32 = [&](size_t offset, uint32_t value) {
        for (int i = 0; i < 4; ++i) grown[offset + static_cast<size_t>(i)] =
            static_cast<uint8_t>(value >> (8 * i));
    };
    put_u32(4, grown_header);
    put_u32(8, grown_level);
    put_u32(12, grown_tier);
    for (size_t i = 0; i < actual.levels.size(); ++i) {
        std::memcpy(grown.data() + grown_header + i * grown_level,
                    buffer.data() + header + i * level_bytes, level_bytes);
    }
    for (size_t i = 0; i < actual.tiers.size(); ++i) {
        std::memcpy(grown.data() + grown_header + actual.levels.size() * grown_level +
                        i * grown_tier,
                    buffer.data() + header + actual.levels.size() * level_bytes + i * tier_bytes,
                    tier_bytes);
    }

    const DecodedStats from_grown = decode_stats(grown.data(), grown.size());
    ASSERT_EQ(from_grown.levels.size(), actual.levels.size());
    ASSERT_EQ(from_grown.tiers.size(), actual.tiers.size());
    for (size_t i = 0; i < actual.levels.size(); ++i) {
        EXPECT_EQ(from_grown.levels[i].file_count, actual.levels[i].file_count) << i;
        EXPECT_EQ(from_grown.levels[i].bytes, actual.levels[i].bytes) << i;
    }
    EXPECT_EQ(from_grown.tier_bytes_total(), actual.tier_bytes_total());
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// Each bound is independent. Folding them — "unbounded only when both are null"
// — turned `[lo, end)` into `[lo, "")`, the empty range, so the scan returned
// nothing at all. That is the worst shape a bug can take here: it does not look
// like a failure, it looks like a store with no data in it.
TEST_F(CApiTest, EachIteratorBoundIsIndependentlyOptional) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 20; ++i) {
        char key[16];
        std::snprintf(key, sizeof(key), "k%02d", i);
        ASSERT_EQ(put(db, key, "v"), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    const auto count = [&](const char* lo, const char* hi) {
        elysiumkv_iter* iter = nullptr;
        EXPECT_EQ(elysiumkv_iter_create(db, reinterpret_cast<const uint8_t*>(lo),
                                      lo == nullptr ? 0 : std::strlen(lo),
                                      reinterpret_cast<const uint8_t*>(hi),
                                      hi == nullptr ? 0 : std::strlen(hi), &iter),
                  ELYSIUMKV_OK);
        int seen = 0;
        while (elysiumkv_iter_next(iter)) ++seen;
        EXPECT_EQ(elysiumkv_iter_status(iter), ELYSIUMKV_OK);
        elysiumkv_iter_destroy(iter);
        return seen;
    };

    EXPECT_EQ(count(nullptr, nullptr), 20) << "both unbounded";
    EXPECT_EQ(count("k05", "k10"), 5) << "half-open on both ends";
    EXPECT_EQ(count("k05", nullptr), 15) << "from here to the end of the keyspace";
    EXPECT_EQ(count(nullptr, "k05"), 5) << "from the start";
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

/// The two tuning knobs that are a workload judgement rather than a capacity one, across the ABI.
///
/// A separate entry point from `elysiumkv_options_configure`, so this also pins that both exist and
/// that the split did not leave the new one unreachable — the "C++ can reach it, the binding cannot"
/// asymmetry this repository has had to remove three times.
TEST_F(CApiTest, CompactionTuningCrossesTheAbi) {
    elysiumkv_options* options = elysiumkv_options_create();
    ASSERT_NE(options, nullptr);

    EXPECT_EQ(elysiumkv_options_configure_compaction(options, 0.5, 2048), ELYSIUMKV_OK);
    // Zero is a real value — it turns the trigger off — and must not be read as "leave unset".
    EXPECT_EQ(elysiumkv_options_configure_compaction(options, 0.0, 0), ELYSIUMKV_OK);

    // A fraction outside [0,1] can never be reached, so it would read as off while looking
    // configured. Refused rather than clamped.
    EXPECT_NE(elysiumkv_options_configure_compaction(options, 1.5, 0), ELYSIUMKV_OK);
    EXPECT_NE(elysiumkv_options_configure_compaction(options, -0.1, 0), ELYSIUMKV_OK);
    EXPECT_NE(elysiumkv_options_configure_compaction(nullptr, 0.5, 0), ELYSIUMKV_OK);

    elysiumkv_options_destroy(options);
}

/// Same shape for the two jitters. Both default to zero, so a binding that never calls this is
/// unchanged — which is the point of it being its own entry rather than two more positions on
/// `elysiumkv_options_configure`.
TEST_F(CApiTest, JitterTuningCrossesTheAbi) {
    elysiumkv_options* options = elysiumkv_options_create();
    ASSERT_NE(options, nullptr);

    EXPECT_EQ(elysiumkv_options_configure_jitter(options, 0.25, 0.5), ELYSIUMKV_OK);
    EXPECT_EQ(elysiumkv_options_configure_jitter(options, 0.0, 0.0), ELYSIUMKV_OK);
    EXPECT_EQ(elysiumkv_options_configure_jitter(options, 1.0, 1.0), ELYSIUMKV_OK);

    EXPECT_NE(elysiumkv_options_configure_jitter(options, 1.5, 0.0), ELYSIUMKV_OK);
    EXPECT_NE(elysiumkv_options_configure_jitter(options, 0.0, -0.1), ELYSIUMKV_OK);
    // NaN passes every comparison it is asked, so it has to be refused by shape.
    EXPECT_NE(elysiumkv_options_configure_jitter(options, std::nan(""), 0.0), ELYSIUMKV_OK);
    EXPECT_NE(elysiumkv_options_configure_jitter(nullptr, 0.25, 0.0), ELYSIUMKV_OK);

    elysiumkv_options_destroy(options);
}

/// The chunked cache constructors, and that the plain ones still behave as they did.
TEST_F(CApiTest, ChunkedCacheConstructorsCrossTheAbi) {
    void* base = elysiumkv_disk_blob_store_create(store_dir_.c_str(), "cache-delegate");
    ASSERT_NE(base, nullptr);

    void* chunked = nullptr;
    EXPECT_EQ(elysiumkv_memory_cache_blob_store_create_chunked(base, nullptr, 1u << 20,
                                                           /*cache_on_write=*/0,
                                                           /*fetch_granularity=*/64u << 10,
                                                           &chunked),
              ELYSIUMKV_OK);
    ASSERT_NE(chunked, nullptr);
    elysiumkv_blob_store_destroy(chunked);

    void* plain = nullptr;
    EXPECT_EQ(elysiumkv_memory_cache_blob_store_create(base, nullptr, 1u << 20, 0, &plain),
              ELYSIUMKV_OK);
    ASSERT_NE(plain, nullptr);
    elysiumkv_blob_store_destroy(plain);

    elysiumkv_blob_store_destroy(base);
}

/// Truncation across the ABI, including that the floor then refuses a write below it — the part a
/// caller is most likely to meet by accident.
TEST_F(CApiTest, TruncateBelowCrossesTheAbi) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 20; ++i) {
        char key[16];
        std::snprintf(key, sizeof(key), "k%02d", i);
        ASSERT_EQ(put(db, key, "v"), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    ASSERT_EQ(elysiumkv_truncate_below(db, reinterpret_cast<const uint8_t*>("k10"), 3),
              ELYSIUMKV_OK);

    elysiumkv_iter* iter = nullptr;
    ASSERT_EQ(elysiumkv_iter_create(db, nullptr, 0, nullptr, 0, &iter), ELYSIUMKV_OK);
    int seen = 0;
    while (elysiumkv_iter_next(iter)) ++seen;
    elysiumkv_iter_destroy(iter);
    EXPECT_EQ(seen, 10);

    // Idempotent, and the floor refuses what falls under it.
    EXPECT_EQ(elysiumkv_truncate_below(db, reinterpret_cast<const uint8_t*>("k05"), 3),
              ELYSIUMKV_OK);
    EXPECT_NE(put(db, "k03", "v"), ELYSIUMKV_OK);

    EXPECT_EQ(elysiumkv_close(db), 0u);
}

/// The reverse entry points, across the ABI. Separate symbols rather than a flag on the forward
/// call, so this also pins that both exist and are reachable — a binding verifies the ABI by its
/// export set, and a missing symbol here is a link failure in whichever consumer reaches it first.
TEST_F(CApiTest, ReverseIterationCrossesTheAbi) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 20; ++i) {
        char key[16];
        std::snprintf(key, sizeof(key), "k%02d", i);
        ASSERT_EQ(put(db, key, "v"), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    const auto collect = [&](elysiumkv_iter* iter) {
        std::vector<std::string> keys;
        while (elysiumkv_iter_next(iter)) {
            const uint8_t* key = nullptr;
            size_t key_len = 0;
            elysiumkv_iter_key(iter, &key, &key_len);
            keys.emplace_back(reinterpret_cast<const char*>(key), key_len);
        }
        EXPECT_EQ(elysiumkv_iter_status(iter), ELYSIUMKV_OK);
        elysiumkv_iter_destroy(iter);
        return keys;
    };

    elysiumkv_iter* iter = nullptr;
    ASSERT_EQ(elysiumkv_iter_create_reverse(db, nullptr, 0, nullptr, 0, &iter), ELYSIUMKV_OK);
    const std::vector<std::string> descending = collect(iter);
    ASSERT_EQ(descending.size(), 20u);
    EXPECT_EQ(descending.front(), "k19");
    EXPECT_EQ(descending.back(), "k00");

    // Bounds keep their forward meaning: lower inclusive, upper exclusive.
    ASSERT_EQ(elysiumkv_iter_create_reverse(db, reinterpret_cast<const uint8_t*>("k05"), 3,
                                          reinterpret_cast<const uint8_t*>("k10"), 3, &iter),
              ELYSIUMKV_OK);
    EXPECT_EQ(collect(iter),
              (std::vector<std::string>{"k09", "k08", "k07", "k06", "k05"}));

    ASSERT_EQ(elysiumkv_iter_prefix_reverse(db, reinterpret_cast<const uint8_t*>("k1"), 2, &iter),
              ELYSIUMKV_OK);
    const std::vector<std::string> prefixed = collect(iter);
    ASSERT_EQ(prefixed.size(), 10u);
    EXPECT_EQ(prefixed.front(), "k19");
    EXPECT_EQ(prefixed.back(), "k10");

    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// ARCHITECTURE.md "The ABI boundary" — the batched advance. The measurement that justified it: a Java scan costs
// ~419ns per entry through next/key/value and ~58ns batched, against ~36ns in
// C++. The correctness requirement is simply that it is the same scan.
TEST_F(CApiTest, TheBatchedAdvanceMatchesTheEntryAtATimeOne) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 500; ++i) {
        char key[24];
        std::snprintf(key, sizeof(key), "key:%06d", i);
        ASSERT_EQ(put(db, key, "value:" + std::to_string(i)), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);

    std::vector<std::string> one_at_a_time;
    {
        elysiumkv_iter* iter = nullptr;
        ASSERT_EQ(elysiumkv_iter_prefix(db, reinterpret_cast<const uint8_t*>("key:"), 4, &iter),
                  ELYSIUMKV_OK);
        while (elysiumkv_iter_next(iter)) {
            const uint8_t* key = nullptr;
            const uint8_t* value = nullptr;
            size_t key_len = 0, value_len = 0;
            elysiumkv_iter_key(iter, &key, &key_len);
            elysiumkv_iter_value(iter, &value, &value_len);
            one_at_a_time.push_back(std::string(reinterpret_cast<const char*>(key), key_len) + "=" +
                                    std::string(reinterpret_cast<const char*>(value), value_len));
        }
        elysiumkv_iter_destroy(iter);
    }

    // A buffer far too small for the whole scan, so refills are exercised.
    std::vector<std::string> batched;
    {
        elysiumkv_iter* iter = nullptr;
        ASSERT_EQ(elysiumkv_iter_prefix(db, reinterpret_cast<const uint8_t*>("key:"), 4, &iter),
                  ELYSIUMKV_OK);
        std::vector<uint8_t> buffer(256);
        while (true) {
            size_t count = 0, bytes = 0;
            ASSERT_EQ(elysiumkv_iter_next_batch(iter, buffer.data(), buffer.size(), &count, &bytes),
                      ELYSIUMKV_OK);
            if (count == 0) {
                if (bytes == 0) break;              // exhausted
                buffer.resize(bytes);               // one entry needs more room
                continue;
            }
            size_t offset = 0;
            for (size_t i = 0; i < count; ++i) {
                const auto read_u32 = [&](size_t at) {
                    return static_cast<uint32_t>(buffer[at]) |
                           static_cast<uint32_t>(buffer[at + 1]) << 8 |
                           static_cast<uint32_t>(buffer[at + 2]) << 16 |
                           static_cast<uint32_t>(buffer[at + 3]) << 24;
                };
                const uint32_t key_len = read_u32(offset);
                const uint32_t value_len = read_u32(offset + 4 + key_len);
                batched.push_back(
                    std::string(reinterpret_cast<const char*>(buffer.data() + offset + 4), key_len) +
                    "=" +
                    std::string(reinterpret_cast<const char*>(buffer.data() + offset + 8 + key_len),
                                value_len));
                offset += 8 + key_len + value_len;
            }
            EXPECT_EQ(offset, bytes) << "the declared byte count must match what was decoded";
        }
        EXPECT_EQ(elysiumkv_iter_status(iter), ELYSIUMKV_OK);
        elysiumkv_iter_destroy(iter);
    }

    ASSERT_EQ(one_at_a_time.size(), 500u);
    EXPECT_EQ(one_at_a_time, batched);
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

TEST_F(CApiTest, DataSurvivesCloseAndReopen) {
    elysiumkv_db* db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 300; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), "v" + std::to_string(i)), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_close(db), 0u);

    db = open();
    ASSERT_NE(db, nullptr);
    for (int i = 0; i < 300; ++i) {
        const std::string key = "key:" + std::to_string(i);
        uint8_t buffer[64];
        size_t len = 0;
        ASSERT_EQ(elysiumkv_get_copy(db, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                                   buffer, sizeof(buffer), &len),
                  ELYSIUMKV_OK)
            << key;
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(buffer), len), "v" + std::to_string(i));
    }
    EXPECT_EQ(elysiumkv_close(db), 0u);
}

// Null handles are what a buggy binding passes; none of these may crash.
TEST_F(CApiTest, NullArgumentsAreRejectedNotDereferenced) {
    const uint8_t* value = nullptr;
    size_t len = 0;
    uint64_t pin = 0;

    EXPECT_EQ(elysiumkv_get(nullptr, nullptr, 0, &value, &len, &pin), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_put(nullptr, nullptr, 0, nullptr, 0), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_delete(nullptr, nullptr, 0), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_flush(nullptr), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_write(nullptr, nullptr), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_open(nullptr, nullptr), ELYSIUMKV_CONFIG);
    EXPECT_EQ(elysiumkv_close(nullptr), 0u);
    EXPECT_EQ(elysiumkv_batch_size(nullptr), 0u);
    size_t stats_bytes = 0;
    EXPECT_EQ(elysiumkv_stats_snapshot(nullptr, nullptr, 0, &stats_bytes), ELYSIUMKV_CONFIG);
    EXPECT_FALSE(elysiumkv_iter_next(nullptr));
    elysiumkv_unpin(nullptr, 7);
    elysiumkv_iter_destroy(nullptr);
    elysiumkv_batch_destroy(nullptr);
    elysiumkv_options_destroy(nullptr);
}

// ARCHITECTURE.md "The ABI boundary" — the seams stay pluggable across the ABI — a binding can supply a store
// written in its own language. This one is written in C++ but reached only
// through the function-pointer vtable, which is the same path.
TEST_F(CApiTest, ABindingCanSupplyItsOwnBlobStore) {
    struct MemoryStore {
        std::map<std::string, std::string> objects;
        int puts = 0;
        int bulk_removes = 0;   // calls, not names
        int removed_names = 0;
    };
    static MemoryStore backing;
    backing = MemoryStore{};

    elysiumkv_blob_store_vtable vtable{};
    vtable.context = &backing;
    vtable.id = [](void*) -> const char* { return "vtable-store"; };
    vtable.get = [](void* context, const char* name, uint64_t offset, size_t len, uint8_t* out,
                    size_t* out_len) -> elysiumkv_status {
        auto* self = static_cast<MemoryStore*>(context);
        auto it = self->objects.find(name);
        if (it == self->objects.end()) return ELYSIUMKV_NOT_FOUND;
        if (offset >= it->second.size()) {
            *out_len = 0;
            return ELYSIUMKV_OK;
        }
        const size_t available = it->second.size() - offset;
        const size_t to_copy = available < len ? available : len;
        std::memcpy(out, it->second.data() + offset, to_copy);
        *out_len = to_copy;
        return ELYSIUMKV_OK;
    };
    vtable.put = [](void* context, const char* name, const uint8_t* bytes,
                    size_t len) -> elysiumkv_status {
        auto* self = static_cast<MemoryStore*>(context);
        if (self->objects.count(name) != 0) return ELYSIUMKV_UNUSABLE;  // write-once
        self->objects[name] = std::string(reinterpret_cast<const char*>(bytes), len);
        ++self->puts;
        return ELYSIUMKV_OK;
    };
    vtable.remove = [](void* context, const char* name) -> elysiumkv_status {
        auto* self = static_cast<MemoryStore*>(context);
        self->objects.erase(name);
        ++self->removed_names;
        return ELYSIUMKV_OK;
    };
    // The optional callback. A binding that leaves it NULL gets the engine's loop
    // over `remove`; supplying it is what lets a store written in the binding's
    // own language batch, which is the only way it can match what the built-in
    // remote store does.
    vtable.remove_many = [](void* context, const char* const* names,
                            size_t count) -> elysiumkv_status {
        auto* self = static_cast<MemoryStore*>(context);
        ++self->bulk_removes;
        for (size_t i = 0; i < count; ++i) {
            self->objects.erase(names[i]);
            ++self->removed_names;
        }
        return ELYSIUMKV_OK;
    };
    vtable.list = [](void* context, const char* prefix,
                     void (*emit)(void*, const char*), void* emit_context) -> elysiumkv_status {
        auto* self = static_cast<MemoryStore*>(context);
        for (const auto& [name, bytes] : self->objects) {
            if (name.compare(0, std::strlen(prefix), prefix) == 0) emit(emit_context, name.c_str());
        }
        return ELYSIUMKV_OK;
    };

    void* store = elysiumkv_blob_store_from_vtable(&vtable);
    ASSERT_NE(store, nullptr);

    elysiumkv_options* options = elysiumkv_options_create();
    ASSERT_EQ(elysiumkv_options_configure(options, catalog_, nullptr, 32u << 10, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, 0, 0, 0, 0, 0),
              ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_add_tier(options, store, ELYSIUMKV_DURABLE, 0, 0, 0), ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_set_level(options, 0, ELYSIUMKV_COMPRESSION_NONE, 0, 4, 0, 0, 0),
              ELYSIUMKV_OK);
    ASSERT_EQ(elysiumkv_options_set_level(options, 1, ELYSIUMKV_COMPRESSION_NONE, 0, 0, 0, 0, 0),
              ELYSIUMKV_OK);

    elysiumkv_db* db = nullptr;
    ASSERT_EQ(elysiumkv_open(options, &db), ELYSIUMKV_OK) << elysiumkv_last_error();
    elysiumkv_options_destroy(options);

    for (int i = 0; i < 400; ++i) {
        ASSERT_EQ(put(db, "key:" + std::to_string(i), std::string(80, 'v')), ELYSIUMKV_OK);
    }
    ASSERT_EQ(elysiumkv_flush(db), ELYSIUMKV_OK);
    EXPECT_GT(backing.puts, 0) << "the engine really did write through the vtable";

    for (int i = 0; i < 400; ++i) {
        const std::string key = "key:" + std::to_string(i);
        uint8_t buffer[128];
        size_t len = 0;
        ASSERT_EQ(elysiumkv_get_copy(db, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                                   buffer, sizeof(buffer), &len),
                  ELYSIUMKV_OK)
            << key;
        EXPECT_EQ(len, 80u);
    }

    // Obsolete-object collection must reach a binding-supplied store the same way
    // it reaches the built-in ones: in bulk. A level-0 compaction is the ordinary
    // occasion, so force one.
    ASSERT_EQ(elysiumkv_compact_level(db, 0), ELYSIUMKV_OK) << elysiumkv_last_error();
    EXPECT_GT(backing.removed_names, 0) << "nothing was collected, so this proves nothing";
    EXPECT_GT(backing.bulk_removes, 0)
        << "collection bypassed the vtable's remove_many and went per object";
    EXPECT_LT(backing.bulk_removes, backing.removed_names)
        << backing.bulk_removes << " calls for " << backing.removed_names
        << " objects — the batch callback is being used one name at a time";

    EXPECT_EQ(elysiumkv_close(db), 0u);
    elysiumkv_blob_store_destroy(store);
}

// ARCHITECTURE.md "Dependencies and artifacts" — **the ABI's shape does not vary with the build.** The remote
// constructors are declared and defined in every configuration, and
// elysiumkv_features() is how a binding learns whether they will work. If they
// vanished instead, a binding resolving symbols at load time would fail to load
// rather than fail to find a feature, and its coverage test could no longer be a
// set comparison.
//
// This test does not know which build it is in — ELYSIUMKV_WITH_AWS is private to
// the library — so it asserts the two configurations are each *self-consistent*,
// which is the property that matters.
TEST_F(CApiTest, TheRemoteConstructorsAgreeWithWhatTheBuildReports) {
    const bool has_aws = (elysiumkv_features() & ELYSIUMKV_FEATURE_AWS) != 0;

    void* store = reinterpret_cast<void*>(-1);
    const elysiumkv_status status = elysiumkv_s3_blob_store_create(
        "bucket", "prefix", nullptr, "http://127.0.0.1:1", "key", "secret", 0, 0, nullptr, &store);

    if (!has_aws) {
        EXPECT_EQ(status, ELYSIUMKV_CONFIG)
            << "a build without the remote implementations must say so as a configuration error, "
               "not as ELYSIUMKV_IO — there is nothing to retry";
        EXPECT_NE(std::string(elysiumkv_last_error()).find("ELYSIUMKV_BUILD_AWS"), std::string::npos)
            << "the message must name the missing build option, or the failure reads as 'S3 is "
               "broken' rather than 'this library does not contain it': "
            << elysiumkv_last_error();
        EXPECT_EQ(store, nullptr) << "the out-parameter is cleared even on the failing path";

        void* catalog = reinterpret_cast<void*>(-1);
        EXPECT_EQ(elysiumkv_s3_manifest_catalog_create("bucket", "prefix", nullptr, nullptr, nullptr,
                                                     nullptr, 0, 0, &catalog),
                  ELYSIUMKV_CONFIG);
        EXPECT_EQ(catalog, nullptr);
        catalog = reinterpret_cast<void*>(-1);
        EXPECT_EQ(elysiumkv_dynamo_manifest_catalog_create("table", "store", nullptr, nullptr, nullptr,
                                                         nullptr, 0, 0, &catalog),
                  ELYSIUMKV_CONFIG);
        EXPECT_EQ(catalog, nullptr);
        return;
    }

    // Construction reaches no network — an S3 client is configuration, not a
    // connection — so this succeeds against an endpoint nothing is listening on.
    // That is what lets a binding's coverage test exercise these without a
    // service behind them.
    ASSERT_EQ(status, ELYSIUMKV_OK) << elysiumkv_last_error();
    ASSERT_NE(store, nullptr);
    elysiumkv_blob_store_destroy(store);

    void* catalog = nullptr;
    ASSERT_EQ(elysiumkv_s3_manifest_catalog_create("bucket", "manifest", nullptr,
                                                 "http://127.0.0.1:1", "key", "secret", 0, 0,
                                                 &catalog),
              ELYSIUMKV_OK)
        << elysiumkv_last_error();
    elysiumkv_manifest_catalog_destroy(catalog);

    catalog = nullptr;
    ASSERT_EQ(elysiumkv_dynamo_manifest_catalog_create("table", "store", nullptr,
                                                     "http://127.0.0.1:1", "key", "secret", 0,
                                                     /*create_table_if_missing=*/0, &catalog),
              ELYSIUMKV_OK)
        << elysiumkv_last_error();
    elysiumkv_manifest_catalog_destroy(catalog);

    // The arguments that are wrong rather than absent, each named separately so a
    // single over-broad check cannot stand in for all three.
    void* rejected = reinterpret_cast<void*>(-1);
    EXPECT_EQ(elysiumkv_s3_blob_store_create("", nullptr, nullptr, nullptr, nullptr, nullptr, 0, 0,
                                           nullptr, &rejected),
              ELYSIUMKV_CONFIG)
        << "an empty bucket is a configuration error, not a store pointing at nothing";
    EXPECT_EQ(rejected, nullptr);

    rejected = reinterpret_cast<void*>(-1);
    EXPECT_EQ(elysiumkv_s3_blob_store_create("bucket", nullptr, nullptr, nullptr, nullptr, nullptr,
                                           -1, 0, nullptr, &rejected),
              ELYSIUMKV_CONFIG)
        << "a negative timeout would become an enormous unsigned duration — 'never time out' is "
           "not what the caller asked for";
    EXPECT_EQ(rejected, nullptr);

    rejected = reinterpret_cast<void*>(-1);
    EXPECT_EQ(elysiumkv_dynamo_manifest_catalog_create("table", "", nullptr, nullptr, nullptr,
                                                     nullptr, 0, 0, &rejected),
              ELYSIUMKV_CONFIG)
        << "an empty store_id would share one partition with every other store on the table";
    EXPECT_EQ(rejected, nullptr);

    // A null out-parameter must be reported, not dereferenced.
    EXPECT_EQ(elysiumkv_s3_blob_store_create("bucket", nullptr, nullptr, nullptr, nullptr, nullptr, 0,
                                           0, nullptr, nullptr),
              ELYSIUMKV_CONFIG);
}

}  // namespace
}  // namespace elysiumkv::test
