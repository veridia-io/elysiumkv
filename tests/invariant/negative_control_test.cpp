// The mutators these depend on are compiled only where the checks they trip are
// (ARCHITECTURE.md "Negative controls" — "test-only mutators behind the debug flag"), so the controls follow
// them. That is three presets of four — release builds the engine without the
// continuous checks, so there is nothing there to control.
#include "db/db_impl.hpp"

#ifdef ELYSIUMKV_PARANOID

#include "support/test_db.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Negative controls" — the negative controls for the continuous invariants.
///
/// `check_invariants()` is asserted `== Status::Ok` in a dozen places across
/// this suite, and every one of those assertions would keep passing if the
/// function were reduced to `return Status::Ok`. That is the shape the section
/// is about: green then means either *no defect* or *the check did not run*,
/// and nothing distinguishes them.
///
/// One injection per constituent, each asserting **its own** invariant is the
/// one reported. A single injection would only prove that one check still
/// fires; the realistic decay is one of five quietly ceasing to while the
/// others keep the bundle looking thorough.
class InvariantNegativeControl : public ::testing::Test {
protected:
    void SetUp() override {
        Options options = make_options(store_, Compression::None, /*memtable_bytes=*/4u << 10);
        options.background = BackgroundMode::Inline;
        options.paranoid_checks = true;
        auto opened = DbImpl::open(options, /*require_all_durable=*/true);
        ASSERT_TRUE(opened.has_value());
        db_ = std::move(opened->db);

        // Two levels' worth of files, so the overlap case has somewhere to go.
        for (int i = 0; i < 120; ++i) {
            ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(std::string(64, 'v'))),
                      Status::Ok);
        }
        ASSERT_EQ(db_->flush(), Status::Ok);
        ASSERT_EQ(engine().compact_level(0), Status::Ok);

        Invariant which = Invariant::None;
        ASSERT_EQ(engine().check_invariants(&which), Status::Ok)
            << "the fixture must start clean, or an injection proves nothing";
        ASSERT_EQ(which, Invariant::None);
    }

    static std::string key_at(int i) {
        char buffer[32];
        std::snprintf(buffer, sizeof(buffer), "key:%06d", i);
        return buffer;
    }

    DbImpl& engine() { return *static_cast<DbImpl*>(db_.get()); }

    /// Breaks exactly one invariant and asserts the check names that one.
    void expect_caught(Invariant injected) {
        ASSERT_EQ(engine().break_invariant_for_test(injected), Status::Ok)
            << "could not construct the violating state for "
            << invariant_name(injected);

        Invariant reported = Invariant::None;
        const Status status = engine().check_invariants(&reported);

        EXPECT_EQ(status, Status::Corrupt)
            << "injected " << invariant_name(injected) << " and the check passed";
        EXPECT_EQ(reported, injected)
            << "injected " << invariant_name(injected) << " but the check reported "
            << invariant_name(reported)
            << " — a control that accepts any failure is vacuous in exactly the way "
               "ARCHITECTURE.md - Negative controls warns about";
    }

    TestStore store_{1};
    std::unique_ptr<DB> db_;
};

TEST_F(InvariantNegativeControl, AMissingObjectIsCaught) {
    expect_caught(Invariant::ObjectMissing);
}

TEST_F(InvariantNegativeControl, AFileNamingAnUnknownStoreIsCaught) {
    expect_caught(Invariant::StoreMissing);
}

TEST_F(InvariantNegativeControl, AWrongEntryCountIsCaught) {
    expect_caught(Invariant::EntryCount);
}

TEST_F(InvariantNegativeControl, AWrongKeyRangeIsCaught) {
    expect_caught(Invariant::KeyRange);
}

TEST_F(InvariantNegativeControl, OverlapBelowL0IsCaught) {
    expect_caught(Invariant::LevelOverlap);
}

/// The control on the controls: nothing above proves the check can also *pass*,
/// and a check wired to always report Corrupt would satisfy all five.
TEST_F(InvariantNegativeControl, ACleanEngineReportsNothing) {
    for (int i = 120; i < 240; ++i) {
        ASSERT_EQ(db_->put(Slice::from(key_at(i)), Slice::from(std::string(64, 'v'))), Status::Ok);
    }
    ASSERT_EQ(db_->flush(), Status::Ok);

    Invariant which = Invariant::LevelOverlap;   // deliberately not None
    EXPECT_EQ(engine().check_invariants(&which), Status::Ok);
    EXPECT_EQ(which, Invariant::None) << "a passing check must clear the out-parameter";
}

}  // namespace
}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_PARANOID
