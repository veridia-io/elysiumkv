#ifndef ELYSIUMKV_UTIL_BUDGET_CHARGE_HPP
#define ELYSIUMKV_UTIL_BUDGET_CHARGE_HPP

#include "elysiumkv/memory_budget.hpp"

#include <cstddef>
#include <memory>

namespace elysiumkv {

/// Charges a transient allocation to the process-wide budget for as long as it is held.
///
/// **For the buffers that are large, short-lived and were invisible.** A migration reads a whole
/// file into memory before writing it out, and a flush or compaction materialises a whole output
/// file before sending it — neither was charged, in a design whose central memory argument is that
/// per-instance sizing multiplies by instance count. Two concurrent migrations of a 400 MiB file
/// were 800 MiB the budget could not see.
///
/// **Charged over the limit rather than refused.** `try_acquire` is for a consumer that can
/// decline; this one cannot usefully. Refusing a migration would leave files on a transient tier
/// past the age the engine promises to move them within, turning a memory decision into a
/// durability one. So the budget is told the truth and the write path acts on the overage, exactly
/// as it does for a memtable arena.
class BudgetCharge {
public:
    BudgetCharge(const std::shared_ptr<MemoryBudget>& budget, size_t bytes)
        : budget_(budget), bytes_(bytes) {
        if (budget_ != nullptr && bytes_ != 0) (void)budget_->try_acquire_over(bytes_);
    }

    ~BudgetCharge() {
        if (budget_ != nullptr && bytes_ != 0) budget_->release(bytes_);
    }

    BudgetCharge(const BudgetCharge&) = delete;
    BudgetCharge& operator=(const BudgetCharge&) = delete;

private:
    std::shared_ptr<MemoryBudget> budget_;
    size_t bytes_ = 0;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_UTIL_BUDGET_CHARGE_HPP
