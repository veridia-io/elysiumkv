#ifndef ELYSIUMKV_IO_COUNTERS_HPP
#define ELYSIUMKV_IO_COUNTERS_HPP

#include <cstdint>

namespace elysiumkv {

/// What one store has been asked to do, since it was opened.
///
/// **Requests as much as bytes**, because object storage bills for both and nothing here could see
/// either. Every layer counts what it was asked for, so a cache and its delegate report different
/// figures; `Stats` takes the tier's authoritative store, which is the one on the invoice.
///
/// Errors are counted whether or not the caller retried past them, for the same reason
/// `background_failures` is: a store working through a degraded endpoint otherwise looks healthy.
struct IoCounters {
    uint64_t gets = 0;
    uint64_t puts = 0;
    uint64_t removes = 0;
    uint64_t lists = 0;
    uint64_t bytes_read = 0;
    uint64_t bytes_written = 0;
    uint64_t errors = 0;

    bool operator==(const IoCounters&) const = default;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_IO_COUNTERS_HPP
