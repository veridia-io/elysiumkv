#ifndef ELYSIUMKV_SST_BLOOM_HPP
#define ELYSIUMKV_SST_BLOOM_HPP

#include "elysiumkv/slice.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace elysiumkv {

/// ARCHITECTURE.md "Inside an SST" — one filter per SST covering all its keys. Blocked layout: 512-bit
/// blocks, so every probe for a key touches one cache line. Double hashing
/// (h1 + i*h2) derives all probes from a single xxHash64.
///
/// Filters serve point lookups only; prefix scans prune by SST key range
/// instead, so nothing here is tuned for the scan path.
///
/// Encoding: `bitmap ‖ num_blocks uint32 ‖ num_probes uint8`. Stored raw, never
/// compressed (ARCHITECTURE.md "Inside an SST").
class BloomBuilder {
public:
    BloomBuilder(int bits_per_key, int num_probes);

    void add(Slice key);
    bool empty() const { return hashes_.empty(); }
    std::string finish();
    void reset() { hashes_.clear(); }

private:
    std::vector<uint64_t> hashes_;
    int bits_per_key_;
    int num_probes_;
};

/// Conservative on a malformed filter: reports "may contain" so a damaged filter
/// costs a data-block read rather than a wrong answer.
bool bloom_may_contain(Slice filter, Slice key);

}  // namespace elysiumkv

#endif  // ELYSIUMKV_SST_BLOOM_HPP
