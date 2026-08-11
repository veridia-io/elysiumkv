#include "diff/op_stream.hpp"

#include <cstdio>
#include <random>

namespace elysiumkv::test {
namespace {

std::string quote(const std::string& text, size_t limit = 24) {
    std::string out = "\"";
    for (size_t i = 0; i < text.size() && i < limit; ++i) {
        const auto c = static_cast<unsigned char>(text[i]);
        if (c >= 0x20 && c < 0x7F) {
            out.push_back(static_cast<char>(c));
        } else {
            char buf[8];
            std::snprintf(buf, sizeof(buf), "\\x%02X", c);
            out += buf;
        }
    }
    if (text.size() > limit) out += "...";
    out.push_back('"');
    return out;
}

/// Heavy prefix clustering, as ARCHITECTURE.md "The differential oracle" requires: keys share long prefixes, so both
/// prefix scans and the block format's prefix compression are exercised.
std::string key_for(std::mt19937_64& rng, int distinct_keys) {
    const int cluster = static_cast<int>(rng() % 16);
    const int id = static_cast<int>(rng() % static_cast<unsigned>(distinct_keys));
    char buf[64];
    std::snprintf(buf, sizeof(buf), "cluster:%02d:key:%08d", cluster, id);
    return buf;
}

/// A truncation point, drawn from the bottom cluster only.
///
/// Not `key_for`: the floor is monotone and the keyspace has sixteen clusters, so truncating below
/// a uniformly drawn key erases about half the store every time it fires. A run doing that thirty
/// times keeps almost nothing alive, and the other operations are then agreeing about an empty
/// store — which is how this first showed up, as the tight-budget configs reporting that they had
/// tested nothing. Confining the floor to cluster 00 makes it creep, which is also what a real
/// caller does.
std::string truncation_key_for(std::mt19937_64& rng, int distinct_keys) {
    const int id = static_cast<int>(rng() % static_cast<unsigned>(distinct_keys));
    char buf[64];
    std::snprintf(buf, sizeof(buf), "cluster:00:key:%08d", id);
    return buf;
}

std::string value_for(std::mt19937_64& rng) {
    // Zero-length values are included deliberately. An empty value is a value —
    // distinct from a tombstone — and a generator with a floor of 8 can never
    // tell the two apart, so nothing in this suite ever exercised the case.
    const size_t size = rng() % 208;
    std::string value(size, '\0');
    for (size_t i = 0; i < size; ++i) value[i] = static_cast<char>('a' + (rng() % 26));
    return value;
}

std::string prefix_for(std::mt19937_64& rng) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "cluster:%02d:", static_cast<int>(rng() % 16));
    return buf;
}

}  // namespace

std::string DiffOp::describe() const {
    switch (kind) {
        case Kind::Put: return "put(" + quote(key) + ", " + quote(value) + ")";
        case Kind::Remove: return "remove(" + quote(key) + ")";
        case Kind::Get: return "get(" + quote(key) + ")";
        case Kind::Batch: {
            std::string out = "batch{";
            for (const auto& [batch_kind, batch_key, batch_value] : batch) {
                out += batch_kind == Kind::DeleteRange
                           ? " delete_range(" + quote(batch_key) + ", " + quote(batch_value) + ")"
                       : batch_kind == Kind::Remove ? " remove(" + quote(batch_key) + ")"
                                 : " put(" + quote(batch_key) + ", " + quote(batch_value) + ")";
            }
            return out + " }";
        }
        case Kind::ScanAll: return "scan_all()";
        case Kind::ScanRange: return "scan_range(" + quote(key) + ", " + quote(upper) + ")";
        case Kind::ScanPrefix: return "scan_prefix(" + quote(key) + ")";
        case Kind::TruncateBelow: return "truncate_below(" + quote(key) + ")";
        case Kind::DeleteRange: return "delete_range(" + quote(key) + ", " + quote(upper) + ")";
        case Kind::ReverseScanAll: return "reverse_scan_all()";
        case Kind::ReverseScanRange:
            return "reverse_scan_range(" + quote(key) + ", " + quote(upper) + ")";
        case Kind::ReverseScanPrefix: return "reverse_scan_prefix(" + quote(key) + ")";
        case Kind::ReverseIterAcrossCompaction: return "reverse_iter_across_compaction()";
        case Kind::Flush: return "flush()";
        case Kind::Compact: return "compact()";
        case Kind::IterAcrossFlush: return "iterate_across_flush()";
        case Kind::IterAcrossCompaction: return "iterate_across_compaction()";
        case Kind::Reopen: return "reopen()";
        case Kind::Kill: return "kill()";
    }
    return "unknown()";
}

std::string describe_ops(const std::vector<DiffOp>& ops) {
    std::string out;
    for (size_t i = 0; i < ops.size(); ++i) {
        // Wide enough for any size_t: gcc's -Wformat-truncation counts the
        // digits %zu can produce, not the ones this loop will reach.
        char index[32];
        std::snprintf(index, sizeof(index), "%4zu: ", i);
        out += index;
        out += ops[i].describe();
        out.push_back('\n');
    }
    return out;
}

std::vector<DiffOp> generate_ops(uint64_t seed, int count, GeneratorOptions options) {
    std::mt19937_64 rng(seed);
    std::vector<DiffOp> ops;
    ops.reserve(static_cast<size_t>(count));

    for (int i = 0; i < count; ++i) {
        DiffOp op;
        const int choice = static_cast<int>(rng() % 100);

        if (choice < 45) {
            op.kind = DiffOp::Kind::Put;
            op.key = key_for(rng, options.distinct_keys);
            op.value = value_for(rng);
        } else if (choice < 60) {
            op.kind = DiffOp::Kind::Remove;
            op.key = key_for(rng, options.distinct_keys);
        } else if (choice < 72) {
            op.kind = DiffOp::Kind::Get;
            op.key = key_for(rng, options.distinct_keys);
        } else if (choice < 74) {
            op.kind = DiffOp::Kind::DeleteRange;
            // Two keys from the ordinary keyspace, ordered — so most ranges land inside one
            // cluster and some straddle several. Left unordered one time in eight, because an
            // inverted range is a no-op the engine has to get right and a generator that only ever
            // produced valid ones would never ask.
            std::string a = key_for(rng, options.distinct_keys);
            std::string b = key_for(rng, options.distinct_keys);
            if (rng() % 8 != 0 && b < a) std::swap(a, b);
            op.key = std::move(a);
            op.upper = std::move(b);
        } else if (choice < 75) {
            op.kind = DiffOp::Kind::TruncateBelow;
            op.key = truncation_key_for(rng, options.distinct_keys);
        } else if (choice < 76) {
            op.kind = DiffOp::Kind::ReverseScanAll;
        } else if (choice < 77) {
            op.kind = DiffOp::Kind::ReverseScanRange;
            op.key = key_for(rng, options.distinct_keys);
            op.upper = key_for(rng, options.distinct_keys);
            if (op.upper < op.key) std::swap(op.key, op.upper);
        } else if (choice < 78) {
            op.kind = DiffOp::Kind::ReverseScanPrefix;
            op.key = prefix_for(rng);
        } else if (choice < 84) {
            op.kind = DiffOp::Kind::Batch;
            for (int j = 0; j < 8; ++j) {
                const unsigned roll = rng() % 16;
                if (roll == 0) {
                    // One in sixteen, and deliberately mixed in among the puts rather than placed
                    // at either end: what this exists to exercise is a put on one side of a range
                    // being covered and a put on the other side surviving.
                    std::string a = key_for(rng, options.distinct_keys);
                    std::string b = key_for(rng, options.distinct_keys);
                    if (b < a) std::swap(a, b);
                    op.batch.emplace_back(DiffOp::Kind::DeleteRange, std::move(a), std::move(b));
                    continue;
                }
                const bool is_delete = roll % 4 == 0;
                std::string key = key_for(rng, options.distinct_keys);
                std::string value = is_delete ? std::string() : value_for(rng);
                op.batch.emplace_back(is_delete ? DiffOp::Kind::Remove : DiffOp::Kind::Put,
                                      std::move(key), std::move(value));
            }
        } else if (choice < 86) {
            op.kind = DiffOp::Kind::ScanAll;
        } else if (choice < 88) {
            op.kind = DiffOp::Kind::ScanRange;
            op.key = key_for(rng, options.distinct_keys);
            op.upper = key_for(rng, options.distinct_keys);
            if (op.upper < op.key) std::swap(op.key, op.upper);
        } else if (choice < 92) {
            op.kind = DiffOp::Kind::ScanPrefix;
            op.key = prefix_for(rng);
        } else if (choice < 94) {
            op.kind = DiffOp::Kind::IterAcrossFlush;
        } else if (choice < 95) {
            op.kind = DiffOp::Kind::IterAcrossCompaction;
        } else if (choice < 96) {
            op.kind = DiffOp::Kind::ReverseIterAcrossCompaction;
        } else if (choice < 97) {
            op.kind = DiffOp::Kind::Compact;
        } else if (choice < 99) {
            op.kind = DiffOp::Kind::Flush;
        } else if (options.allow_kills && rng() % 2 == 0) {
            op.kind = DiffOp::Kind::Kill;
        } else {
            op.kind = DiffOp::Kind::Reopen;
        }
        ops.push_back(std::move(op));
    }
    return ops;
}

}  // namespace elysiumkv::test
