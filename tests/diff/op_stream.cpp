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
            for (const auto& [is_delete, batch_key, batch_value] : batch) {
                out += is_delete ? " remove(" + quote(batch_key) + ")"
                                 : " put(" + quote(batch_key) + ", " + quote(batch_value) + ")";
            }
            return out + " }";
        }
        case Kind::ScanAll: return "scan_all()";
        case Kind::ScanRange: return "scan_range(" + quote(key) + ", " + quote(upper) + ")";
        case Kind::ScanPrefix: return "scan_prefix(" + quote(key) + ")";
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
        } else if (choice < 78) {
            op.kind = DiffOp::Kind::Get;
            op.key = key_for(rng, options.distinct_keys);
        } else if (choice < 84) {
            op.kind = DiffOp::Kind::Batch;
            for (int j = 0; j < 8; ++j) {
                const bool is_delete = rng() % 4 == 0;
                std::string key = key_for(rng, options.distinct_keys);
                std::string value = is_delete ? std::string() : value_for(rng);
                op.batch.emplace_back(is_delete, std::move(key), std::move(value));
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
        } else if (choice < 96) {
            op.kind = DiffOp::Kind::IterAcrossCompaction;
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
