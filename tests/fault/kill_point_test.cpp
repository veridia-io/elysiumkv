#include "diff/op_stream.hpp"
#include "diff/replay.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

/// ARCHITECTURE.md "Fault injection" — fault cases run in the same synchronous mode, **with kill points
/// expressed as positions in the op stream**, so they reproduce from a seed like
/// any other operation. A `Kill` op drops the process: everything since the last
/// successful flush is lost (ARCHITECTURE.md "Positional recency"), everything before it must come back exactly,
/// and the replayer checks the whole store against the oracle immediately after
/// the reopen.
class KillPointTest : public ::testing::TestWithParam<ReplayConfig> {};

TEST_P(KillPointTest, SurvivesKillsAtArbitraryPointsInTheStream) {
    const ReplayConfig config = GetParam();
    GeneratorOptions generator;
    generator.allow_kills = true;

    for (int seed = 1; seed <= 4; ++seed) {
        const std::vector<DiffOp> ops =
            generate_ops(static_cast<uint64_t>(seed) * 977, 1500, generator);

        // Only worth running if the stream actually contains kills.
        size_t kills = 0;
        for (const DiffOp& op : ops) {
            if (op.kind == DiffOp::Kind::Kill) ++kills;
        }
        ASSERT_GT(kills, 0u) << "seed " << seed << " generated no kill points";

        auto failure = replay(ops, config);
        if (!failure.has_value()) continue;

        const std::vector<DiffOp> minimal = shrink(ops, config);
        FAIL() << "\nkill-point mismatch\n  config: " << config.name << "\n  seed:   " << seed
               << "\n  failed: operation " << failure->op_index << "\n  message: "
               << failure->message << "\n\nshrunk to " << minimal.size() << " operations\n\n"
               << describe_ops(minimal);
    }
}

INSTANTIATE_TEST_SUITE_P(
    Configs, KillPointTest,
    ::testing::Values(ReplayConfig{"NoCompression", Compression::None, false, 64u << 10},
                      ReplayConfig{"Zstd", Compression::Zstd, false, 64u << 10},
                      ReplayConfig{"TwoStores", Compression::Zstd, true, 64u << 10}),
    [](const auto& scenario) { return scenario.param.name; });  // not `info`: gtest shadows it

}  // namespace
}  // namespace elysiumkv::test
