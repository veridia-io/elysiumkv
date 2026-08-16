/* `elysiumkv stats` — the shape of a store, from its manifest alone.
 *
 * It does not open the store. Opening needs every tier reachable, and the hot tier of a running
 * service is on that machine's disk — so a command that opened would only work from inside the pod
 * it was asking about, which is where it is least useful. Everything here is derived from the file
 * list, so it works wherever the manifest does.
 *
 * The cost is that two things cannot be known from here: whether the files still exist (that is
 * `verify`), and the engine's activity counters — flushes, compactions, cache hits — which are
 * atomics on a live handle and belong to the writing process.
 */

#include "commands.hpp"
#include "catalog.hpp"
#include "version_load.hpp"

#include "version/version_edit.hpp"
#include "version/watermark.hpp"

#include <nlohmann/json.hpp>
#include <tabulate/table.hpp>

#include <chrono>
#include <cstdio>
#include <iostream>
#include <map>
#include <string>

namespace elysiumkv::cli {
namespace {

std::string mib(uint64_t bytes) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%.1f", static_cast<double>(bytes) / 1048576.0);
    return buf;
}

std::string age(uint64_t oldest_ms, uint64_t now_ms) {
    if (oldest_ms == 0 || oldest_ms > now_ms) return "-";
    const uint64_t ms = now_ms - oldest_ms;
    char buf[32];
    if (ms < 60'000) std::snprintf(buf, sizeof buf, "%.1fs", static_cast<double>(ms) / 1e3);
    else if (ms < 3'600'000) std::snprintf(buf, sizeof buf, "%.1fm", static_cast<double>(ms) / 6e4);
    else std::snprintf(buf, sizeof buf, "%.1fh", static_cast<double>(ms) / 3.6e6);
    return buf;
}

void style(tabulate::Table& table) {
    table.format().hide_border_top().hide_border_bottom().padding_left(1).padding_right(1);
    table[0].format().show_border_bottom();
    if (table.size() > 1) table[1].format().show_border_top();
    for (size_t column = 0; column < table[0].size(); ++column) {
        bool numeric = true;
        for (size_t row = 1; row < table.size() && numeric; ++row) {
            const std::string text = table[row][column].get_text();
            numeric = !text.empty() && text.find_first_not_of("0123456789.") == std::string::npos;
        }
        if (numeric) table.column(column).format().font_align(tabulate::FontAlign::right);
    }
}

struct Group {
    size_t files = 0;
    uint64_t bytes = 0, entries = 0, tombstones = 0;
    uint64_t oldest_write_ms = 0;   ///< the minimum min_write_time_ms, so the largest age

    void add(const FileMetadata& f) {
        ++files;
        bytes += f.file_bytes;
        entries += f.num_entries;
        tombstones += f.num_tombstones;
        if (oldest_write_ms == 0 || f.min_write_time_ms < oldest_write_ms) {
            oldest_write_ms = f.min_write_time_ms;
        }
    }
};

/// What a restore would resume after, by the engine's own rule rather than a second reading of it.
/// Every file counts as a survivor: whether one is actually missing is `verify`'s question, and
/// assuming loss here would under-report on a healthy store.
std::optional<uint64_t> resume_after(const Version& v) {
    RecoveryWatermark rule;
    for (const auto& level : v.levels()) {
        for (const FileMetadata& f : level) rule.observe_survivor(f.watermark);
    }
    return rule.resume_after();
}

uint64_t now_ms() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

}  // namespace

void add_stats_command(CLI::App& root, const GlobalOptions& globals) {
    auto* command = root.add_subcommand(
        "stats", "what each level and tier holds, how old it is, and where a restore resumes");

    auto options = std::make_shared<CatalogOptions>();
    auto generation = std::make_shared<uint64_t>(0);
    add_catalog_flags(*command, *options);
    auto* generation_flag = command->add_option("--generation", *generation,
                                                "read this generation instead of the current one");

    command->callback([options, generation, generation_flag, &globals]() {
        std::shared_ptr<ManifestCatalog> catalog = open_catalog(*options);
        const uint64_t chosen =
            generation_flag->count() ? *generation : current_generation(*catalog);
        size_t edits = 0;
        std::shared_ptr<const Version> v = load_version(*catalog, chosen, edits);
        const uint64_t now = now_ms();

        std::map<int, Group> by_level;
        std::map<std::string, Group> by_tier;
        Group all;
        for (const auto& level : v->levels()) {
            for (const FileMetadata& f : level) {
                by_level[f.level].add(f);
                by_tier[f.store_id].add(f);
                all.add(f);
            }
        }
        const std::optional<uint64_t> watermark = resume_after(*v);

        if (globals.json) {
            nlohmann::json out;
            out["generation"] = chosen;
            out["edits_replayed"] = edits;
            out["resume_after"] = watermark ? nlohmann::json(*watermark) : nlohmann::json();
            out["totals"] = {{"files", all.files}, {"bytes", all.bytes},
                             {"entries", all.entries}, {"tombstones", all.tombstones}};
            out["levels"] = nlohmann::json::array();
            for (const auto& [level, g] : by_level) {
                out["levels"].push_back({{"level", level}, {"files", g.files}, {"bytes", g.bytes},
                                         {"entries", g.entries}, {"tombstones", g.tombstones},
                                         {"oldest_write_time_ms", g.oldest_write_ms}});
            }
            out["tiers"] = nlohmann::json::array();
            for (const auto& [tier, g] : by_tier) {
                out["tiers"].push_back({{"tier", tier}, {"files", g.files}, {"bytes", g.bytes},
                                        {"oldest_write_time_ms", g.oldest_write_ms}});
            }
            out["not_available_here"] = {
                {"activity_counters", "atomics on a live handle; ask the writing process"},
                {"files_present", "run `verify` — this reads the manifest, not the stores"}};
            std::cout << out.dump(2) << "\n";
            return;
        }

        std::printf("generation    : %llu (%zu edit(s) replayed)\n",
                    static_cast<unsigned long long>(chosen), edits);
        if (watermark) {
            std::printf("resume after  : %llu\n", static_cast<unsigned long long>(*watermark));
        } else {
            // Absent is not zero: zero is a position, and a restore told to resume there would
            // skip the whole log.
            std::printf("resume after  : (nothing certified — a restore starts from the beginning)\n");
        }
        std::printf("live files    : %zu, %s MiB, %llu entries, %llu tombstones\n\n", all.files,
                    mib(all.bytes).c_str(), static_cast<unsigned long long>(all.entries),
                    static_cast<unsigned long long>(all.tombstones));

        tabulate::Table levels;
        levels.add_row({"level", "files", "MiB", "entries", "tombstones", "oldest"});
        for (const auto& [level, g] : by_level) {
            levels.add_row({"L" + std::to_string(level), std::to_string(g.files), mib(g.bytes),
                            std::to_string(g.entries), std::to_string(g.tombstones),
                            age(g.oldest_write_ms, now)});
        }
        style(levels);
        std::cout << "levels:\n" << levels << "\n\n";

        tabulate::Table tiers;
        tiers.add_row({"tier", "files", "MiB", "oldest"});
        for (const auto& [tier, g] : by_tier) {
            tiers.add_row({tier, std::to_string(g.files), mib(g.bytes), age(g.oldest_write_ms, now)});
        }
        style(tiers);
        std::cout << "tiers:\n" << tiers << "\n";
    });
}

}  // namespace elysiumkv::cli
