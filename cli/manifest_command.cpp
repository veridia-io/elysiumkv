/* `elysiumkv manifest` — what a reopen would load.
 *
 * A manifest is a snapshot plus the edits committed after it, so reading one means replaying:
 * take the generation the pointer names, decode its snapshot, apply its edits in order. That is
 * what recovery does, and doing the same here is the only way to report the file list an operator
 * actually has rather than the one the snapshot alone describes.
 *
 * It decodes nothing itself. A tool that reimplements the wire format is a second definition
 * of it, and the two drift the first time a field is added — which already happened once, where a
 * snapshot read of an *edit* succeeded and quietly left a byte over, because the layouts share a
 * head. This calls `decode_version_snapshot`, `decode_version_edit` and `Version::apply`, so a
 * format change breaks the build instead of the answer.
 */

#include "catalog.hpp"
#include "commands.hpp"
#include "encryption.hpp"
#include "version_load.hpp"

#include "version/version.hpp"
#include "version/version_edit.hpp"

#include <nlohmann/json.hpp>
#include <tabulate/table.hpp>

#include <algorithm>
#include <iostream>

#include <unistd.h>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv::cli {
namespace {

std::string mib(uint64_t bytes) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%.1f", static_cast<double>(bytes) / 1048576.0);
    return buf;
}

/// One look for every table here, so a reader does not have to work out whether two of them are
/// showing the same kind of thing.
///
/// Alignment is decided by the data, not by a hand-maintained list of column indices: a column
/// whose every cell parses as a number is right-aligned, everything else is left. That keeps
/// digits lined up for comparison without a new table needing its own rules.
///
/// Bold only on a terminal. `tabulate` emits ANSI escapes unconditionally, and this output is
/// meant to be redirected — a `> report.txt` full of escape sequences is exactly the badly-shaped
/// output a table library is supposed to prevent.
void style(tabulate::Table& table) {
    const bool tty = isatty(fileno(stdout)) != 0;
    table.format().hide_border_top().hide_border_bottom().padding_left(1).padding_right(1);
    // Borders are shared between adjacent rows, so the rule under the header needs both halves
    // asking for it — row 0's bottom alone renders nothing.
    table[0].format().show_border_bottom();
    if (table.size() > 1) table[1].format().show_border_top();
    if (tty) table[0].format().font_style({tabulate::FontStyle::bold});

    const size_t columns = table[0].size();
    for (size_t column = 0; column < columns; ++column) {
        bool numeric = true;
        // Row 0 is the header — a label whatever the column holds — so alignment is decided from
        // the data rows alone.
        for (size_t row = 1; row < table.size() && numeric; ++row) {
            const std::string text = table[row][column].get_text();
            numeric = !text.empty() && text.find_first_not_of("0123456789.") == std::string::npos;
        }
        if (numeric) table.column(column).format().font_align(tabulate::FontAlign::right);
    }
}

const char* codec_name(Compression c) {
    switch (c) {
        case Compression::None: return "none";
        case Compression::Lz4: return "lz4";
        case Compression::Zstd: return "zstd";
    }
    return "?";
}

struct Totals {
    size_t files = 0;
    uint64_t bytes = 0, entries = 0, tombstones = 0, range_tombstones = 0;

    void add(const FileMetadata& f) {
        ++files;
        bytes += f.file_bytes;
        entries += f.num_entries;
        tombstones += f.num_tombstones;
        range_tombstones += f.num_range_tombstones;
    }
};

void print_text(const Version& v, uint64_t generation, size_t edits) {
    Totals all;
    std::map<std::string, Totals> by_store;
    for (const auto& level : v.levels()) {
        for (const FileMetadata& f : level) {
            all.add(f);
            by_store[f.store_id].add(f);
        }
    }

    std::printf("generation       : %llu\n", static_cast<unsigned long long>(generation));
    std::printf("edits replayed   : %zu\n", edits);
    std::printf("next_file_number : %llu\n", static_cast<unsigned long long>(v.next_file_number()));
    std::printf("truncation_point : %s\n",
                v.truncation_point().empty() ? "(none)" : v.truncation_point().c_str());
    std::printf("live files       : %zu, %.1f MiB, %llu entries, %llu tombstones\n\n", all.files,
                static_cast<double>(all.bytes) / 1048576.0,
                static_cast<unsigned long long>(all.entries),
                static_cast<unsigned long long>(all.tombstones));

    tabulate::Table levels;
    levels.add_row({"level", "files", "MiB", "entries"});
    for (size_t level = 0; level < v.levels().size(); ++level) {
        Totals t;
        for (const FileMetadata& f : v.levels()[level]) t.add(f);
        levels.add_row({"L" + std::to_string(level), std::to_string(t.files),
                        mib(t.bytes), std::to_string(t.entries)});
    }
    style(levels);
    std::cout << "by level:\n" << levels << "\n\n";

    tabulate::Table tiers;
    tiers.add_row({"tier", "files", "MiB"});
    for (const auto& [store, t] : by_store) {
        tiers.add_row({store, std::to_string(t.files), mib(t.bytes)});
    }
    style(tiers);
    std::cout << "by tier:\n" << tiers << "\n\n";

    tabulate::Table files;
    files.add_row({"L", "file", "tier", "bytes", "entries", "tomb", "codec", "write window (ms)"});
    for (const auto& level : v.levels()) {
        for (const FileMetadata& f : level) {
            files.add_row({std::to_string(f.level), std::to_string(f.file_number), f.store_id,
                           std::to_string(f.file_bytes), std::to_string(f.num_entries),
                           std::to_string(f.num_tombstones), codec_name(f.compression),
                           std::to_string(f.min_write_time_ms) + ".." +
                               std::to_string(f.max_write_time_ms)});
        }
    }
    style(files);
    std::cout << files << "\n";
}

void print_json(const Version& v, uint64_t generation, size_t edits) {
    Totals all;
    for (const auto& level : v.levels()) {
        for (const FileMetadata& f : level) all.add(f);
    }

    nlohmann::json out;
    out["generation"] = generation;
    out["edits_replayed"] = edits;
    out["next_file_number"] = v.next_file_number();
    out["truncation_point"] = v.truncation_point();
    out["totals"] = {{"files", all.files},
                     {"bytes", all.bytes},
                     {"entries", all.entries},
                     {"tombstones", all.tombstones},
                     {"range_tombstones", all.range_tombstones}};

    out["compaction_pointers"] = nlohmann::json::object();
    for (const auto& [level, key] : v.compaction_pointers()) {
        out["compaction_pointers"][std::to_string(level)] = key;
    }

    out["files"] = nlohmann::json::array();
    for (const auto& level : v.levels()) {
        for (const FileMetadata& f : level) {
            out["files"].push_back({
                {"level", f.level},
                {"file_number", f.file_number},
                {"store_id", f.store_id},
                {"bytes", f.file_bytes},
                {"entries", f.num_entries},
                {"tombstones", f.num_tombstones},
                {"range_tombstones", f.num_range_tombstones},
                {"compression", codec_name(f.compression)},
                {"min_write_time_ms", f.min_write_time_ms},
                {"max_write_time_ms", f.max_write_time_ms},
                {"smallest_key", f.smallest_key},
                {"largest_key", f.largest_key},
                // null rather than a sentinel: absent is a real state — a file older than the
                // first watermark has no lower bound — and -1 would read as a position.
                {"watermark_low", f.watermark.low ? nlohmann::json(*f.watermark.low) : nlohmann::json()},
                {"watermark_high", f.watermark.high ? nlohmann::json(*f.watermark.high) : nlohmann::json()},
            });
        }
    }
    std::cout << out.dump(2) << "\n";
}

}  // namespace

void add_manifest_command(CLI::App& root, const GlobalOptions& globals) {
    auto* command = root.add_subcommand(
        "manifest", "summarise a manifest: files, levels, tiers, what a reopen would load");

    auto options = std::make_shared<CatalogOptions>();
    auto encryption = std::make_shared<EncryptionOptions>();
    auto generation = std::make_shared<uint64_t>(0);

    add_catalog_flags(*command, *options);
    add_encryption_flags(*command, *encryption);
    auto* generation_flag =
        command->add_option("--generation", *generation,
                            "read this generation instead of the one the pointer names");

    command->callback([options, encryption, generation, generation_flag, &globals]() {
        std::shared_ptr<ManifestCatalog> catalog = open_catalog(*options);
        const ProviderRegistry registry = open_registry(*encryption);

        const uint64_t chosen =
            generation_flag->count() ? *generation : current_generation(*catalog);

        size_t edits = 0;
        std::shared_ptr<const Version> version = load_version(*catalog, registry, chosen, edits);
        if (globals.json) {
            print_json(*version, chosen, edits);
        } else {
            print_text(*version, chosen, edits);
        }
    });
}

}  // namespace elysiumkv::cli
