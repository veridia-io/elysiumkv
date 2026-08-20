#include "elysiumkv/dynamo_manifest_catalog.hpp"

#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/DeleteItemRequest.h>
#include <aws/dynamodb/model/QueryRequest.h>

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <optional>
#include <string>
#include <thread>
#include <vector>
using namespace elysiumkv;
static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("  %-56s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++failures;
}

// A raw client, so the chunk layout can be asserted and damaged directly. The
// catalog itself holds the SDK initialised for as long as it is alive, so this
// needs no guard of its own.
static Aws::DynamoDB::DynamoDBClient raw_client(const char* endpoint) {
    Aws::Client::ClientConfiguration config;
    config.region = "us-east-1";
    config.endpointOverride = endpoint;
    config.scheme = Aws::Http::Scheme::HTTP;
    return Aws::DynamoDB::DynamoDBClient(Aws::Auth::AWSCredentials("test", "test"), config);
}

static std::vector<std::string> keys_with_prefix(const char* endpoint, const std::string& table,
                                                 const std::string& store,
                                                 const std::string& prefix) {
    auto client = raw_client(endpoint);
    Aws::DynamoDB::Model::QueryRequest request;
    request.SetTableName(table);
    request.SetKeyConditionExpression("PK = :pk AND begins_with(SK, :sk)");
    request.AddExpressionAttributeValues(":pk", Aws::DynamoDB::Model::AttributeValue(store));
    request.AddExpressionAttributeValues(":sk", Aws::DynamoDB::Model::AttributeValue(prefix));
    request.SetProjectionExpression("SK");

    std::vector<std::string> keys;
    Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> start;
    do {
        if (!start.empty()) request.SetExclusiveStartKey(start);
        auto outcome = client.Query(request);
        if (!outcome.IsSuccess()) return keys;
        for (const auto& item : outcome.GetResult().GetItems()) keys.push_back(item.at("SK").GetS());
        start = outcome.GetResult().GetLastEvaluatedKey();
    } while (!start.empty());
    std::sort(keys.begin(), keys.end());
    return keys;
}

static std::vector<std::string> snapshot_chunk_keys(const char* endpoint, const std::string& table,
                                                    const std::string& store, uint64_t generation) {
    char prefix[64];
    std::snprintf(prefix, sizeof(prefix), "gen#%012llu#snap#",
                  static_cast<unsigned long long>(generation));
    return keys_with_prefix(endpoint, table, store, prefix);
}

static size_t count_edit_chunks(const char* endpoint, const std::string& table,
                                const std::string& store, uint64_t generation, uint64_t seq) {
    char prefix[64];
    std::snprintf(prefix, sizeof(prefix), "gen#%012llu#edit#%012llu#",
                  static_cast<unsigned long long>(generation),
                  static_cast<unsigned long long>(seq));
    return keys_with_prefix(endpoint, table, store, prefix).size();
}

static size_t count_snapshot_chunks(const char* endpoint, const std::string& table,
                                   const std::string& store, uint64_t generation) {
    return snapshot_chunk_keys(endpoint, table, store, generation).size();
}

static bool delete_item(const char* endpoint, const std::string& table, const std::string& store,
                        const std::string& sort_key) {
    auto client = raw_client(endpoint);
    Aws::DynamoDB::Model::DeleteItemRequest request;
    request.SetTableName(table);
    request.AddKey("PK", Aws::DynamoDB::Model::AttributeValue(store));
    request.AddKey("SK", Aws::DynamoDB::Model::AttributeValue(sort_key));
    return client.DeleteItem(request).IsSuccess();
}
int main() {
    const char* endpoint = std::getenv("ELYSIUMKV_DYNAMO_ENDPOINT");
    if (endpoint == nullptr) endpoint = std::getenv("ELYSIUMKV_S3_ENDPOINT");
    if (endpoint == nullptr) { std::printf("skipped\n"); return 77; }
    // Write-once means a fixed store_id passes only on a pristine table. Each run
    // takes its own partition.
    const std::string run = "store-" + std::to_string(::getpid());
    DynamoOptions o;
    o.table = "elysiumkv-manifest"; o.store_id = run;
    o.endpoint = endpoint; o.access_key = "test"; o.secret_key = "test";
    o.create_table_if_missing = true;

    auto opened = DynamoManifestCatalog::open(o);
    if (!opened) { std::printf("open failed\n"); return 1; }
    auto& c = **opened;

    auto empty = c.read();
    check(empty.has_value() && !empty->has_value(), "no pointer yet reads as empty, not an error");

    auto first = c.compare_and_set(std::nullopt, 1);
    check(first.has_value() && first->has_value() && (*first)->generation == 1,
          "first install takes the empty expectation");
    const auto token1 = (*first)->token;
    check(!token1.empty(), "the token (version) is non-empty");

    auto dup = c.compare_and_set(std::nullopt, 1);
    check(dup.has_value() && !dup->has_value(), "a second empty-expectation install loses the CAS");

    auto stale = c.compare_and_set(ManifestCatalog::Entry{1, "999999"}, 2);
    check(stale.has_value() && !stale->has_value(),
          "a stale token loses the CAS (condition failed, not an error)");

    auto again = c.read();
    check(again.has_value() && again->has_value() && (*again)->generation == 1,
          "a lost CAS left the pointer untouched");

    auto second = c.compare_and_set(ManifestCatalog::Entry{1, token1}, 2);
    check(second.has_value() && second->has_value() && (*second)->generation == 2,
          "the current token wins the CAS");
    check((*second)->token != token1, "the token changes on install");

    // The chunking path, and the payload has to be incompressible to reach it: anything zstd can
    // fold below the 400 KB cap lands in a single item while the test still reads as "chunked".
    // splitmix64 output does not compress, so 1 MiB stays 1 MiB.
    std::string big(1024 * 1024, '\0');
    {
        uint64_t state = 1;
        for (size_t i = 0; i < big.size(); ++i) {
            state += 0x9E3779B97F4A7C15ull;
            uint64_t z = state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
            big[i] = static_cast<char>(z ^ (z >> 31));
        }
    }
    check(c.put_snapshot(2, Slice::from(big)).get() == Status::Ok, "put_snapshot (1 MiB, chunked)");
    // Asserted structurally rather than inferred from the round trip, which passes just as happily
    // with one chunk.
    check(count_snapshot_chunks(endpoint, o.table, run, 2) > 1,
          "the snapshot really did land in more than one item");
    auto snap = c.get_snapshot(2).get();
    check(snap.has_value() && snap->size() == big.size(), "chunked snapshot round-trips");
    check(snap.has_value() && std::string(snap->begin(), snap->end()) == big,
          "reassembled bytes identical");

    // ARCHITECTURE.md "Negative controls" — the negative control for `total_chunks`. Losing a chunk is what
    // a half-written generation looks like, and the whole point of recording the
    // count is that the loss is loud. Take one out and the read must refuse:
    // without this check the same call would return a truncated manifest that
    // decompresses to a plausible-looking prefix. Restored afterwards so the rest
    // of the run still has its snapshot.
    {
        const auto keys = snapshot_chunk_keys(endpoint, o.table, run, 2);
        check(keys.size() > 1 && delete_item(endpoint, o.table, run, keys.back()),
              "remove one chunk of the snapshot");
        auto damaged = c.get_snapshot(2).get();
        check(!damaged.has_value() && damaged.error() == Status::Corrupt,
              "a short chunk set is Corrupt, not a silently truncated manifest");

        // And losing the *first* chunk must be caught too. `total_chunks` is
        // written on every chunk rather than only the first precisely so the count
        // survives the loss of whichever item the reader happens to look at; that
        // is load-bearing and otherwise untested.
        check(delete_item(endpoint, o.table, run, keys.front()), "remove the first chunk as well");
        auto headless = c.get_snapshot(2).get();
        check(!headless.has_value() && headless.error() == Status::Corrupt,
              "losing the first chunk is Corrupt too — the count is on every chunk");

        for (const auto& key : keys) (void)delete_item(endpoint, o.table, run, key);
        check(c.put_snapshot(2, Slice::from(big)).get() == Status::Ok, "rewrite the snapshot");
    }

    // The attempt in a snapshot address makes a retry possible; it does not make a snapshot
    // rewritable. Write-once still holds against a *finished* one, which is what the contract
    // suite pins across all three catalogs. Only residue is retryable — the case below.
    {
        check(c.put_snapshot(2, Slice::from(std::string("other"))).get() == Status::Config,
              "a complete snapshot still refuses a second put — write-once is unchanged");
    }

    // The bug this layout exists to remove. A chunked `put_snapshot` writes items one at a
    // time, so a failure partway leaves some behind — and with a fixed address the retry collided
    // with its own residue, `attribute_not_exists` refused it, and the engine read that refusal as
    // another writer having won the roll. One transient error fenced the store permanently, and
    // the residue survived the reopen, so it stayed fenced.
    {
        const auto keys = snapshot_chunk_keys(endpoint, o.table, run, 2);
        check(keys.size() > 1, "a multi-chunk snapshot to leave a partial copy of");
        for (size_t i = 1; i < keys.size(); ++i) (void)delete_item(endpoint, o.table, run, keys[i]);

        check(c.put_snapshot(2, Slice::from(big)).get() == Status::Ok,
              "a retry over a partial snapshot succeeds rather than fencing the store");
        auto after = c.get_snapshot(2).get();
        check(after.has_value() && std::string(after->begin(), after->end()) == big,
              "and reads back the attempt that completed, not the residue");

        // Read by an instance that wrote none of it. Without the pointer naming the attempt this
        // is the case that would have to guess between the residue and the good copy.
        DynamoOptions reader_options = o;
        reader_options.create_table_if_missing = false;
        auto reader = DynamoManifestCatalog::open(reader_options);
        check(reader.has_value(), "a second instance opens");
        if (reader.has_value()) {
            (void)(*reader)->read();
            auto seen = (*reader)->get_snapshot(2).get();
            check(seen.has_value() && std::string(seen->begin(), seen->end()) == big,
                  "a fresh instance reads the installed attempt");
        }
    }

    // The edit address stays write-once, because a collision there is what detects a second
    // writer. What changed is that residue *this* instance left is cleared and retried, rather
    // than being mistaken for a rival.
    {
        check(c.put_edit(9, 1, Slice::from(big)).get() == Status::Ok, "an edit to leave partial");
        const auto keys =
                keys_with_prefix(endpoint, o.table, run, "gen#000000000009#edit#000000000001#");
        check(keys.size() > 1, "the edit is chunked");
        for (size_t i = 1; i < keys.size(); ++i) (void)delete_item(endpoint, o.table, run, keys[i]);

        check(c.put_edit(9, 1, Slice::from(big)).get() == Status::Ok,
              "a retry over our own partial edit clears it and rewrites");
        auto back = c.get_edit(9, 1).get();
        check(back.has_value() && std::string(back->begin(), back->end()) == big,
              "and the edit reads back whole");
    }
    {
    }

    // A small snapshot goes down the same path — one chunk, not a special case.
    check(c.put_snapshot(3, Slice::from(std::string("tiny"))).get() == Status::Ok,
          "put_snapshot (small, one chunk)");
    auto tiny = c.get_snapshot(3).get();
    check(tiny.has_value() && std::string(tiny->begin(), tiny->end()) == "tiny",
          "single-chunk snapshot round-trips");
    check(!c.get_snapshot(77).get().has_value() &&
              c.get_snapshot(77).get().error() == Status::NotFound,
          "a missing snapshot is NotFound, not Io");

    check(c.put_edit(2, 1, Slice::from(std::string("e1"))).get() == Status::Ok, "put_edit");
    check(c.put_edit(2, 2, Slice::from(std::string("e2"))).get() == Status::Ok, "put_edit 2");
    check(c.put_edit(2, 2, Slice::from(std::string("x"))).get() == Status::Config,
          "put_edit never overwrites");
    auto edit = c.get_edit(2, 1).get();
    check(edit.has_value() && std::string(edit->begin(), edit->end()) == "e1", "get_edit");
    auto edits = c.list_edits(2).get();
    check(edits.has_value() && edits->size() == 2 && (*edits)[0] == 1 && (*edits)[1] == 2,
          "list_edits is sorted and complete");
    // The edit chunking path, structurally. A compaction edit carries a full record per output
    // file, so it can exceed the 400 KB cap. Same incompressible payload as the snapshot above, and
    // for the same reason.
    check(c.put_edit(2, 3, Slice::from(big)).get() == Status::Ok, "put_edit (1 MiB, chunked)");
    check(count_edit_chunks(endpoint, o.table, run, 2, 3) > 1,
          "the edit really did land in more than one item");
    auto big_edit = c.get_edit(2, 3).get();
    check(big_edit.has_value() && big_edit->size() == big.size(), "chunked edit round-trips");
    check(big_edit.has_value() && std::string(big_edit->begin(), big_edit->end()) == big,
          "reassembled edit bytes identical");
    // Several items, one sequence number: replay asks for each sequence once.
    auto after_big = c.list_edits(2).get();
    check(after_big.has_value() &&
              *after_big == std::vector<uint64_t>({1, 2, 3}),
          "a chunked edit is listed once, not once per chunk");

    auto missing = c.get_edit(2, 99).get();
    check(!missing.has_value() && missing.error() == Status::NotFound,
          "a missing edit is NotFound, not Io");

    // delete_generation must take everything of gen 2 and nothing of gen 3, and
    // must never take the pointer — whose sort key does not carry the prefix.
    check(c.delete_generation(2).get() == Status::Ok, "delete_generation");
    check(c.list_edits(2).get().value().empty(), "gen 2 edits are gone");
    check(!c.get_snapshot(2).get().has_value(), "gen 2 snapshot is gone");
    auto survivor = c.get_snapshot(3).get();
    check(survivor.has_value() && std::string(survivor->begin(), survivor->end()) == "tiny",
          "gen 3 untouched by deleting gen 2");
    auto pointer = c.read();
    check(pointer.has_value() && pointer->has_value() && (*pointer)->generation == 2,
          "the pointer survived deleting its generation's objects");

    // ARCHITECTURE.md "Contract suites" — the headline case: two *separate* instances racing from the same
    // expectation. Exactly one may win. Unlike the S3 catalog this re-reads the
    // expectation each round — the token here is a counter, so it moves on every
    // install and a fixed expectation would fence both racers from round two on.
    {
        DynamoOptions racer = o;
        racer.store_id = run + "-race";
        racer.create_table_if_missing = false;  // the table exists by now
        auto a = DynamoManifestCatalog::open(racer);
        auto b = DynamoManifestCatalog::open(racer);
        check(a.has_value() && b.has_value(), "two independent catalog instances");

        auto installed = (*a)->compare_and_set(std::nullopt, 1);
        check(installed.has_value() && installed->has_value(), "seed the race with generation 1");

        constexpr int kRounds = 12;
        int winners = 0, losers = 0, errors = 0;
        for (int round = 0; round < kRounds; ++round) {
            auto now = (*a)->read();
            if (!now.has_value() || !now->has_value()) { ++errors; continue; }
            std::optional<ManifestCatalog::Entry> from = **now;

            std::optional<ManifestCatalog::Entry> results[2];
            bool failed[2] = {false, false};
            std::thread racer_a([&] {
                auto outcome = (*a)->compare_and_set(from, 2);
                if (outcome.has_value()) results[0] = *outcome; else failed[0] = true;
            });
            std::thread racer_b([&] {
                auto outcome = (*b)->compare_and_set(from, 3);
                if (outcome.has_value()) results[1] = *outcome; else failed[1] = true;
            });
            racer_a.join();
            racer_b.join();

            for (int i = 0; i < 2; ++i) {
                if (failed[i]) ++errors;
                else if (results[i].has_value()) ++winners;
                else ++losers;
            }
        }
        std::printf("  race over %d rounds: %d won, %d fenced, %d errored\n",
                    kRounds, winners, losers, errors);
        check(errors == 0, "a contended CAS is never an error, only won or lost");
        check(winners == kRounds && losers == kRounds,
              "exactly one writer wins each round — the other is fenced");
    }

    // A store_id is a partition, so a second store on the same table must not see
    // the first one's pointer. This is what makes one table serve many databases.
    {
        DynamoOptions other = o;
        other.store_id = run + "-other";
        other.create_table_if_missing = false;
        auto isolated = DynamoManifestCatalog::open(other);
        check(isolated.has_value(), "a second store on the same table opens");
        auto its_pointer = (*isolated)->read();
        check(its_pointer.has_value() && !its_pointer->has_value(),
              "a different store_id sees no pointer — partitions are isolated");
    }

    std::printf("%s\n", failures ? "FAILURES" : "all probes passed");
    return failures;
}
