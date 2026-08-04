#include "elysiumkv/s3_manifest_catalog.hpp"
#include "probe_support.hpp"
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <optional>
#include <string>
#include <thread>
using namespace elysiumkv;
static int failures = 0;
static void check(bool ok, const char* what) {
    std::printf("  %-56s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++failures;
}
int main() {
    const char* endpoint = std::getenv("ELYSIUMKV_S3_ENDPOINT");
    if (endpoint == nullptr) { std::printf("skipped\n"); return 77; }
    // Write-once is the point, so a fixed prefix would make this pass only on a
    // pristine store and fail on every rerun. Each run gets its own namespace.
    const std::string run = "catalog-" + std::to_string(::getpid());
    S3Options o;
    o.bucket = "elysiumkv-probe"; o.prefix = run;
    o.endpoint = endpoint; o.access_key = "test"; o.secret_key = "test";

    auto opened = S3ManifestCatalog::open(o);
    if (!opened) { std::printf("open failed\n"); return 1; }
    auto& c = **opened;
    elysiumkv_probe::ensure_bucket(endpoint, o.bucket);

    auto empty = c.read();
    check(empty.has_value() && !empty->has_value(), "no pointer yet reads as empty, not an error");

    auto first = c.compare_and_set(std::nullopt, 1);
    check(first.has_value() && first->has_value() && (*first)->generation == 1,
          "first install takes the empty expectation");
    const auto token1 = (*first)->token;
    check(!token1.empty(), "the token (ETag) is non-empty");

    // A second "first install" must lose: another writer got there.
    auto dup = c.compare_and_set(std::nullopt, 1);
    check(dup.has_value() && !dup->has_value(), "a second empty-expectation install loses the CAS");

    auto stale = c.compare_and_set(ManifestCatalog::Entry{1, "\"00000000000000000000000000000000\""}, 2);
    check(stale.has_value() && !stale->has_value(), "a stale token loses the CAS (412, not an error)");

    auto again = c.read();
    check(again.has_value() && again->has_value() && (*again)->generation == 1,
          "a lost CAS left the pointer untouched");

    auto second = c.compare_and_set(ManifestCatalog::Entry{1, token1}, 2);
    check(second.has_value() && second->has_value() && (*second)->generation == 2,
          "the current token wins the CAS");
    check((*second)->token != token1, "the token changes on install");

    // Generation objects: write-once, and a snapshot large enough to matter.
    const std::string big(900 * 1024, 'z');
    check(c.put_snapshot(2, Slice::from(big)).get() == Status::Ok, "put_snapshot (900 KiB)");
    auto snap = c.get_snapshot(2).get();
    check(snap.has_value() && snap->size() == big.size(), "snapshot round-trips through compression");
    check(snap.has_value() && std::string(snap->begin(), snap->end()) == big, "snapshot bytes identical");
    check(c.put_snapshot(2, Slice::from(std::string("other"))).get() == Status::Config,
          "put_snapshot never overwrites");

    check(c.put_edit(2, 1, Slice::from(std::string("e1"))).get() == Status::Ok, "put_edit");
    check(c.put_edit(2, 2, Slice::from(std::string("e2"))).get() == Status::Ok, "put_edit 2");
    check(c.put_edit(2, 2, Slice::from(std::string("x"))).get() == Status::Config,
          "put_edit never overwrites");
    auto edits = c.list_edits(2).get();
    check(edits.has_value() && edits->size() == 2 && (*edits)[0] == 1 && (*edits)[1] == 2,
          "list_edits is sorted and complete");
    auto missing = c.get_edit(2, 99).get();
    check(!missing.has_value() && missing.error() == Status::NotFound,
          "a missing edit is NotFound, not Io");

    // delete_generation must take everything of gen 2 and nothing of gen 1.
    check(c.put_snapshot(1, Slice::from(std::string("gen-one"))).get() == Status::Ok, "seed gen 1");
    check(c.delete_generation(2).get() == Status::Ok, "delete_generation");
    check(c.list_edits(2).get().value().empty(), "gen 2 edits are gone");
    check(!c.get_snapshot(2).get().has_value(), "gen 2 snapshot is gone");
    auto survivor = c.get_snapshot(1).get();
    check(survivor.has_value() && std::string(survivor->begin(), survivor->end()) == "gen-one",
          "gen 1 untouched by deleting gen 2");

    // ARCHITECTURE.md "Contract suites" — the headline case, and the reason the token exists: two *separate*
    // instances racing `compare_and_set` from the same expectation. Exactly one
    // may win. This cannot be tested against a single-writer filesystem — the
    // local catalog validates its token but can never actually contend — so S3 is
    // the first place the fence is exercised for real.
    {
        S3Options racer = o;
        racer.prefix = run + "-race";
        auto a = S3ManifestCatalog::open(racer);
        auto b = S3ManifestCatalog::open(racer);
        check(a.has_value() && b.has_value(), "two independent catalog instances");

        auto installed = (*a)->compare_and_set(std::nullopt, 1);
        check(installed.has_value() && installed->has_value(), "seed the race with generation 1");
        const auto shared_expectation = **installed;

        constexpr int kRounds = 12;
        int winners = 0, losers = 0, errors = 0;
        for (int round = 0; round < kRounds; ++round) {
            // Both start from the same expectation, so exactly one may install.
            std::optional<ManifestCatalog::Entry> from = shared_expectation;
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
            // Put the pointer back to the shared expectation for the next round.
            auto now = (*a)->read();
            if (now.has_value() && now->has_value() && (*now)->generation != 1) {
                (void)(*a)->compare_and_set(**now, 1);
            }
        }
        std::printf("  race over %d rounds: %d won, %d fenced, %d errored\n",
                    kRounds, winners, losers, errors);
        check(errors == 0, "a contended CAS is never an error, only won or lost");
        check(winners == kRounds && losers == kRounds,
              "exactly one writer wins each round — the other is fenced");
    }

    std::printf("%s\n", failures ? "FAILURES" : "all probes passed");
    return failures;
}
