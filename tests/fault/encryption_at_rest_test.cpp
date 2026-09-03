/* Encryption at rest, through the engine rather than through the cipher.
 *
 * The invariant everything else is in service of is the first test here: no byte a store holds
 * is plaintext. Every other property — that reads work, that compaction works, that migration stays
 * a byte-for-byte copy — is a way of showing the engine still functions with the boundary in place.
 * A suite that only checked those would pass against a build that encrypted nothing.
 */

#include "db/db_impl.hpp"

#include "support/test_db.hpp"
#include "support/test_encryption.hpp"
#include "elysiumkv/db.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace elysiumkv::test {
namespace {

std::string key_at(int i) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "key:%08d", i);
    return buf;
}

/// The value written for `i`, deliberately long and distinctive so that finding it in a stored
/// object is unambiguous.
std::string value_at(int i) {
    return "PLAINTEXT-CANARY-" + std::to_string(i) + "-" + std::string(64, 'v');
}

/// Every byte of every object on `store`, concatenated. Read below the boundary, so what this sees
/// is what an attacker with the bucket would see.
std::string all_stored_bytes(BlobStore& store) {
    std::string all;
    auto names = store.list("").get();
    EXPECT_TRUE(names.has_value());
    if (!names) return all;
    for (const std::string& name : *names) {
        auto bytes = store.get(name, 0, BlobStore::kReadToEnd).get();
        EXPECT_TRUE(bytes.has_value()) << name;
        if (bytes) all.append(reinterpret_cast<const char*>(bytes->data()), bytes->size());
    }
    return all;
}

/// Every byte of every file under `root`, which for these fixtures is both the blob stores *and*
/// the manifest catalog. Broader than `all_stored_bytes` on purpose: the manifest is where the key
/// bounds live, so a scan that skipped it would miss the user data most easily overlooked.
std::string all_bytes_under(const std::filesystem::path& root) {
    std::string all;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        std::ifstream in(entry.path(), std::ios::binary);
        all.append((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    }
    return all;
}

class EncryptionAtRest : public ::testing::Test {
protected:
    Options encrypted_options(int seed = 1) {
        Options options = make_options(store_, Compression::None, 64u << 10);
        options.background = BackgroundMode::Inline;
        options.encryption.providers["kms-gcm"] = make_test_provider(seed);
        options.encryption.primary_provider = "kms-gcm";
        return options;
    }

    TestStore store_{1};
};

/// I1. The one that matters.
TEST_F(EncryptionAtRest, NoStoredByteIsPlaintext) {
    Options options = encrypted_options();
    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);

    for (int i = 0; i < 400; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);

    const std::string stored = all_stored_bytes(*store_.store(0));
    ASSERT_FALSE(stored.empty()) << "nothing was written, so nothing was proved";

    for (int i = 0; i < 400; ++i) {
        EXPECT_EQ(stored.find(value_at(i)), std::string::npos) << "value " << i << " is readable";
        EXPECT_EQ(stored.find(key_at(i)), std::string::npos) << "key " << i << " is readable";
    }
    // The canary substring alone, in case a value were stored split across a boundary.
    EXPECT_EQ(stored.find("PLAINTEXT-CANARY"), std::string::npos);

    // And the manifest, which is where a key most easily escapes. Every file record carries its
    // file's smallest and largest key, so a manifest in the clear leaks user keys however well the
    // SSTs are sealed. This is the assertion Phase 2 exists for.
    const std::string everything = all_bytes_under(store_.path());
    ASSERT_GT(everything.size(), stored.size()) << "the manifest was not included in the scan";
    for (int i = 0; i < 400; ++i) {
        EXPECT_EQ(everything.find(value_at(i)), std::string::npos)
            << "value " << i << " is readable somewhere under the store root";
        EXPECT_EQ(everything.find(key_at(i)), std::string::npos)
            << "key " << i << " is readable somewhere under the store root";
    }
    EXPECT_EQ(everything.find("PLAINTEXT-CANARY"), std::string::npos);
}

TEST_F(EncryptionAtRest, EveryKeyReadsBackThroughGetAndIteration) {
    Options options = encrypted_options();
    auto db = std::move(*DB::open(options));

    for (int i = 0; i < 400; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);

    for (int i = 0; i < 400; ++i) {
        auto found = db->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i << " -> " << status_name(found.error());
        EXPECT_EQ(std::string(found->begin(), found->end()), value_at(i)) << i;
    }

    int seen = 0;
    auto it = db->iterator();
    while (it->next()) ++seen;
    EXPECT_EQ(it->status(), Status::Ok);
    EXPECT_EQ(seen, 400);

}

/// Reopening is where the recorded provider and metadata are actually exercised: the readers built
/// during the first run are gone, so every file is opened from what the manifest says about it.
TEST_F(EncryptionAtRest, TheStoreReopensAndStillReads) {
    Options options = encrypted_options();
    {
        auto db = std::move(*DB::open(options));
        for (int i = 0; i < 200; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    auto reopened = DB::open(encrypted_options());
    ASSERT_TRUE(reopened.has_value()) << status_name(reopened.error());
    for (int i = 0; i < 200; ++i) {
        auto found = (*reopened)->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i;
        EXPECT_EQ(std::string(found->begin(), found->end()), value_at(i)) << i;
    }
}

/// Migration mode leaves old files plaintext and writes new ones encrypted. Both must read until
/// compaction has converged the store and the compatibility option can be removed.
TEST_F(EncryptionAtRest, PlaintextAndEncryptedFilesCoexist) {
    {
        Options plain = make_options(store_, Compression::None, 64u << 10);
        plain.background = BackgroundMode::Inline;
        auto db = std::move(*DB::open(plain));
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    Options migrating = encrypted_options();
    migrating.encryption.accept_plaintext = true;
    auto db = std::move(*DB::open(migrating));
    for (int i = 100; i < 200; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);

    for (int i = 0; i < 200; ++i) {
        auto found = db->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i;
        EXPECT_EQ(std::string(found->begin(), found->end()), value_at(i)) << i;
    }

    // The old file is still plaintext — which is the honest state, and why enabling encryption on a
    // populated store is not the same as the store being encrypted.
    const std::string stored = all_stored_bytes(*store_.store(0));
    EXPECT_NE(stored.find(value_at(0)), std::string::npos)
        << "the pre-existing file was written before encryption and is not rewritten by enabling it";
    EXPECT_EQ(stored.find(value_at(150)), std::string::npos) << "but new files are encrypted";

    // And compaction is what converges it: the output is written by the primary provider.
    ASSERT_EQ(db->compact_level(0), Status::Ok);
    const std::string after = all_stored_bytes(*store_.store(0));
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(after.find(value_at(i)), std::string::npos)
            << "value " << i << " survived compaction as plaintext";
    }
}

/// A file this configuration cannot read is a configuration failure, not damage. The operator's
/// remedy is to register the provider, and `Corrupt` would send them to a restore instead.
///
/// The manifest has to stay readable for this to be about the file at all. So the store is
/// reopened with a *different* provider primary and the file's own absent — which is the shape a
/// half-finished rotation actually has. Dropping the provider entirely is the case below.
TEST_F(EncryptionAtRest, AFileWhoseProviderIsNotRegisteredIsAConfigurationError) {
    {
        auto db = std::move(*DB::open(encrypted_options()));
        for (int i = 0; i < 50; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    // Move the manifest onto the new provider while the old file stays where it is: one edit per
    // generation, so the write below rolls and the fresh snapshot is written under `kms-gcm-2`.
    {
        Options rotating = make_options(store_, Compression::None, 64u << 10);
        rotating.background = BackgroundMode::Inline;
        rotating.manifest_edits_per_generation = 1;
        rotating.encryption.providers["kms-gcm"] = make_test_provider(1);
        rotating.encryption.providers["kms-gcm-2"] = make_test_provider(2);
        rotating.encryption.primary_provider = "kms-gcm-2";
        auto db = std::move(*DB::open(rotating));
        ASSERT_EQ(db->put(Slice::from(key_at(900)), Slice::from(value_at(900))), Status::Ok);
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    Options rotated = make_options(store_, Compression::None, 64u << 10);
    rotated.background = BackgroundMode::Inline;
    rotated.manifest_edits_per_generation = 1;
    rotated.encryption.providers["kms-gcm-2"] = make_test_provider(2);
    rotated.encryption.primary_provider = "kms-gcm-2";
    auto reopened = DB::open(rotated);
    ASSERT_TRUE(reopened.has_value()) << "the manifest is readable; the files are read on demand";
    EXPECT_EQ((*reopened)->get(Slice::from(key_at(0))).error(), Status::Config);
    // And the file written under the surviving provider still reads, so this is about the one
    // missing provider and not about the store being broken.
    EXPECT_TRUE((*reopened)->get(Slice::from(key_at(900))).has_value());
}

/// The manifest is encrypted too, so the same mistake is caught a step earlier. Dropping the
/// provider a store was written with now fails at `open` rather than at whichever read happened to
/// touch an encrypted file first — and it says which provider is missing.
TEST_F(EncryptionAtRest, AManifestWhoseProviderIsNotRegisteredRefusesToOpen) {
    {
        auto db = std::move(*DB::open(encrypted_options()));
        for (int i = 0; i < 50; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }

    Options without = make_options(store_, Compression::None, 64u << 10);
    without.background = BackgroundMode::Inline;
    auto reopened = DB::open(without);
    ASSERT_FALSE(reopened.has_value());
    EXPECT_EQ(reopened.error(), Status::Config)
        << "not Corrupt: the bytes are intact and the remedy is to register the provider";
    // The status alone cannot say *which* provider, and that is the whole remedy.
    EXPECT_NE(std::string(last_error()).find("kms-gcm"), std::string::npos)
        << "the failure has to name the provider to register: " << last_error();
}

/// A rotation converges only because something drives it. Changing `primary_provider` governs
/// what is written next; the files already on disk keep the provider they were written under, and
/// for a cold file compaction may never touch them. That is precisely the file a key rotation was
/// performed to stop depending on.
TEST_F(EncryptionAtRest, RewritingToThePrimaryFinishesARotation) {
    // Written under "kms-gcm", then compacted down so the data is below L0 — which is where a
    // rotation actually has to reach, and where this pass operates.
    {
        auto db = std::move(*DB::open(encrypted_options()));
        for (int i = 0; i < 400; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_EQ(db->compact_level(0), Status::Ok);
    }

    Options rotated = make_options(store_, Compression::None, 64u << 10);
    rotated.background = BackgroundMode::Inline;
    rotated.encryption.providers["kms-gcm"] = make_test_provider(1);
    rotated.encryption.providers["kms-gcm-2"] = make_test_provider(2);
    rotated.encryption.primary_provider = "kms-gcm-2";

    // Off first, and this half is the point. Without the switch a rotation stalls silently:
    // the store reads fine, so nothing complains, and the old key stays load-bearing for ever.
    {
        auto db = std::move(*DB::open(rotated));
        ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);
        const Stats stats = db->stats();
        EXPECT_GT(stats.files_pending_reencryption, 0u)
            << "nothing drives the rotation, so it must not have moved";
        EXPECT_EQ(stats.reencryptions, 0u);
    }

    rotated.encryption.rewrite_to_primary = true;
    auto opened = DB::open(rotated);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);
    ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);

    const Stats stats = db->stats();
    EXPECT_EQ(stats.files_pending_reencryption, 0u) << "the rotation has to converge";
    EXPECT_GT(stats.reencryptions, 0u) << "and it has to have done work to get there";

    // Every value still reads, which is the only thing that makes the rewrite worth having.
    for (int i = 0; i < 400; ++i) {
        auto found = db->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i << " -> " << status_name(found.error());
        EXPECT_EQ(std::string(found->begin(), found->end()), value_at(i)) << i;
    }
    EXPECT_EQ(static_cast<DbImpl&>(*db).check_invariants(), Status::Ok);
}

/// The old provider is only safe to unregister once the gauge reads zero — so a store that has
/// converged must open without it, and that is what makes the gauge actionable rather than
/// decorative.
TEST_F(EncryptionAtRest, AConvergedRotationOpensWithoutTheOldProvider) {
    {
        auto db = std::move(*DB::open(encrypted_options()));
        for (int i = 0; i < 400; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
        ASSERT_EQ(db->compact_level(0), Status::Ok);
    }
    {
        Options rotating = make_options(store_, Compression::None, 64u << 10);
        rotating.background = BackgroundMode::Inline;
        rotating.encryption.providers["kms-gcm"] = make_test_provider(1);
        rotating.encryption.providers["kms-gcm-2"] = make_test_provider(2);
        rotating.encryption.primary_provider = "kms-gcm-2";
        rotating.encryption.rewrite_to_primary = true;
        auto db = std::move(*DB::open(rotating));
        ASSERT_EQ(static_cast<DbImpl&>(*db).compact_until_quiet(), Status::Ok);
        ASSERT_EQ(db->stats().files_pending_reencryption, 0u);
    }

    Options alone = make_options(store_, Compression::None, 64u << 10);
    alone.background = BackgroundMode::Inline;
    alone.encryption.providers["kms-gcm-2"] = make_test_provider(2);
    alone.encryption.primary_provider = "kms-gcm-2";
    auto reopened = DB::open(alone);
    ASSERT_TRUE(reopened.has_value())
        << "the retired provider is gone and nothing should still need it: "
        << status_name(reopened.error()) << " — " << last_error();
    for (int i = 0; i < 400; ++i) {
        auto found = (*reopened)->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i;
    }
}

/// I12. A configuration the engine cannot honour is refused at open, not at first write — and
/// each refusal says which one it was, because `Config` alone does not narrow it down at all.
TEST_F(EncryptionAtRest, AnUnusableConfigurationIsRefusedAtOpen) {
    Options unknown_primary = make_options(store_, Compression::None, 64u << 10);
    unknown_primary.encryption.primary_provider = "never-registered";
    EXPECT_EQ(DB::open(unknown_primary).error(), Status::Config);
    EXPECT_NE(std::string(last_error()).find("never-registered"), std::string::npos)
        << last_error();

    Options reserved_id = make_options(store_, Compression::None, 64u << 10);
    reserved_id.encryption.providers[""] = make_test_provider();
    EXPECT_EQ(DB::open(reserved_id).error(), Status::Config)
        << "the empty id belongs to the passthrough";
    EXPECT_NE(std::string(last_error()).find("reserved"), std::string::npos) << last_error();

    Options null_provider = make_options(store_, Compression::None, 64u << 10);
    null_provider.encryption.providers["broken"] = nullptr;
    null_provider.encryption.primary_provider = "broken";
    EXPECT_EQ(DB::open(null_provider).error(), Status::Config);
    EXPECT_NE(std::string(last_error()).find("broken"), std::string::npos) << last_error();
}

/// With no encryption configured the passthrough is primary, files record the reserved empty id,
/// and nothing about the store changes. The consistency the always-a-provider rule buys is only
/// worth having if it costs this case nothing.
TEST_F(EncryptionAtRest, WithoutAProviderFilesRecordTheReservedEmptyId) {
    Options plain = make_options(store_, Compression::None, 64u << 10);
    plain.background = BackgroundMode::Inline;
    auto opened = DbImpl::open(plain, /*require_all_durable=*/true);
    ASSERT_TRUE(opened.has_value());
    auto db = std::move(opened->db);

    ASSERT_EQ(db->put(Slice::from(key_at(0)), Slice::from(value_at(0))), Status::Ok);
    ASSERT_EQ(db->flush(), Status::Ok);

    const std::string stored = all_stored_bytes(*store_.store(0));
    EXPECT_NE(stored.find(value_at(0)), std::string::npos) << "plaintext, as configured";
}

/// I3. Migration moves an object between tiers without decoding it, and that has to survive
/// encryption or the cheapest operation in the engine becomes the most expensive.
///
/// It is also the case that nearly broke: a migrated copy is renumbered, so authenticating
/// chunks against the file's current number would leave every migrated file unreadable. The identity
/// is recorded when the object is first written and travels with it, which is what this pins.
TEST_F(EncryptionAtRest, MigrationCopiesCiphertextUnchangedAndItStillReads) {
    TestStore tiers{2};
    uint64_t now = 1'000'000;

    Options options = make_transient_options(tiers, Duration(60'000), Duration(120'000));
    options.background = BackgroundMode::Inline;
    options.clock = [&now] { return now; };
    options.encryption.providers["kms-gcm"] = make_test_provider();
    options.encryption.primary_provider = "kms-gcm";

    auto opened = DbImpl::open(options, /*require_all_durable=*/false);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(opened->db);
    auto& engine = *static_cast<DbImpl*>(db.get());

    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);
    ASSERT_GT(db->stats().tiers[0].file_count, 0) << "young data lives on the hot tier";

    // Exactly what the hot tier holds, before anything moves.
    const std::string before = all_stored_bytes(*tiers.store(0));
    ASSERT_FALSE(before.empty());

    now += 200'000;
    ASSERT_EQ(engine.compact_until_quiet(), Status::Ok);
    ASSERT_EQ(db->stats().tiers[0].file_count, 0) << "everything aged off the hot tier";
    ASSERT_GT(db->stats().tiers[1].file_count, 0);

    // Byte for byte. Not merely readable afterwards: the same ciphertext, which is what makes
    // migration a copy rather than a re-encryption.
    EXPECT_EQ(all_stored_bytes(*tiers.store(1)), before)
        << "migration re-encrypted instead of copying";

    for (int i = 0; i < 200; ++i) {
        auto found = db->get_copy(Slice::from(key_at(i)));
        ASSERT_TRUE(found.has_value()) << i << " -> " << status_name(found.error());
        EXPECT_EQ(std::string(found->begin(), found->end()), value_at(i)) << i;
    }
}

/// The constraint that turned out not to exist.
///
/// A cache rounds a miss out to its own granularity and then serves the exact window asked for out
/// of what it holds — so a cached range beginning mid-chunk is never handed to the boundary as if it
/// were a chunk. The boundary always asks for whole chunks in physical space and gets exactly those
/// bytes back, however the layer below chose to fetch them. This is asserted rather than guarded:
/// a granularity deliberately coprime with both the chunk size and the physical stride.
TEST_F(EncryptionAtRest, ACacheGranularityUnalignedWithTheChunkSizeIsHarmless) {
    Options options = encrypted_options();
    cache_every_tier(options, store_.path() / "cache", /*fetch_granularity=*/999);

    auto opened = DB::open(options);
    ASSERT_TRUE(opened.has_value()) << status_name(opened.error());
    auto db = std::move(*opened);

    for (int i = 0; i < 300; ++i) {
        ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
    }
    ASSERT_EQ(db->flush(), Status::Ok);
    ASSERT_EQ(db->compact_level(0), Status::Ok);

    // Twice, so the second pass is served by the caches rather than the store.
    for (int pass = 0; pass < 2; ++pass) {
        for (int i = 0; i < 300; ++i) {
            auto found = db->get_copy(Slice::from(key_at(i)));
            ASSERT_TRUE(found.has_value()) << "pass " << pass << " key " << i;
            EXPECT_EQ(std::string(found->begin(), found->end()), value_at(i));
        }
    }
}

/// Counts what it is asked to do, and otherwise forwards.
class CountingProvider final : public EncryptionProvider {
public:
    explicit CountingProvider(std::shared_ptr<EncryptionProvider> inner)
        : inner_(std::move(inner)) {}

    Result<NewObject> create(uint64_t object_id) override {
        ++creates;
        return inner_->create(object_id);
    }
    Result<std::shared_ptr<ObjectCipher>> open(uint64_t object_id, Slice metadata) override {
        ++opens;
        return inner_->open(object_id, metadata);
    }

    int creates = 0;
    int opens = 0;

private:
    std::shared_ptr<EncryptionProvider> inner_;
};

/// I9. Routing is by the id a file recorded, never by trying a provider and seeing whether it
/// worked.
///
/// Counted rather than inferred from success, because success does not distinguish the two: a
/// trial implementation would ask the wrong provider, fail, fall back, and return the right bytes.
/// What separates them is whether the wrong provider was asked at all.
TEST_F(EncryptionAtRest, OnlyTheProviderThatWroteAFileIsAsked) {
    auto first = std::make_shared<CountingProvider>(make_test_provider(1));
    auto second = std::make_shared<CountingProvider>(make_test_provider(2));

    Options options = make_options(store_, Compression::None, 64u << 10);
    options.background = BackgroundMode::Inline;
    options.encryption.providers["first"] = first;
    options.encryption.providers["second"] = second;
    options.encryption.primary_provider = "first";

    {
        auto db = std::move(*DB::open(options));
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(db->put(Slice::from(key_at(i)), Slice::from(value_at(i))), Status::Ok);
        }
        ASSERT_EQ(db->flush(), Status::Ok);
    }
    ASSERT_GT(first->creates, 0) << "the primary wrote the file";
    EXPECT_EQ(second->creates, 0) << "a provider that is not primary writes nothing";

    // Reopen with the *other* provider primary, so a trial implementation would reach for it first.
    Options reversed = options;
    reversed.encryption.primary_provider = "second";
    first->opens = 0;
    second->opens = 0;

    auto db = std::move(*DB::open(reversed));
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(db->get_copy(Slice::from(key_at(i))).has_value()) << i;
    }

    EXPECT_GT(first->opens, 0) << "the file records `first`, so `first` is what answers";
    EXPECT_EQ(second->opens, 0)
        << "the primary was never asked about a file it did not write — routing is by the recorded "
           "id, not by trying providers in turn";
}

}  // namespace
}  // namespace elysiumkv::test
