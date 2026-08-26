/* The CLI's manifest read, against a store a real `VersionSet` wrote.
 *
 * Nothing here fabricates a manifest, because the defect this exists for was exactly a
 * disagreement with the writer: every payload is framed whether or not the store is encrypted, and
 * the CLI handed the framed bytes straight to `decode_version_snapshot` and reported damage. It
 * read no store written after that framing landed, and with the code reachable only from `main`
 * nothing in the suite could have said so.
 */

#include "encryption.hpp"
#include "version_load.hpp"

#include "support/temp_dir.hpp"
#include "elysiumkv/aes256_gcm_encryption_provider.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"
#include "elysiumkv/static_encryption_key_manager.hpp"
#include "version/manifest_payload.hpp"
#include "version/version_set.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace elysiumkv::cli {
namespace {

using test::TempDir;

/// 32 bytes, hex, as `--encryption-key-hex` takes them.
constexpr const char* kMasterKeyHex =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

constexpr const char* kProviderId = "aes-gcm";

FileMetadata file(int level, uint64_t number) {
    return FileMetadata{.level = level,
                        .file_number = number,
                        .store_id = "store-a",
                        .smallest_key = "a",
                        .largest_key = "z",
                        .file_bytes = 1000,
                        .num_entries = 10,
                        .min_write_time_ms = 100};
}

/// The registry a writer holds, built without going through the CLI's flags: the claim under test
/// is that the two arrive at the same providers, so deriving one from the other would prove
/// nothing.
ProviderRegistry writer_registry() {
    auto keys = StaticEncryptionKeyManager::from_hex(kMasterKeyHex);
    EXPECT_TRUE(keys.has_value());
    auto provider = Aes256GcmEncryptionProvider::open(*keys);
    EXPECT_TRUE(provider.has_value());

    ProviderRegistry registry = passthrough_registry();
    registry.providers[kProviderId] = *provider;
    registry.primary = kProviderId;
    return registry;
}

EncryptionOptions key_flags() {
    EncryptionOptions options;
    options.providers = {kProviderId, std::string("hex:") + kMasterKeyHex};
    return options;
}

/// Everything below is passed through, so what a test names is the only thing that fails.
class FailingSnapshotCatalog final : public ManifestCatalog {
public:
    explicit FailingSnapshotCatalog(ManifestCatalog& below, Status status)
        : below_(below), status_(status) {}

    Result<std::optional<Entry>> read() override { return below_.read(); }
    Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                 uint64_t generation) override {
        return below_.compare_and_set(std::move(expected), generation);
    }
    std::future<Status> put_snapshot(uint64_t generation, Slice bytes) override {
        return below_.put_snapshot(generation, bytes);
    }
    std::future<GetResult> get_snapshot(uint64_t) override {
        return make_ready_future(GetResult(std::unexpected(status_)));
    }
    std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) override {
        return below_.put_edit(generation, seq, bytes);
    }
    std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) override {
        return below_.get_edit(generation, seq);
    }
    std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) override {
        return below_.list_edits(generation);
    }
    std::future<Status> delete_generation(uint64_t generation) override {
        return below_.delete_generation(generation);
    }

private:
    ManifestCatalog& below_;
    Status status_;
};

class CliVersionLoad : public ::testing::Test {
protected:
    /// A store with one snapshot and `edits` edits above it, written by the engine.
    std::unique_ptr<VersionSet> write_store(const ProviderRegistry& registry, int edits) {
        auto versions = std::make_unique<VersionSet>(
            catalog_, 1000,
            [](const std::vector<FileMetadata>&) { return std::vector<FileMetadata>{}; }, registry);
        EXPECT_EQ(versions->create(), Status::Ok);
        for (int i = 0; i < edits; ++i) {
            VersionEdit edit;
            edit.added.push_back(file(0, versions->allocate_file_number()));
            EXPECT_EQ(versions->apply(std::move(edit)), Status::Ok);
        }
        return versions;
    }

    /// The message `fail` reported, which is the whole of what an operator sees.
    static std::string message_from(const std::function<void()>& call) {
        try {
            call();
        } catch (const CLI::Error& error) {
            return error.what();
        }
        return "(no failure)";
    }

    TempDir dir_;
    DiskManifestCatalog catalog_{dir_.path()};
};

TEST_F(CliVersionLoad, ReadsAStoreWrittenWithoutEncryption) {
    const ProviderRegistry writer = passthrough_registry();
    auto versions = write_store(writer, 3);

    size_t edits = 0;
    auto version = load_version(catalog_, open_registry(EncryptionOptions{}),
                                versions->generation(), edits);
    ASSERT_NE(version, nullptr);
    EXPECT_EQ(edits, 3u);
    EXPECT_EQ(version->all_files().size(), 3u);
    EXPECT_EQ(version->next_file_number(), versions->current()->next_file_number());
}

TEST_F(CliVersionLoad, ReadsAnEncryptedStoreFromTheKeyTheFlagsName) {
    auto versions = write_store(writer_registry(), 2);

    size_t edits = 0;
    auto version =
        load_version(catalog_, open_registry(key_flags()), versions->generation(), edits);
    ASSERT_NE(version, nullptr);
    EXPECT_EQ(edits, 2u);
    EXPECT_EQ(version->all_files().size(), 2u);
}

/// `file:` and `env:` each hand the key through a temporary that the `string_view` taking it must
/// outlive, and each trims what surrounds it. Exercised rather than left to `hex:` alone because a
/// dangling view here is exactly what the sanitiser builds are for.
TEST_F(CliVersionLoad, ReadsAnEncryptedStoreFromAKeyFileAndFromTheEnvironment) {
    auto versions = write_store(writer_registry(), 1);
    const uint64_t generation = versions->generation();

    // Not the catalog's directory: a stray file there is the catalog's business.
    TempDir keys;
    const std::filesystem::path key_path = keys.path() / "master.hex";
    {
        std::ofstream out(key_path);
        out << kMasterKeyHex << "\n";   // a trailing newline is what a real key file has
    }

    EncryptionOptions from_file;
    from_file.providers = {kProviderId, "file:" + key_path.string()};
    size_t edits = 0;
    EXPECT_NE(load_version(catalog_, open_registry(from_file), generation, edits), nullptr);

    const std::string padded = std::string(" ") + kMasterKeyHex + "\n";
    ASSERT_EQ(::setenv("ELYSIUMKV_TEST_MASTER_KEY", padded.c_str(), 1), 0);
    EncryptionOptions from_env;
    from_env.providers = {kProviderId, "env:ELYSIUMKV_TEST_MASTER_KEY"};
    edits = 0;
    EXPECT_NE(load_version(catalog_, open_registry(from_env), generation, edits), nullptr);
    ::unsetenv("ELYSIUMKV_TEST_MASTER_KEY");
}

/// The premise of the test above: with no key configured it must fail, or that one passes against
/// a build that decrypts nothing.
TEST_F(CliVersionLoad, AnEncryptedStoreWithoutTheKeySaysWhichProviderIsMissing) {
    auto versions = write_store(writer_registry(), 1);

    const uint64_t generation = versions->generation();
    const std::string message = message_from([&] {
        size_t edits = 0;
        load_version(catalog_, open_registry(EncryptionOptions{}), generation, edits);
    });
    EXPECT_NE(message.find(kProviderId), std::string::npos) << message;
}

/// A key that is well-formed and wrong is not damage, and must not be reported as it.
TEST_F(CliVersionLoad, TheWrongKeyIsReportedAsAKeyProblem) {
    auto versions = write_store(writer_registry(), 1);

    EncryptionOptions other = key_flags();
    other.providers[1] = std::string("hex:") + std::string(64, 'a');

    const uint64_t generation = versions->generation();
    const std::string message = message_from([&] {
        size_t edits = 0;
        load_version(catalog_, open_registry(other), generation, edits);
    });
    EXPECT_NE(message.find("key"), std::string::npos) << message;
}

/// Recovery stops at the first gap, so an edit above one is unacknowledged and a reopen would not
/// apply it. Reporting it would describe a store nobody can open.
TEST_F(CliVersionLoad, ReplayStopsAtAGapAsRecoveryDoes) {
    const ProviderRegistry writer = passthrough_registry();
    auto versions = write_store(writer, 1);
    const uint64_t generation = versions->generation();

    VersionEdit orphan;
    orphan.added.push_back(file(0, 900));
    const std::string bytes = encode_version_edit(orphan);
    auto framed = ManifestPayload::seal(writer, generation,
                                        ManifestPayload::edit_address(generation, 3),
                                        Slice::from(bytes));
    ASSERT_TRUE(framed.has_value());
    ASSERT_EQ(catalog_.put_edit(generation, 3, Slice::from(*framed)).get(), Status::Ok);

    size_t edits = 0;
    auto version = load_version(catalog_, open_registry(EncryptionOptions{}), generation, edits);
    ASSERT_NE(version, nullptr);
    EXPECT_EQ(edits, 1u);
    EXPECT_EQ(version->all_files().size(), 1u) << "the edit above the gap was replayed";
}

/// `NotFound` is a collected generation; anything else is the store being unreachable or refused.
/// One sentence for both is what sent an operator looking at their data for a defect in the tool.
TEST_F(CliVersionLoad, AFetchFailureIsNotReportedAsAMissingSnapshot) {
    auto versions = write_store(passthrough_registry(), 0);
    const uint64_t generation = versions->generation();

    FailingSnapshotCatalog absent(catalog_, Status::NotFound);
    EXPECT_NE(message_from([&] {
                  size_t edits = 0;
                  load_version(absent, open_registry(EncryptionOptions{}), generation, edits);
              }).find("no snapshot"),
              std::string::npos);

    FailingSnapshotCatalog unreachable(catalog_, Status::Io);
    const std::string message = message_from([&] {
        size_t edits = 0;
        load_version(unreachable, open_registry(EncryptionOptions{}), generation, edits);
    });
    EXPECT_EQ(message.find("no snapshot"), std::string::npos) << message;
    EXPECT_NE(message.find(std::string(status_name(Status::Io))), std::string::npos) << message;
}

}  // namespace
}  // namespace elysiumkv::cli
