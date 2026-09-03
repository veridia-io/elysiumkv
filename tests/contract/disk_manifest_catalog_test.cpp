#include "contract/manifest_catalog_contract.hpp"
#include "support/temp_dir.hpp"
#include "elysiumkv/disk_manifest_catalog.hpp"

#include <gtest/gtest.h>

#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <filesystem>
#include <fstream>

#include <memory>

namespace elysiumkv::test {
namespace {

bool write_byte(int fd, char value) {
    while (true) {
        const ssize_t written = ::write(fd, &value, 1);
        if (written == 1) return true;
        if (written >= 0 || errno != EINTR) return false;
    }
}

bool read_byte(int fd, char& value) {
    while (true) {
        const ssize_t read = ::read(fd, &value, 1);
        if (read == 1) return true;
        if (read >= 0 || errno != EINTR) return false;
    }
}

/// One temp directory per test, shared by every catalog the factory hands out —
/// which is what lets the contract's fencing case race two instances against the
/// same store.
ManifestCatalogFactory local_factory() {
    auto dir = std::make_shared<TempDir>();
    return {"DiskManifestCatalog", [dir] {
                auto* catalog = new DiskManifestCatalog(dir->path());
                return std::shared_ptr<ManifestCatalog>(
                    catalog, [dir](ManifestCatalog* p) { delete p; });
            }};
}

INSTANTIATE_TEST_SUITE_P(DiskCatalog, ManifestCatalogContract,
                         ::testing::Values(local_factory()), ManifestCatalogFactoryName());

TEST(DiskManifestCatalog, PointerSurvivesReopen) {
    TempDir dir;
    ManifestCatalog::Entry installed;
    {
        DiskManifestCatalog catalog(dir.path());
        auto entry = catalog.compare_and_set(std::nullopt, 1);
        ASSERT_TRUE(entry.has_value() && entry->has_value());
        installed = **entry;
    }
    {
        DiskManifestCatalog reopened(dir.path());
        auto pointer = reopened.read();
        ASSERT_TRUE(pointer.has_value() && pointer->has_value());
        EXPECT_EQ((*pointer)->generation, installed.generation);
        EXPECT_EQ((*pointer)->token, installed.token);
    }
}

TEST(DiskManifestCatalog, ADamagedPointerIsCorruptNotAbsent) {
    TempDir dir;
    DiskManifestCatalog catalog(dir.path());
    ASSERT_TRUE(catalog.compare_and_set(std::nullopt, 1).has_value());

    std::ofstream(dir.path() / "manifest" / "CURRENT", std::ios::trunc) << "not a pointer";
    auto pointer = catalog.read();
    ASSERT_FALSE(pointer.has_value());
    EXPECT_EQ(pointer.error(), Status::Corrupt)
        << "an unreadable pointer must not look like an empty store";
}

TEST(DiskManifestCatalog, ConcurrentProcessesLeaveExactlyOneCasWinner) {
    TempDir dir;
    DiskManifestCatalog catalog(dir.path());
    auto installed = catalog.compare_and_set(std::nullopt, 1);
    ASSERT_TRUE(installed.has_value() && installed->has_value());

    constexpr size_t kProcesses = 8;
    std::array<std::array<int, 2>, kProcesses> ready{};
    std::array<std::array<int, 2>, kProcesses> go{};
    std::array<std::array<int, 2>, kProcesses> result{};
    std::array<pid_t, kProcesses> children{};

    for (size_t i = 0; i < kProcesses; ++i) {
        ASSERT_EQ(::pipe(ready[i].data()), 0);
        ASSERT_EQ(::pipe(go[i].data()), 0);
        ASSERT_EQ(::pipe(result[i].data()), 0);
        children[i] = ::fork();
        ASSERT_GE(children[i], 0);
        if (children[i] == 0) {
            ::close(ready[i][0]);
            ::close(go[i][1]);
            ::close(result[i][0]);
            DiskManifestCatalog contender(dir.path());
            const char signal = 'R';
            if (!write_byte(ready[i][1], signal)) _exit(2);
            char start = 0;
            if (!read_byte(go[i][0], start)) _exit(3);
            auto outcome = contender.compare_and_set(**installed, 2 + i);
            const char answer = !outcome                 ? 'E'
                                : outcome->has_value()   ? 'W'
                                                        : 'L';
            if (!write_byte(result[i][1], answer)) _exit(4);
            _exit(0);
        }
        ::close(ready[i][1]);
        ::close(go[i][0]);
        ::close(result[i][1]);
    }

    for (size_t i = 0; i < kProcesses; ++i) {
        char signal = 0;
        ASSERT_TRUE(read_byte(ready[i][0], signal));
        ASSERT_EQ(signal, 'R');
    }
    for (size_t i = 0; i < kProcesses; ++i) {
        const char start = 'G';
        ASSERT_TRUE(write_byte(go[i][1], start));
    }

    int winners = 0;
    int losers = 0;
    int errors = 0;
    for (size_t i = 0; i < kProcesses; ++i) {
        char answer = 0;
        ASSERT_TRUE(read_byte(result[i][0], answer));
        winners += answer == 'W' ? 1 : 0;
        losers += answer == 'L' ? 1 : 0;
        errors += answer == 'E' ? 1 : 0;
        int status = 0;
        ASSERT_EQ(::waitpid(children[i], &status, 0), children[i]);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
    }
    EXPECT_EQ(winners, 1);
    EXPECT_EQ(losers, static_cast<int>(kProcesses - 1));
    EXPECT_EQ(errors, 0) << "contention must be reported as a lost CAS, not an I/O failure";
}

}  // namespace
}  // namespace elysiumkv::test
