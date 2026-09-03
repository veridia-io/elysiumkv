#ifndef ELYSIUMKV_TEST_FAULT_INJECTING_MANIFEST_FILE_SYSTEM_HPP
#define ELYSIUMKV_TEST_FAULT_INJECTING_MANIFEST_FILE_SYSTEM_HPP

#include "manifest/disk_manifest_file_system.hpp"

#include <optional>
#include <set>

namespace elysiumkv::test {

class FaultInjectingManifestFileSystem final : public detail::DiskManifestFileSystem {
public:
    enum class Op { Open, Write, Fsync, Rename, DirectoryFsync };

    void fail_next(Op op) { failure_ = op; }
    void crash();

    int open(const std::filesystem::path& path, int flags, mode_t mode) override;
    ssize_t write(int fd, const void* bytes, size_t size) override;
    ssize_t read(int fd, void* bytes, size_t size) override;
    int fsync(int fd) override;
    int close(int fd) override;
    int unlink(const std::filesystem::path& path) override;
    bool create_directories(const std::filesystem::path& path, std::error_code& error) override;
    bool remove(const std::filesystem::path& path, std::error_code& error) override;
    void rename(const std::filesystem::path& from, const std::filesystem::path& to,
                std::error_code& error) override;
    uintmax_t remove_all(const std::filesystem::path& path, std::error_code& error) override;
    bool sync_directory(const std::filesystem::path& path) override;

private:
    bool should_fail(Op op);

    std::optional<Op> failure_;
    std::set<std::filesystem::path> pending_entries_;
    std::set<std::filesystem::path> pending_directories_;
};

}  // namespace elysiumkv::test

#endif
