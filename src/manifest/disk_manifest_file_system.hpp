#ifndef ELYSIUMKV_DISK_MANIFEST_FILE_SYSTEM_HPP
#define ELYSIUMKV_DISK_MANIFEST_FILE_SYSTEM_HPP

#include <sys/types.h>

#include <cstddef>
#include <filesystem>
#include <system_error>

namespace elysiumkv::detail {

class DiskManifestFileSystem {
public:
    virtual int open(const std::filesystem::path& path, int flags, mode_t mode = 0) = 0;
    virtual ssize_t write(int fd, const void* bytes, size_t size) = 0;
    virtual ssize_t read(int fd, void* bytes, size_t size) = 0;
    virtual int fsync(int fd) = 0;
    virtual int close(int fd) = 0;
    virtual int unlink(const std::filesystem::path& path) = 0;
    virtual bool create_directories(const std::filesystem::path& path,
                                    std::error_code& error) = 0;
    virtual bool remove(const std::filesystem::path& path, std::error_code& error) = 0;
    virtual void rename(const std::filesystem::path& from, const std::filesystem::path& to,
                        std::error_code& error) = 0;
    virtual uintmax_t remove_all(const std::filesystem::path& path, std::error_code& error) = 0;
    virtual bool sync_directory(const std::filesystem::path& path) = 0;
    virtual ~DiskManifestFileSystem() = default;
};

}  // namespace elysiumkv::detail

#endif
