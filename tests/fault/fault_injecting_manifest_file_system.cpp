#include "fault/fault_injecting_manifest_file_system.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <vector>

namespace elysiumkv::test {
namespace fs = std::filesystem;

bool FaultInjectingManifestFileSystem::should_fail(Op op) {
    if (failure_ != op) return false;
    failure_.reset();
    errno = EIO;
    return true;
}

int FaultInjectingManifestFileSystem::open(const fs::path& path, int flags, mode_t mode) {
    if (should_fail(Op::Open)) return -1;
    const bool existed = fs::exists(path);
    const int fd = ::open(path.c_str(), flags, mode);
    if (fd >= 0 && (flags & O_CREAT) != 0 && !existed) pending_entries_.insert(path);
    return fd;
}

ssize_t FaultInjectingManifestFileSystem::write(int fd, const void* bytes, size_t size) {
    if (should_fail(Op::Write)) return -1;
    return ::write(fd, bytes, size);
}

ssize_t FaultInjectingManifestFileSystem::read(int fd, void* bytes, size_t size) {
    return ::read(fd, bytes, size);
}

int FaultInjectingManifestFileSystem::fsync(int fd) {
    if (should_fail(Op::Fsync)) return -1;
    return ::fsync(fd);
}

int FaultInjectingManifestFileSystem::close(int fd) {
    return ::close(fd);
}

int FaultInjectingManifestFileSystem::unlink(const fs::path& path) {
    pending_entries_.erase(path);
    return ::unlink(path.c_str());
}

bool FaultInjectingManifestFileSystem::create_directories(const fs::path& path,
                                                           std::error_code& error) {
    std::vector<fs::path> missing;
    for (fs::path current = path; !current.empty() && !fs::exists(current);
         current = current.parent_path()) {
        missing.push_back(current);
    }
    const bool created = fs::create_directories(path, error);
    if (!error) pending_directories_.insert(missing.begin(), missing.end());
    return created;
}

bool FaultInjectingManifestFileSystem::remove(const fs::path& path, std::error_code& error) {
    pending_entries_.erase(path);
    return fs::remove(path, error);
}

void FaultInjectingManifestFileSystem::rename(const fs::path& from, const fs::path& to,
                                               std::error_code& error) {
    if (should_fail(Op::Rename)) {
        error = std::make_error_code(std::errc::io_error);
        return;
    }
    fs::rename(from, to, error);
    if (!error) {
        pending_entries_.erase(from);
        pending_entries_.insert(to);
    }
}

uintmax_t FaultInjectingManifestFileSystem::remove_all(const fs::path& path,
                                                        std::error_code& error) {
    const uintmax_t removed = fs::remove_all(path, error);
    if (!error) {
        std::erase_if(pending_entries_, [&](const fs::path& entry) {
            return entry == path || entry.string().starts_with(path.string() + "/");
        });
        std::erase_if(pending_directories_, [&](const fs::path& directory) {
            return directory == path || directory.string().starts_with(path.string() + "/");
        });
    }
    return removed;
}

bool FaultInjectingManifestFileSystem::sync_directory(const fs::path& path) {
    if (should_fail(Op::DirectoryFsync)) return false;
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0 && ::close(fd) == 0;
    if (ok) {
        std::erase_if(pending_entries_,
                      [&](const fs::path& entry) { return entry.parent_path() == path; });
        pending_directories_.erase(path);
    }
    return ok;
}

void FaultInjectingManifestFileSystem::crash() {
    std::error_code ignored;
    for (const fs::path& entry : pending_entries_) fs::remove(entry, ignored);
    std::vector<fs::path> directories(pending_directories_.begin(), pending_directories_.end());
    std::sort(directories.begin(), directories.end(),
              [](const fs::path& left, const fs::path& right) {
                  return left.native().size() > right.native().size();
              });
    for (const fs::path& directory : directories) fs::remove(directory, ignored);
    pending_entries_.clear();
    pending_directories_.clear();
}

}  // namespace elysiumkv::test
