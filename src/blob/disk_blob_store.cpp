#include "elysiumkv/disk_blob_store.hpp"

#include "blob/object_name.hpp"
#include "blob/open_file_cache.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <string>
#include <system_error>
#include <utility>

namespace elysiumkv {
namespace {

namespace fs = std::filesystem;

constexpr std::string_view kTempPrefix = ".tmp.";

/// Small on purpose — see `set_max_open_files`. Enough for L0 plus the file a scan is walking on
/// each of a few levels, which is the whole hot set of an LSM read.
constexpr size_t kDefaultMaxOpenFiles = 32;

/// Flat, non-empty, no path separators, no leading dot. The leading-dot rule
/// also keeps user objects clear of the temp files below.
/// Everything that is not "this object is definitely absent" is Io (ARCHITECTURE.md "Immutable named objects").
Status errno_to_status(int err) {
    return err == ENOENT ? Status::NotFound : Status::Io;
}

class FileDescriptor {
public:
    explicit FileDescriptor(int fd) : fd_(fd) {}
    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    ~FileDescriptor() {
        if (fd_ >= 0) ::close(fd_);
    }
    int get() const { return fd_; }
    bool valid() const { return fd_ >= 0; }

private:
    int fd_;
};

bool write_all(int fd, const uint8_t* data, size_t size) {
    size_t written = 0;
    while (written < size) {
        const ssize_t n = ::write(fd, data + written, size - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

bool fsync_directory(const fs::path& dir) {
    FileDescriptor fd(::open(dir.c_str(), O_RDONLY));
    if (!fd.valid()) return false;
    return ::fsync(fd.get()) == 0;
}

}  // namespace

DiskBlobStore::DiskBlobStore(fs::path root, std::string id)
    : root_(std::move(root)),
      id_(std::move(id)),
      open_files_(std::make_unique<OpenFileCache>(kDefaultMaxOpenFiles)) {
    if (id_.empty()) {
        std::error_code ec;
        const fs::path canonical = fs::weakly_canonical(root_, ec);
        id_ = ec ? root_.string() : canonical.string();
    }
}

DiskBlobStore::~DiskBlobStore() = default;

void DiskBlobStore::set_max_open_files(size_t count) {
    open_files_ = std::make_unique<OpenFileCache>(count);
}

bool DiskBlobStore::root_is_directory() const {
    std::error_code ec;
    return fs::is_directory(root_, ec);
}

fs::path DiskBlobStore::path_for(std::string_view name) const {
    return root_ / std::string(name);
}

std::future<GetResult> DiskBlobStore::get(std::string_view name, uint64_t offset, size_t len) {
    return make_ready_future(get_sync(name, offset, len));
}
GetResult DiskBlobStore::get_sync(std::string_view name, uint64_t offset, size_t len) {
    auto result = do_get(name, offset, len);
    note_get(result);
    return result;
}
std::future<Status> DiskBlobStore::put(std::string_view name, Slice bytes) {
    const Status status = do_put(name, bytes);
    note_put(status, bytes.size());
    return make_ready_future(status);
}
std::future<Status> DiskBlobStore::remove(std::string_view name) {
    const Status status = do_remove(name);
    note_remove(status);
    return make_ready_future(status);
}
std::future<ListResult> DiskBlobStore::list(std::string_view prefix) {
    auto result = do_list(prefix);
    note_list(result);
    return make_ready_future(std::move(result));
}

std::shared_ptr<const OpenFile> DiskBlobStore::open_for_read(std::string_view name,
                                                            Status& failure) {
    if (auto held = open_files_->lookup(name)) return held;

    int fd = ::open(path_for(name).c_str(), O_RDONLY);
    if (fd < 0 && (errno == EMFILE || errno == ENFILE)) {
        // Out of descriptors, and this cache is holding some. Giving them all back is strictly
        // better than failing a read to keep them.
        open_files_->clear();
        fd = ::open(path_for(name).c_str(), O_RDONLY);
    }
    if (fd < 0) {
        failure = errno_to_status(errno);
        // A missing file inside a missing root is not evidence of absence.
        if (failure == Status::NotFound && !root_is_directory()) failure = Status::Io;
        return nullptr;
    }

    struct stat st {};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        failure = Status::Io;
        return nullptr;
    }
    return open_files_->insert(
        name, std::make_shared<const OpenFile>(fd, static_cast<uint64_t>(st.st_size)));
}

GetResult DiskBlobStore::do_get(std::string_view name, uint64_t offset, size_t len) {
    if (!is_valid_object_name(name)) return std::unexpected(Status::Config);

    Status failure = Status::Io;
    auto file = open_for_read(name, failure);
    if (file == nullptr) return std::unexpected(failure);

    const uint64_t file_size = file->size;
    if (offset >= file_size) return Buffer{};

    const uint64_t available = file_size - offset;
    const uint64_t want = (len == kReadToEnd || len > available) ? available
                                                                 : static_cast<uint64_t>(len);
    Buffer out(static_cast<size_t>(want));

    size_t done = 0;
    while (done < out.size()) {
        const ssize_t n = ::pread(file->fd, out.data() + done, out.size() - done,
                                  static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) continue;
            return std::unexpected(Status::Io);
        }
        if (n == 0) break;  // raced with a truncation; return what exists
        done += static_cast<size_t>(n);
    }
    out.resize(done);
    return out;
}

Status DiskBlobStore::do_put(std::string_view name, Slice bytes) {
    if (!is_valid_object_name(name)) return Status::Config;
    if (!root_is_directory()) return Status::Io;

    // Write to a temp file, then hard-link it into place. link() fails with
    // EEXIST if the name is taken, which is how write-once is enforced;
    // rename() would silently overwrite.
    const std::string temp_name = std::string(kTempPrefix) + std::to_string(::getpid()) + "." +
                                  std::to_string(temp_counter_.fetch_add(1)) + "." +
                                  std::string(name);
    const fs::path temp_path = root_ / temp_name;
    const fs::path final_path = path_for(name);

    {
        FileDescriptor fd(::open(temp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644));
        if (!fd.valid()) return Status::Io;
        if (!write_all(fd.get(), bytes.data(), bytes.size())) {
            ::unlink(temp_path.c_str());
            return Status::Io;
        }
        if (sync_writes_ && ::fsync(fd.get()) != 0) {
            ::unlink(temp_path.c_str());
            return Status::Io;
        }
    }

    if (::link(temp_path.c_str(), final_path.c_str()) != 0) {
        const int err = errno;
        ::unlink(temp_path.c_str());
        // The caller renumbers rather than retrying bytes under an immutable name.
        return err == EEXIST ? Status::Unusable : Status::Io;
    }
    ::unlink(temp_path.c_str());

    if (sync_writes_ && !fsync_directory(root_)) return Status::Io;
    return Status::Ok;
}

Status DiskBlobStore::do_remove(std::string_view name) {
    if (!is_valid_object_name(name)) return Status::Config;
    // Before the unlink, not after. A held descriptor keeps reading an unlinked inode
    // perfectly well, so leaving one behind would make a removed object still readable.
    open_files_->erase(name);
    if (::unlink(path_for(name).c_str()) == 0) return Status::Ok;
    if (errno == ENOENT) {
        // Idempotent — but only if we could actually look.
        return root_is_directory() ? Status::Ok : Status::Io;
    }
    return Status::Io;
}

ListResult DiskBlobStore::do_list(std::string_view prefix) {
    std::error_code ec;
    fs::directory_iterator it(root_, ec);
    if (ec) return std::unexpected(Status::Io);  // includes a missing root

    std::vector<std::string> names;
    for (const fs::directory_entry& entry : it) {
        std::string name = entry.path().filename().string();
        if (name.starts_with(kTempPrefix)) continue;  // never visible
        if (!is_valid_object_name(name)) continue;
        if (name.compare(0, prefix.size(), prefix) != 0) continue;
        names.push_back(std::move(name));
    }
    // An empty existing directory lists successfully and means empty (ARCHITECTURE.md "Immutable named objects").
    std::sort(names.begin(), names.end());
    return names;
}

}  // namespace elysiumkv
