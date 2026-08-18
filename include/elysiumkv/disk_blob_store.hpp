#ifndef ELYSIUMKV_DISK_BLOB_STORE_HPP
#define ELYSIUMKV_DISK_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>

namespace elysiumkv {

class OpenFileCache;
struct OpenFile;

/// ARCHITECTURE.md "Immutable named objects" — production store over a directory. Futures complete synchronously.
///
/// **The root directory must exist.** A missing root is `Status::Io`, never
/// `NotFound`: creating it is the deployment's job, and an unmounted volume must
/// fail loudly rather than resemble a fresh one (ARCHITECTURE.md "Immutable named objects"). An *empty* existing root
/// lists successfully and means empty.
class DiskBlobStore final : public BlobStore {
public:
    /// `id` must be stable across restarts — it is persisted per file in
    /// FileMetadata::store_id. Defaults to the canonicalised root path, which is
    /// stable only if the deployment mounts the store at a fixed path; pass an
    /// explicit id when it does not.
    explicit DiskBlobStore(std::filesystem::path root, std::string id = {});
    ~DiskBlobStore() override;

    /// Writes are fsync'd before the object becomes visible, and the directory
    /// entry after. A `Durable` level promises that a written object survives a
    /// crash; without this the manifest could reference an object whose bytes
    /// never reached the platter, which reopens as `Status::Corrupt`. Cache
    /// layers (ARCHITECTURE.md "Caches chain") hold only copies and turn this off.
    void set_sync_writes(bool sync) { sync_writes_ = sync; }

    /// How many descriptors this store may keep open between reads. Objects are immutable, so a
    /// held descriptor can never be stale, and keeping one removes an `open`, an `fstat` and a
    /// `close` from every block read.
    ///
    /// **Deliberately small by default.** This multiplies by every store in the process — a
    /// service with two tiers per partition has dozens — against a soft limit as low as 256, and
    /// an LSM's hot set is L0 plus whatever a scan is walking. Zero disables the cache.
    void set_max_open_files(size_t count);

    std::string id() const override { return id_; }
    const std::filesystem::path& root() const { return root_; }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    GetResult get_sync(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    std::future<ListResult> list(std::string_view prefix) override;

private:
    /// The cached descriptor for `name`, opening and recording it if it is not held. Null on
    /// failure, with `failure` set.
    std::shared_ptr<const OpenFile> open_for_read(std::string_view name, Status& failure);

    GetResult do_get(std::string_view name, uint64_t offset, size_t len);
    Status do_put(std::string_view name, Slice bytes);
    Status do_remove(std::string_view name);
    ListResult do_list(std::string_view prefix);

    bool root_is_directory() const;
    std::filesystem::path path_for(std::string_view name) const;

    std::filesystem::path root_;
    std::string id_;
    bool sync_writes_ = true;
    std::unique_ptr<OpenFileCache> open_files_;
    std::atomic<uint64_t> temp_counter_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DISK_BLOB_STORE_HPP
