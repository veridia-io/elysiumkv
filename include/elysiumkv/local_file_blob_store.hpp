#ifndef ELYSIUMKV_LOCAL_FILE_BLOB_STORE_HPP
#define ELYSIUMKV_LOCAL_FILE_BLOB_STORE_HPP

#include "elysiumkv/blob_store.hpp"

#include <atomic>
#include <filesystem>
#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "Immutable named objects" — production store over a directory. Futures complete synchronously.
///
/// **The root directory must exist.** A missing root is `Status::Io`, never
/// `NotFound`: creating it is the deployment's job, and an unmounted volume must
/// fail loudly rather than resemble a fresh one (ARCHITECTURE.md "Immutable named objects"). An *empty* existing root
/// lists successfully and means empty.
class LocalFileBlobStore final : public BlobStore {
public:
    /// `id` must be stable across restarts — it is persisted per file in
    /// FileMetadata::store_id. Defaults to the canonicalised root path, which is
    /// stable only if the deployment mounts the store at a fixed path; pass an
    /// explicit id when it does not.
    explicit LocalFileBlobStore(std::filesystem::path root, std::string id = {});

    /// Writes are fsync'd before the object becomes visible, and the directory
    /// entry after. A `Durable` level promises that a written object survives a
    /// crash; without this the manifest could reference an object whose bytes
    /// never reached the platter, which reopens as `Status::Corrupt`. Cache
    /// layers (ARCHITECTURE.md "Caches chain") hold only copies and turn this off.
    void set_sync_writes(bool sync) { sync_writes_ = sync; }

    std::string id() const override { return id_; }
    const std::filesystem::path& root() const { return root_; }

    std::future<GetResult> get(std::string_view name, uint64_t offset, size_t len) override;
    std::future<Status> put(std::string_view name, Slice bytes) override;
    std::future<Status> remove(std::string_view name) override;
    std::future<ListResult> list(std::string_view prefix) override;

private:
    GetResult do_get(std::string_view name, uint64_t offset, size_t len);
    Status do_put(std::string_view name, Slice bytes);
    Status do_remove(std::string_view name);
    ListResult do_list(std::string_view prefix);

    bool root_is_directory() const;
    std::filesystem::path path_for(std::string_view name) const;

    std::filesystem::path root_;
    std::string id_;
    bool sync_writes_ = true;
    std::atomic<uint64_t> temp_counter_{0};
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_LOCAL_FILE_BLOB_STORE_HPP
