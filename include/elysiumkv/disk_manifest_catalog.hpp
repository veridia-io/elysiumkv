#ifndef ELYSIUMKV_DISK_MANIFEST_CATALOG_HPP
#define ELYSIUMKV_DISK_MANIFEST_CATALOG_HPP

#include "elysiumkv/manifest_catalog.hpp"

#include <filesystem>
#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "Ownership is one compare-and-set" — generation objects as files under `manifest/{generation}/`; the
/// pointer as a temp file atomically renamed over `CURRENT`.
///
/// A dedicated lock file serializes the pointer's compare and replacement across processes.
/// `token` is a monotonic counter embedded in CURRENT, so a stale expectation still loses after
/// the process holding the lock exits.
class DiskManifestCatalog final : public ManifestCatalog {
public:
    explicit DiskManifestCatalog(std::filesystem::path directory);

    Result<std::optional<Entry>> read() override;
    Result<std::optional<Entry>> compare_and_set(std::optional<Entry> expected,
                                                 uint64_t generation) override;

    std::future<Status> put_snapshot(uint64_t generation, Slice bytes) override;
    std::future<GetResult> get_snapshot(uint64_t generation) override;
    std::future<Status> put_edit(uint64_t generation, uint64_t seq, Slice bytes) override;
    std::future<GetResult> get_edit(uint64_t generation, uint64_t seq) override;
    std::future<Result<std::vector<uint64_t>>> list_edits(uint64_t generation) override;
    std::future<Status> delete_generation(uint64_t generation) override;
    /// Enumerated from the directory, so a generation left behind by a crash at any point in this
    /// store's history is found — not only a recent one.
    std::future<Result<std::vector<uint64_t>>> list_generations() override;

    const std::filesystem::path& directory() const { return directory_; }

private:
    std::filesystem::path generation_dir(uint64_t generation) const;
    std::filesystem::path current_path() const;
    Status write_object(const std::filesystem::path& path, Slice bytes);
    GetResult read_object(const std::filesystem::path& path);

    std::filesystem::path directory_;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_DISK_MANIFEST_CATALOG_HPP
