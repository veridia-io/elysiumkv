#ifndef ELYSIUMKV_DISK_MANIFEST_CATALOG_HPP
#define ELYSIUMKV_DISK_MANIFEST_CATALOG_HPP

#include "elysiumkv/manifest_catalog.hpp"

#include <filesystem>
#include <string>

namespace elysiumkv {

/// ARCHITECTURE.md "Ownership is one compare-and-set" — generation objects as files under `manifest/{generation}/`; the
/// pointer as a temp file atomically renamed over `CURRENT`.
///
/// `token` is a monotonic counter embedded in that file, so the CAS is validated
/// even though a single-writer filesystem makes contention impossible. The point
/// is that the *engine* exercises the same code path it will run against a
/// contended remote catalog.
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
