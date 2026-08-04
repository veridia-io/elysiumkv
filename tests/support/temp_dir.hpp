#ifndef ELYSIUMKV_TESTS_SUPPORT_TEMP_DIR_HPP
#define ELYSIUMKV_TESTS_SUPPORT_TEMP_DIR_HPP

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>

namespace elysiumkv::test {

/// A unique directory removed on destruction.
class TempDir {
public:
    TempDir() {
        std::string tmpl = (std::filesystem::temp_directory_path() / "elysiumkv-XXXXXX").string();
        char* made = ::mkdtemp(tmpl.data());
        path_ = made != nullptr ? std::filesystem::path(made) : std::filesystem::path(tmpl);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_SUPPORT_TEMP_DIR_HPP
