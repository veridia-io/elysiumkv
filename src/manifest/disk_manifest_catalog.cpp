#include "elysiumkv/disk_manifest_catalog.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <limits>
#include <string>
#include <system_error>

namespace elysiumkv {
namespace {

namespace fs = std::filesystem;

/// CURRENT holds "{generation} {token}\n" — the token is a monotonic counter, so
/// a CAS can be validated without any filesystem support for conditional writes.
struct CurrentFile {
    uint64_t generation = 0;
    uint64_t token = 0;
};

std::string format_token(uint64_t token) { return std::to_string(token); }

bool parse_current(const std::string& text, CurrentFile& out) {
    const char* begin = text.data();
    const char* end = begin + text.size();
    auto [after_generation, ec1] = std::from_chars(begin, end, out.generation);
    if (ec1 != std::errc() || after_generation == end || *after_generation != ' ') return false;
    auto [after_token, ec2] = std::from_chars(after_generation + 1, end, out.token);
    return ec2 == std::errc();
}

/// Strict: every character a digit, and no overflow. A name that is not ours must be left alone
/// rather than guessed at.
bool parse_u64(const std::string& text, uint64_t& out) {
    if (text.empty()) return false;
    uint64_t value = 0;
    for (const char c : text) {
        if (c < '0' || c > '9') return false;
        if (value > (std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(c - '0')) / 10) {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(c - '0');
    }
    out = value;
    return true;
}

std::string generation_name(uint64_t generation) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%012llu", static_cast<unsigned long long>(generation));
    return buf;
}

std::string edit_name(uint64_t seq) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "edit-%012llu", static_cast<unsigned long long>(seq));
    return buf;
}

}  // namespace

DiskManifestCatalog::DiskManifestCatalog(fs::path directory) : directory_(std::move(directory)) {}

fs::path DiskManifestCatalog::generation_dir(uint64_t generation) const {
    return directory_ / "manifest" / generation_name(generation);
}

fs::path DiskManifestCatalog::current_path() const { return directory_ / "manifest" / "CURRENT"; }

Status DiskManifestCatalog::write_object(const fs::path& path, Slice bytes) {
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    if (ec) return Status::Io;

    // Write-once: O_EXCL, because a put at an existing address is a programming
    // error rather than an overwrite (ARCHITECTURE.md "Ownership is one compare-and-set").
    //
    // **`Config`, and the same code every implementation reports.** This used to be
    // `Unusable` while the S3 and DynamoDB catalogs returned `Config` for the same
    // condition, and the contract suite only checked that it was not `Ok` — so the
    // divergence was invisible. It matters because the engine acts on it: an
    // occupied edit address is how a fenced writer finds out, and a reaction keyed
    // on the status cannot work if the status depends on which catalog is
    // configured.
    //
    // The compare_and_set path shares this helper but removes its temp file first,
    // so it does not normally reach here.
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return errno == EEXIST ? Status::Config : Status::Io;

    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::write(fd, bytes.data() + written, bytes.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            ::unlink(path.c_str());
            return Status::Io;
        }
        written += static_cast<size_t>(n);
    }
    // An edit must be durable before the objects it releases may be deleted
    // (ARCHITECTURE.md "Open and recovery"), so this fsync is load-bearing rather than defensive.
    const bool synced = ::fsync(fd) == 0;
    ::close(fd);
    return synced ? Status::Ok : Status::Io;
}

GetResult DiskManifestCatalog::read_object(const fs::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return std::unexpected(errno == ENOENT ? Status::NotFound : Status::Io);

    Buffer out;
    uint8_t chunk[64 * 1024];
    while (true) {
        const ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n < 0) {
            if (errno == EINTR) continue;
            ::close(fd);
            return std::unexpected(Status::Io);
        }
        if (n == 0) break;
        out.insert(out.end(), chunk, chunk + n);
    }
    ::close(fd);
    return out;
}

Result<std::optional<ManifestCatalog::Entry>> DiskManifestCatalog::read() {
    auto bytes = read_object(current_path());
    if (!bytes) {
        if (bytes.error() == Status::NotFound) return std::optional<Entry>{};
        return std::unexpected(bytes.error());
    }
    CurrentFile current;
    if (!parse_current(std::string(bytes->begin(), bytes->end()), current)) {
        return std::unexpected(Status::Corrupt);
    }
    return std::optional<Entry>(Entry{current.generation, format_token(current.token)});
}

Result<std::optional<ManifestCatalog::Entry>> DiskManifestCatalog::compare_and_set(
    std::optional<Entry> expected, uint64_t generation) {
    auto observed = read();
    if (!observed) return std::unexpected(observed.error());

    const bool matches =
        (!expected.has_value() && !observed->has_value()) ||
        (expected.has_value() && observed->has_value() &&
         expected->generation == (*observed)->generation && expected->token == (*observed)->token);
    if (!matches) return std::optional<Entry>{};  // fenced

    uint64_t token = 1;
    if (observed->has_value()) {
        CurrentFile parsed;
        parsed.token = 0;
        std::from_chars((*observed)->token.data(),
                        (*observed)->token.data() + (*observed)->token.size(), parsed.token);
        token = parsed.token + 1;
    }

    const std::string text = std::to_string(generation) + " " + format_token(token) + "\n";
    const fs::path temp = current_path().string() + ".tmp";

    std::error_code ec;
    fs::create_directories(current_path().parent_path(), ec);
    if (ec) return std::unexpected(Status::Io);
    fs::remove(temp, ec);
    if (Status status = write_object(temp, Slice::from(text)); status != Status::Ok) {
        return std::unexpected(status);
    }
    fs::rename(temp, current_path(), ec);
    if (ec) return std::unexpected(Status::Io);

    return std::optional<Entry>(Entry{generation, format_token(token)});
}

std::future<Status> DiskManifestCatalog::put_snapshot(uint64_t generation, Slice bytes) {
    return make_ready_future(write_object(generation_dir(generation) / "snapshot", bytes));
}

std::future<GetResult> DiskManifestCatalog::get_snapshot(uint64_t generation) {
    return make_ready_future(read_object(generation_dir(generation) / "snapshot"));
}

std::future<Status> DiskManifestCatalog::put_edit(uint64_t generation, uint64_t seq, Slice bytes) {
    return make_ready_future(write_object(generation_dir(generation) / edit_name(seq), bytes));
}

std::future<GetResult> DiskManifestCatalog::get_edit(uint64_t generation, uint64_t seq) {
    return make_ready_future(read_object(generation_dir(generation) / edit_name(seq)));
}

std::future<Result<std::vector<uint64_t>>> DiskManifestCatalog::list_generations() {
    std::error_code ec;
    fs::directory_iterator it(directory_ / "manifest", ec);
    if (ec) {
        // No manifest directory is no generations, not a failure: the store may not exist yet.
        return make_ready_future(Result<std::vector<uint64_t>>(std::vector<uint64_t>{}));
    }

    std::vector<uint64_t> generations;
    for (const fs::directory_entry& entry : it) {
        if (!entry.is_directory()) continue;   // CURRENT is a file
        const std::string name = entry.path().filename().string();
        if (name.size() != 12) continue;
        uint64_t generation = 0;
        if (!parse_u64(name, generation)) continue;   // not ours to reason about
        generations.push_back(generation);
    }
    return make_ready_future(Result<std::vector<uint64_t>>(std::move(generations)));
}

std::future<Result<std::vector<uint64_t>>> DiskManifestCatalog::list_edits(uint64_t generation) {
    std::error_code ec;
    fs::directory_iterator it(generation_dir(generation), ec);
    if (ec) {
        // A generation directory that does not exist has no edits — the pointer,
        // not the directory, is what says the generation is live.
        return make_ready_future(Result<std::vector<uint64_t>>(std::vector<uint64_t>{}));
    }

    std::vector<uint64_t> seqs;
    for (const fs::directory_entry& entry : it) {
        const std::string name = entry.path().filename().string();
        if (!name.starts_with("edit-")) continue;
        uint64_t seq = 0;
        const char* begin = name.data() + 5;
        if (std::from_chars(begin, name.data() + name.size(), seq).ec == std::errc()) {
            seqs.push_back(seq);
        }
    }
    std::sort(seqs.begin(), seqs.end());
    return make_ready_future(Result<std::vector<uint64_t>>(std::move(seqs)));
}

std::future<Status> DiskManifestCatalog::delete_generation(uint64_t generation) {
    std::error_code ec;
    fs::remove_all(generation_dir(generation), ec);
    return make_ready_future(ec ? Status::Io : Status::Ok);
}

}  // namespace elysiumkv
