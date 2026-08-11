#ifndef ELYSIUMKV_BLOB_OBJECT_NAME_HPP
#define ELYSIUMKV_BLOB_OBJECT_NAME_HPP

#include <string_view>

namespace elysiumkv {

/// What names a `BlobStore` accepts (ARCHITECTURE.md "Immutable named objects"), in **one** place.
///
/// It was in two, and they disagreed: `S3BlobStore` accepted a leading dot while
/// `DiskBlobStore` rejected it, so `put(".hidden")` was a configuration error
/// on one store and a stored object on the other. The shared contract case
/// (`MalformedNamesAreAConfigurationError`) pins the rule, and found the divergence
/// the first time the remote store was run through it.
///
/// The rule is the filesystem store's, because it is the strictest: a name is a
/// single path component, so no separator, no escape, no embedded NUL, and no
/// leading dot — a dotfile is invisible to `list` on a filesystem, which would make
/// a stored object silently unlistable. S3 does not care, but a name that means
/// different things in different tiers would not survive a migration.
inline bool is_valid_object_name(std::string_view name) {
    if (name.empty() || name.size() > 255) return false;
    if (name.front() == '.') return false;
    return name.find('/') == std::string_view::npos &&
           name.find('\\') == std::string_view::npos &&
           name.find('\0') == std::string_view::npos;
}

}  // namespace elysiumkv

#endif  // ELYSIUMKV_BLOB_OBJECT_NAME_HPP
