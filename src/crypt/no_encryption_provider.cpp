#include "elysiumkv/no_encryption_provider.hpp"

namespace elysiumkv {
namespace {

/// Identity, and genuinely on the read path: the boundary runs this exactly as it runs a real
/// cipher. Zero overhead is what makes the physical layout equal to the logical one, so the
/// offset mapping above degenerates to the identity rather than being skipped.
class NoEncryptionCipher final : public ObjectCipher {
public:
    size_t chunk_bytes() const override { return 4096; }
    size_t overhead_bytes() const override { return 0; }
    uint64_t object_id() const override { return 0; }

    Status seal(uint64_t, Slice plaintext, Slice, std::string& out) override {
        out.append(reinterpret_cast<const char*>(plaintext.data()), plaintext.size());
        return Status::Ok;
    }
    Status open(uint64_t, Slice ciphertext, Slice, std::string& out) override {
        out.append(reinterpret_cast<const char*>(ciphertext.data()), ciphertext.size());
        return Status::Ok;
    }
};

}  // namespace

Result<NewObject> NoEncryptionProvider::create(uint64_t) {
    return NewObject{std::make_shared<NoEncryptionCipher>(), std::string()};
}

Result<std::shared_ptr<ObjectCipher>> NoEncryptionProvider::open(uint64_t, Slice metadata) {
    // Nothing wrote metadata under this provider, so anything here came from somewhere else — a
    // file whose recorded id was lost, or a registry that routed wrongly. Either way these are not
    // bytes it can serve.
    if (!metadata.empty()) return std::unexpected(Status::Corrupt);
    return std::shared_ptr<ObjectCipher>(std::make_shared<NoEncryptionCipher>());
}

Result<ObjectLayout> NoEncryptionProvider::layout(Slice metadata) {
    if (!metadata.empty()) return std::unexpected(Status::Corrupt);
    return ObjectLayout{4096, 0};
}

}  // namespace elysiumkv
