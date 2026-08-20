#ifndef ELYSIUMKV_NO_ENCRYPTION_PROVIDER_HPP
#define ELYSIUMKV_NO_ENCRYPTION_PROVIDER_HPP

#include "elysiumkv/encryption.hpp"

namespace elysiumkv {

/// The provider that does nothing: the plaintext is the ciphertext.
///
/// Registered by the engine under the reserved empty id and always present, so there is no
/// unconfigured state and no null provider anywhere below the boundary. A store with encryption
/// switched off is this provider being primary, and a file written before encryption existed
/// records the same empty id — one case rather than two, and no branch in the read path.
///
/// Freely constructible: it holds no state, and nothing depends on there being a single instance.
///
/// Run like any other provider, not routed around. The engine resolves the empty id through the
/// same map lookup as every other id and calls this cipher, so there is one path through the read
/// and write paths rather than a fast one and a general one. Measured against a special case that
/// skips it, the difference on an unencrypted store is within run-to-run noise — a point lookup is
/// dominated by the block read and its checksum, not by a copy — so the uniformity is free.
class NoEncryptionProvider final : public EncryptionProvider {
public:
    Result<NewObject> create(uint64_t object_id) override;
    Result<std::shared_ptr<ObjectCipher>> open(uint64_t object_id, Slice metadata) override;
};

}  // namespace elysiumkv

#endif  // ELYSIUMKV_NO_ENCRYPTION_PROVIDER_HPP
