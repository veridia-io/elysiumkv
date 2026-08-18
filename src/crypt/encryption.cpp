#include "elysiumkv/encryption.hpp"

#include <openssl/crypto.h>

namespace elysiumkv {

SecretKey::~SecretKey() {
    // **`OPENSSL_cleanse`, not a loop the optimiser may delete.** Zeroing a buffer nothing reads
    // afterwards is exactly the store a compiler is allowed to remove, and it does.
    if (!bytes_.empty()) OPENSSL_cleanse(bytes_.data(), bytes_.size());
}

void secure_zero(void* data, size_t size) {
    if (data != nullptr && size != 0) OPENSSL_cleanse(data, size);
}

}  // namespace elysiumkv
