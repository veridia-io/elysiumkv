/* The engine's half of the encryption boundary: the mapping between the offsets a reader asks at
 * and the offsets the bytes live at.
 *
 * **This is where the bugs are.** The cryptography is a library call; the arithmetic is ours, it
 * runs on every read, and an off-by-one in it produces plausible-looking bytes rather than an
 * error. So the ranged-read case is checked exhaustively rather than at a few interesting offsets:
 * every start, every length, across every chunk boundary.
 */

#include "crypt/encrypted_object.hpp"

#include "elysiumkv/disk_blob_store.hpp"
#include "elysiumkv/aes256_gcm_encryption_provider.hpp"
#include "elysiumkv/no_encryption_provider.hpp"
#include "support/temp_dir.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace elysiumkv::test {
namespace {

class KeyManagerStub final : public EncryptionKeyManager {
public:
    Result<DataKey> new_data_key() override {
        std::string material(32, '\0');
        for (size_t i = 0; i < material.size(); ++i) {
            material[i] = static_cast<char>(next_ * 7 + static_cast<int>(i));
        }
        ++next_;
        DataKey key;
        key.key = SecretKey(reinterpret_cast<const uint8_t*>(material.data()), material.size());
        key.envelope = material;
        return key;
    }
    Result<SecretKey> open_data_key(Slice envelope) override {
        return SecretKey(envelope.data(), envelope.size());
    }

private:
    int next_ = 1;
};

std::string pattern(size_t size) {
    std::string out(size, '\0');
    for (size_t i = 0; i < size; ++i) out[i] = static_cast<char>((i * 131 + i / 251) & 0xFF);
    return out;
}

std::string as_string(const Buffer& b) {
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

class EncryptedObjectTest : public ::testing::Test {
protected:
    static constexpr const char* kName = "000000000042.sst";
    static constexpr uint64_t kObjectId = 42;

    void SetUp() override {
        std::filesystem::create_directories(dir_.path() / "store");
        store_ = std::make_shared<DiskBlobStore>(dir_.path() / "store", "store-a");
        store_->set_sync_writes(false);

        auto made = Aes256GcmEncryptionProvider::open(std::make_shared<KeyManagerStub>(), kChunk);
        ASSERT_TRUE(made.has_value());
        provider_ = *made;
    }

    /// Writes `plaintext` encrypted, and returns a view that reads it back.
    std::unique_ptr<EncryptedObject> write(const std::string& plaintext) {
        auto created = provider_->create(kObjectId);
        EXPECT_TRUE(created.has_value());
        metadata_ = created->metadata;

        EncryptedObject writer(*store_, created->cipher, kName, plaintext.size());
        EXPECT_EQ(writer.put(kName, Slice::from(plaintext)).get(), Status::Ok);

        auto cipher = provider_->open(kObjectId, Slice::from(metadata_));
        EXPECT_TRUE(cipher.has_value());
        return std::make_unique<EncryptedObject>(*store_, *cipher, kName, plaintext.size());
    }

    static constexpr size_t kChunk = 64;   // small, so a modest object spans many chunks

    TempDir dir_;
    std::shared_ptr<DiskBlobStore> store_;
    std::shared_ptr<EncryptionProvider> provider_;
    std::string metadata_;
};

TEST_F(EncryptedObjectTest, ThePhysicalObjectIsLongerByExactlyOneTagPerChunk) {
    const std::string plaintext = pattern(200);   // 4 chunks at 64: 64+64+64+8
    auto view = write(plaintext);

    // Read straight from the store, below the boundary, to see what was really written.
    auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(raw.has_value());
    EXPECT_EQ(raw->size(), 200u + 4u * 16u);
    EXPECT_EQ(raw->size(), EncryptedObject::physical_size(200, kChunk, 16));

    // And it is not the plaintext.
    EXPECT_EQ(as_string(*raw).find(plaintext.substr(0, 32)), std::string::npos)
        << "the plaintext must not be findable in what was stored";
}

TEST_F(EncryptedObjectTest, AnEmptyObjectOccupiesNothing) {
    auto view = write(std::string());
    auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(raw.has_value());
    EXPECT_EQ(raw->size(), 0u) << "no bytes, so no chunks, so no tags";

    auto read = view->get_sync(kName, 0, BlobStore::kReadToEnd);
    ASSERT_TRUE(read.has_value());
    EXPECT_TRUE(read->empty());
}

/// **The exhaustive one.** Every offset and every length over an object spanning several chunks,
/// including reads that begin and end mid-chunk and those that run past the end.
TEST_F(EncryptedObjectTest, EveryRangedReadReturnsExactlyTheRightBytes) {
    const std::string plaintext = pattern(300);   // 5 chunks, last one short
    auto view = write(plaintext);

    for (size_t offset = 0; offset <= plaintext.size(); ++offset) {
        for (size_t len = 0; len <= plaintext.size() - offset + 8; ++len) {
            auto read = view->get_sync(kName, offset, len);
            ASSERT_TRUE(read.has_value()) << "offset " << offset << " len " << len;

            const size_t expected = std::min(len, plaintext.size() - offset);
            ASSERT_EQ(read->size(), expected) << "offset " << offset << " len " << len;
            EXPECT_EQ(as_string(*read), plaintext.substr(offset, expected))
                << "offset " << offset << " len " << len;
        }
    }
}

TEST_F(EncryptedObjectTest, AReadToTheEndAndPastItBehaveLikeAnyOtherStore) {
    const std::string plaintext = pattern(150);
    auto view = write(plaintext);

    auto whole = view->get_sync(kName, 0, BlobStore::kReadToEnd);
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(as_string(*whole), plaintext);

    auto tail = view->get_sync(kName, 100, BlobStore::kReadToEnd);
    ASSERT_TRUE(tail.has_value());
    EXPECT_EQ(as_string(*tail), plaintext.substr(100));

    // At or past the end is an empty answer, never an error.
    auto at_end = view->get_sync(kName, 150, 10);
    ASSERT_TRUE(at_end.has_value());
    EXPECT_TRUE(at_end->empty());
    auto beyond = view->get_sync(kName, 9999, 10);
    ASSERT_TRUE(beyond.has_value());
    EXPECT_TRUE(beyond->empty());
}

/// An object whose length is an exact multiple of the chunk size has no short final chunk, which is
/// the boundary the length arithmetic is most likely to get wrong in the other direction.
TEST_F(EncryptedObjectTest, AnObjectThatEndsOnAChunkBoundaryReadsCorrectly) {
    const std::string plaintext = pattern(kChunk * 3);
    auto view = write(plaintext);

    auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(raw.has_value());
    EXPECT_EQ(raw->size(), kChunk * 3 + 3 * 16);

    auto whole = view->get_sync(kName, 0, BlobStore::kReadToEnd);
    ASSERT_TRUE(whole.has_value());
    EXPECT_EQ(as_string(*whole), plaintext);

    auto last = view->get_sync(kName, kChunk * 2, kChunk);
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(as_string(*last), plaintext.substr(kChunk * 2));
}

TEST_F(EncryptedObjectTest, DamageAnywhereIsReportedRatherThanReturned) {
    const std::string plaintext = pattern(200);
    auto view = write(plaintext);

    auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(raw.has_value());
    std::string damaged = as_string(*raw);
    damaged[100] = static_cast<char>(damaged[100] ^ 0x01);

    ASSERT_EQ(store_->remove(kName).get(), Status::Ok);
    ASSERT_EQ(store_->put(kName, Slice::from(damaged)).get(), Status::Ok);

    // The damaged byte is in chunk 1, so a read of chunk 0 still succeeds — damage is contained to
    // the chunk it is in, which is what per-chunk authentication buys.
    auto clean = view->get_sync(kName, 0, 32);
    ASSERT_TRUE(clean.has_value());
    EXPECT_EQ(as_string(*clean), plaintext.substr(0, 32));

    EXPECT_EQ(view->get_sync(kName, 100, 32).error(), Status::Corrupt);
}

/// Truncation is a claim about an object's length, and the claim is authenticated. Without the
/// length in chunk zero's associated data this read would succeed and quietly return a prefix.
TEST_F(EncryptedObjectTest, ATruncatedObjectIsRefusedRatherThanReadAsShorter) {
    const std::string plaintext = pattern(300);
    auto view = write(plaintext);

    auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(raw.has_value());
    // Drop the final chunk entirely, then present the object as if it were that much shorter.
    const std::string shortened = as_string(*raw).substr(0, 4 * (kChunk + 16));
    ASSERT_EQ(store_->remove(kName).get(), Status::Ok);
    ASSERT_EQ(store_->put(kName, Slice::from(shortened)).get(), Status::Ok);

    auto cipher = provider_->open(kObjectId, Slice::from(metadata_));
    ASSERT_TRUE(cipher.has_value());
    EncryptedObject lying(*store_, *cipher, kName, 4 * kChunk);

    EXPECT_EQ(lying.get_sync(kName, 0, 16).error(), Status::Corrupt)
        << "the length was sealed into chunk zero, so a different one cannot be presented";
}

/// A chunk moved from elsewhere in the same object fails, because the chunk index is authenticated.
TEST_F(EncryptedObjectTest, AChunkMovedWithinTheObjectIsRefused) {
    const std::string plaintext = pattern(kChunk * 4);
    auto view = write(plaintext);

    auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
    ASSERT_TRUE(raw.has_value());
    std::string swapped = as_string(*raw);
    const size_t stride = kChunk + 16;
    std::string chunk1 = swapped.substr(stride, stride);
    std::string chunk2 = swapped.substr(2 * stride, stride);
    swapped.replace(stride, stride, chunk2);
    swapped.replace(2 * stride, stride, chunk1);

    ASSERT_EQ(store_->remove(kName).get(), Status::Ok);
    ASSERT_EQ(store_->put(kName, Slice::from(swapped)).get(), Status::Ok);

    EXPECT_EQ(view->get_sync(kName, kChunk, 8).error(), Status::Corrupt);
}

/// One view serves one object. Being asked for another is a wiring error, and answering it from the
/// wrong key would be worse than refusing.
TEST_F(EncryptedObjectTest, AViewRefusesAnObjectThatIsNotItsOwn) {
    auto view = write(pattern(64));
    EXPECT_EQ(view->get_sync("000000000099.sst", 0, 8).error(), Status::Config);
}

TEST_F(EncryptedObjectTest, ThePhysicalSizeFormulaMatchesWhatIsWritten) {
    for (uint64_t logical : {uint64_t{0}, uint64_t{1}, uint64_t{63}, uint64_t{64}, uint64_t{65},
                             uint64_t{1000}}) {
        auto view = write(pattern(static_cast<size_t>(logical)));
        auto raw = store_->get(kName, 0, BlobStore::kReadToEnd).get();
        ASSERT_TRUE(raw.has_value()) << logical;
        EXPECT_EQ(raw->size(), EncryptedObject::physical_size(logical, kChunk, 16)) << logical;
        ASSERT_EQ(store_->remove(kName).get(), Status::Ok);
    }
}

}  // namespace
}  // namespace elysiumkv::test
