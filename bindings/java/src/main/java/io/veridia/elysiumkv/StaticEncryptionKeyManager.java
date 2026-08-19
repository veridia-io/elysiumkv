package io.veridia.elysiumkv;

import java.util.Arrays;

/**
 * One master key held in this process, wrapping a fresh data key per object.
 *
 * <p><b>Still a fresh key per object</b>, because the engine derives its nonces from the chunk
 * index: one key across every object would repeat nonces and break GCM outright. So choosing this
 * over {@link AwsKmsEncryptionKeyManager} is a decision about key custody, not about strength.
 *
 * <p>Suitable where the master key arrives from a secrets manager at startup. Where it must never
 * enter the process at all, use KMS.
 *
 * <p><b>The array you pass cannot be wiped for you.</b> It is copied into native storage that is
 * zeroed deterministically, and this object retains no copy past construction — but a {@code byte[]}
 * is collector-owned and a moving collector may already have left copies behind. Wipe your own
 * array when you are done with it, and understand that this is best-effort in a JVM.
 */
public final class StaticEncryptionKeyManager extends BuiltinEncryptionKeyManager {
    private final byte[] masterKey;

    private StaticEncryptionKeyManager(byte[] masterKey) {
        this.masterKey = masterKey;
    }

    /** @param masterKey exactly 32 bytes; copied, and the caller's array is left untouched. */
    public static StaticEncryptionKeyManager of(byte[] masterKey) {
        if (masterKey == null || masterKey.length != 32) {
            throw new IllegalArgumentException("the master key must be exactly 32 bytes");
        }
        return new StaticEncryptionKeyManager(Arrays.copyOf(masterKey, masterKey.length));
    }

    /** The same key as 64 hex characters, which is how one usually arrives from a secrets store. */
    public static StaticEncryptionKeyManager fromHex(String hex) {
        if (hex == null || hex.length() != 64) {
            throw new IllegalArgumentException("the master key must be 64 hex characters");
        }
        byte[] key = new byte[32];
        for (int i = 0; i < key.length; i++) {
            final int high = Character.digit(hex.charAt(2 * i), 16);
            final int low = Character.digit(hex.charAt(2 * i + 1), 16);
            if (high < 0 || low < 0) {
                Arrays.fill(key, (byte) 0);
                throw new IllegalArgumentException("the master key is not valid hex");
            }
            key[i] = (byte) ((high << 4) | low);
        }
        return new StaticEncryptionKeyManager(key);
    }

    @Override
    void register(long optionsHandle, String id, long chunkBytes) {
        Native.optionsAddAes256GcmEncryptionWithStaticKey(optionsHandle, id, masterKey, chunkBytes);
    }
}
