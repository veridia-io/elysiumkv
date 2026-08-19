package io.veridia.elysiumkv;

/**
 * A key manager the engine already implements, configured rather than written.
 *
 * <p><b>The alternative to {@link EncryptionKeyManager}, not a kind of it.</b> That interface is a
 * seam you fill; these are native-side, so no per-object call crosses back into the JVM and no key
 * material passes through a Java callback. Where both would do, prefer these: the envelope layout
 * is then produced by one implementation rather than two that must agree byte for byte.
 *
 * <p>Pass one to {@link ElysiumKVOptions#encryptWith(String, BuiltinEncryptionKeyManager, long)}.
 *
 * <p>Closed by construction — the constructor is package-private, so the set is
 * {@link StaticEncryptionKeyManager} and {@link AwsKmsEncryptionKeyManager}. Not {@code sealed}
 * only because this binding compiles at Java 11.
 */
public abstract class BuiltinEncryptionKeyManager {

    BuiltinEncryptionKeyManager() {}

    /** Registers this manager under {@code id}. Package-private: the handle is not the caller's. */
    abstract void register(long optionsHandle, String id, long chunkBytes);
}
