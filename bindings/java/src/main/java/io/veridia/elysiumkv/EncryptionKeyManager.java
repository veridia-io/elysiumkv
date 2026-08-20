package io.veridia.elysiumkv;

/**
 * Wrapping and unwrapping data keys. The engine owns the cryptography; you own the key
 * custody — which is the split that keeps a KMS integration from becoming a cipher integration.
 *
 * <p>Called once per object rather than per read, and {@link #openDataKey} results are
 * cached, so a KMS round trip here is affordable. It is called from engine background threads
 * (flush, compaction, migration), so an implementation must be thread-safe.
 *
 * <p>Key material handed to the engine cannot be wiped by you. A {@code byte[]} is
 * collector-owned and may already have been copied by a moving collector, so a Java-held key
 * survives in memory for as long as the collector chooses. The engine copies it into storage it
 * zeroes deterministically and does not retain the array; what it cannot do is undo the copies the
 * JVM already made. If that is unacceptable, supply the provider through the C ABI instead.
 */
public interface EncryptionKeyManager {

    /**
     * A fresh data key. Returns exactly two arrays: the 32-byte plaintext key, then the wrapped
     * form to persist beside the object.
     *
     * <p>Throwing is reported to the engine as an I/O failure, which is retryable — appropriate for
     * a KMS that is briefly unreachable.
     */
    byte[][] newDataKey();

    /**
     * The plaintext key for an envelope this manager produced.
     *
     * <p>Throwing is reported as a configuration failure rather than corruption: a key that cannot
     * be unwrapped means the wrong manager is configured, and the remedy is the right one rather
     * than a restore.
     */
    byte[] openDataKey(byte[] envelope);
}
