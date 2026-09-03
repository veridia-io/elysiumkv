package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Encryption as a binding sees it. What the cipher does is settled by the C++ suite; what is settled
 * here is that a key manager written in Java is actually reached from engine threads, and that the
 * bytes on disk are not the ones that were written.
 *
 * <p>The disk check is the point. A round trip passes against a build that called the manager and
 * then encrypted nothing, which is the failure a callback seam invites: the callbacks fire, so it
 * looks wired.
 */
class EncryptionTest {

    /** The envelope is the key. A KMS would add a network and prove nothing more about the seam. */
    static final class DirectKeys implements EncryptionKeyManager {
        final AtomicInteger issued = new AtomicInteger();
        final AtomicInteger opened = new AtomicInteger();

        @Override
        public byte[][] newDataKey() {
            byte[] key = new byte[32];
            int n = issued.incrementAndGet();
            for (int i = 0; i < key.length; ++i) key[i] = (byte) (n * 31 + i);
            return new byte[][] {key, key.clone()};
        }

        @Override
        public byte[] openDataKey(byte[] envelope) {
            opened.incrementAndGet();
            return envelope.clone();
        }
    }

    private static final String CANARY = "CANARY-THROUGH-JNI-0123456789abcdef";

    @Test
    void aJavaKeyManagerEncryptsTheStore(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            DirectKeys keys = new DirectKeys();
            ElysiumKV db = PinLeakExtension.watch(support.own(
                    ElysiumKV.open(support.options().encryptWith("java-kms", keys, 0))));

            for (int i = 0; i < 200; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes(CANARY + i));
            }
            db.flush();
            db.compactLevel(0);
            assertTrue(keys.issued.get() > 0, "the Java manager was asked for a key");

            for (int i = 0; i < 200; ++i) {
                try (Pinned found = db.get(TestSupport.key(i))) {
                    assertEquals(CANARY + i, TestSupport.string(found.toByteArray()));
                }
            }

            // Nothing on disk is the canary.
            try (Stream<Path> files = Files.walk(dir)) {
                java.util.List<Path> regular =
                        files.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toList());
                for (Path file : regular) {
                    String contents =
                            new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
                    assertTrue(contents.indexOf(CANARY) < 0, "plaintext found in " + file);
                }
            }
            db.close();
        }
    }

    /** Reopening is what exercises {@code openDataKey}: the first run's readers are gone. */
    @Test
    void reopeningUnwrapsThroughTheJavaManager(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            DirectKeys first = new DirectKeys();
            ElysiumKV db = support.own(
                    ElysiumKV.open(support.options().encryptWith("java-kms", first, 0)));
            for (int i = 0; i < 50; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes(CANARY + i));
            }
            db.flush();
            db.close();

            DirectKeys second = new DirectKeys();
            ElysiumKV reopened = PinLeakExtension.watch(support.own(
                    ElysiumKV.open(support.options().encryptWith("java-kms", second, 0))));
            for (int i = 0; i < 50; ++i) {
                try (Pinned found = reopened.get(TestSupport.key(i))) {
                    assertEquals(CANARY + i, TestSupport.string(found.toByteArray()));
                }
            }
            assertTrue(second.opened.get() > 0, "the envelope was unwrapped through Java");
            reopened.close();
        }
    }

    @Test
    void repeatedKeyCallbacksStayInsideTheirJniLocalFrame(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            DirectKeys keys = new DirectKeys();
            ElysiumKV db = PinLeakExtension.watch(support.own(
                    ElysiumKV.open(support.options().encryptWith("java-kms", keys, 0))));

            for (int i = 0; i < 64; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("v"));
                db.flush();
            }
            assertTrue(keys.issued.get() >= 64, "the checked-JNI gate needs repeated callbacks");
            db.close();
        }
    }

    private static final String MASTER_HEX =
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    private static byte[] masterKey() {
        byte[] key = new byte[32];
        for (int i = 0; i < key.length; ++i) key[i] = (byte) i;
        return key;
    }

    /**
     * The built-in manager, end to end: the same disk check as the Java-implemented one, because
     * "the call did not throw" is equally consistent with a store that encrypted nothing.
     */
    @Test
    void theStaticKeyManagerEncryptsTheStore(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.own(ElysiumKV.open(
                    support.options().encryptWith("static",
                                                  StaticEncryptionKeyManager.of(masterKey()), 0))));
            for (int i = 0; i < 200; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes(CANARY + i));
            }
            db.flush();
            db.compactLevel(0);
            for (int i = 0; i < 200; ++i) {
                try (Pinned found = db.get(TestSupport.key(i))) {
                    assertEquals(CANARY + i, TestSupport.string(found.toByteArray()));
                }
            }
            assertNoPlaintextUnder(dir);
            db.close();
        }
    }

    /**
     * Reopening is what exercises the unwrap, and the two spellings of one key have to reach the
     * same provider — a store written through {@code of} that {@code fromHex} could not read would
     * be a parser bug nothing else here would catch.
     */
    @Test
    void aStaticKeyStoreReopensUnderEitherSpellingOfTheKey(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.own(ElysiumKV.open(
                    support.options().encryptWith("static",
                                                  StaticEncryptionKeyManager.of(masterKey()), 0)));
            for (int i = 0; i < 50; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes(CANARY + i));
            }
            db.flush();
            db.close();

            ElysiumKV reopened = PinLeakExtension.watch(support.own(ElysiumKV.open(
                    support.options().encryptWith("static",
                                                  StaticEncryptionKeyManager.fromHex(MASTER_HEX),
                                                  0))));
            for (int i = 0; i < 50; ++i) {
                try (Pinned found = reopened.get(TestSupport.key(i))) {
                    assertEquals(CANARY + i, TestSupport.string(found.toByteArray()));
                }
            }
            reopened.close();
        }
    }

    /**
     * The wrong master key must fail, and fail as configuration. An engine that served
     * something anyway would be the worst outcome available here, so this is asserted rather than
     * assumed from the cipher's C++ tests.
     *
     * <p>It fails at {@code open}, not at the first read, because the manifest is encrypted too —
     * so the mistake is caught before the store is usable rather than by whichever query happened
     * to touch an encrypted file first.
     */
    @Test
    void anotherMasterKeyCannotReadTheStore(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.own(ElysiumKV.open(
                    support.options().encryptWith("static",
                                                  StaticEncryptionKeyManager.of(masterKey()), 0)));
            db.put(TestSupport.key(1), TestSupport.bytes(CANARY));
            db.flush();
            db.close();

            byte[] wrong = masterKey();
            wrong[0] ^= 0x01;
            ElysiumKVException thrown = assertThrows(
                    ElysiumKVException.class,
                    () -> ElysiumKV.open(support.options().encryptWith(
                            "static", StaticEncryptionKeyManager.of(wrong), 0)));
            assertTrue(thrown instanceof ConfigException,
                       () -> "a key that cannot unwrap is a configuration to fix, not damage to "
                               + "restore from: " + thrown);
            // The status alone would leave an operator bisecting their options; the binding has to
            // carry the engine's explanation across the boundary too.
            assertTrue(thrown.getMessage().contains("static"),
                       () -> "the message must name the provider involved: " + thrown.getMessage());
        }
    }

    private static void assertNoPlaintextUnder(Path dir) throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            java.util.List<Path> regular =
                    files.filter(Files::isRegularFile).collect(java.util.stream.Collectors.toList());
            for (Path file : regular) {
                String contents = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
                assertTrue(contents.indexOf(CANARY) < 0, "plaintext found in " + file);
            }
        }
    }

    /** The reserved id belongs to the passthrough, and a null manager is not a configuration. */
    @Test
    void badConfigurationIsRefusedBeforeItReachesTheEngine(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options();
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("", new DirectKeys(), 0));
            // The cast is required, not decorative: encryptWith is overloaded on the two kinds of
            // key manager, so a bare null names neither.
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("id", (EncryptionKeyManager) null, 0));
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("id", new DirectKeys(), -1));

            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("", StaticEncryptionKeyManager.of(new byte[32]),
                                                   0));
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("id", (BuiltinEncryptionKeyManager) null, 0));
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("id", StaticEncryptionKeyManager.of(new byte[32]),
                                                   -1));
            assertThrows(IllegalArgumentException.class,
                         () -> StaticEncryptionKeyManager.of(new byte[31]));
            assertThrows(IllegalArgumentException.class,
                         () -> StaticEncryptionKeyManager.fromHex("nothex"));
            assertThrows(IllegalArgumentException.class,
                         () -> AwsKmsEncryptionKeyManager.builder(""));
        }
    }
}
