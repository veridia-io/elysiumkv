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

    /** The reserved id belongs to the passthrough, and a null manager is not a configuration. */
    @Test
    void badConfigurationIsRefusedBeforeItReachesTheEngine(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options();
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("", new DirectKeys(), 0));
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("id", null, 0));
            assertThrows(IllegalArgumentException.class,
                         () -> options.encryptWith("id", new DirectKeys(), -1));
        }
    }
}
