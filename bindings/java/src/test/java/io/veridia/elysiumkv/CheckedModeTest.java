package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * What {@code paranoidChecks} buys on the Java side.
 *
 * <p>Under Panama FFM a confined {@code Arena} would give both of these for
 * free. JNI hands back a direct {@code ByteBuffer} whose address cannot be
 * revoked, so the checks are rebuilt by hand — and they reach {@link
 * Pinned#value()}, not a reference the caller already holds. That limit is real
 * and is documented rather than papered over.
 */
@ExtendWith(PinLeakExtension.class)
class CheckedModeTest {
    @Test
    void usingAPinAfterCloseIsRefused(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));

            Pinned pinned = db.get(TestSupport.bytes("k"));
            assertNotNull(pinned);
            assertNotNull(pinned.value());
            pinned.close();

            assertThrows(IllegalStateException.class, pinned::value);
            assertThrows(IllegalStateException.class, pinned::toByteArray);
            pinned.close();   // idempotent, so try-with-resources after an explicit close is fine
            db.close();
        }
    }

    @Test
    void usingAPinFromAnotherThreadIsRefused(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));

            try (Pinned pinned = db.get(TestSupport.bytes("k"))) {
                assertNotNull(pinned);
                AtomicReference<Throwable> caught = new AtomicReference<>();
                Thread other = new Thread(() -> {
                    try {
                        pinned.value();
                    } catch (Throwable t) {
                        caught.set(t);
                    }
                });
                other.start();
                other.join();
                assertNotNull(caught.get(), "a pin must not be readable from another thread");
                assertTrue(caught.get() instanceof IllegalStateException);
            }
            db.close();
        }
    }

    @Test
    void closingWithAPinOutstandingFails(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.open();
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));

            Pinned leaked = db.get(TestSupport.bytes("k"));
            assertNotNull(leaked);
            assertEquals(1, db.pinsOutstanding());

            // Deliberately not closed. In checked mode that is a test failure,
            // because a leaked pin holds a block-cache entry indefinitely.
            IllegalStateException thrown = assertThrows(IllegalStateException.class, db::close);
            assertTrue(thrown.getMessage().contains("outstanding"), thrown.getMessage());
        }
    }

    @Test
    void anIteratorSurvivesTheDatabaseClosing(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));

            ElysiumKVIterator iterator = db.iterator(null, null);
            assertTrue(iterator.next());

            // The C ABI detaches live iterators instead of freeing them, so
            // destroying the handle afterwards is safe rather than a
            // use-after-free — which is what it was until ASan said otherwise.
            assertEquals(1, db.closeReportingOutstanding());
            iterator.close();
        }
    }
}
