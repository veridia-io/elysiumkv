package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The geometric level layout through the binding.
 *
 * <p>The helper it calls had no caller anywhere — not in the engine, not in the tests, not in
 * either binding — so a shape embedders are invited to use was never once produced. Reaching it
 * from Java is what makes it real rather than merely offered.
 */
class GeometricLevelsTest {

    @Test
    void aGeometricStoreOpensAndRoundTrips(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options().geometricLevels(64L << 10, 10, 4);
            ElysiumKV db = PinLeakExtension.watch(support.own(ElysiumKV.open(options)));

            for (int i = 0; i < 400; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("v" + i));
            }
            db.flush();

            for (int i = 0; i < 400; ++i) {
                try (Pinned found = db.get(TestSupport.key(i))) {
                    assertEquals("v" + i, TestSupport.string(found.toByteArray()));
                }
            }

            // Four levels configured, and the layout is what the stats report — the levels are the
            // ones the engine resolved, not the ones the caller believes it asked for.
            assertEquals(4, db.stats().levels().size());
            db.close();
        }
    }

    /**
     * Rejected in Java rather than at open, because these are arithmetic mistakes rather than
     * configurations: a multiplier below two produces a layout that never grows, and one level is
     * not a hierarchy.
     */
    @Test
    void nonsenseIsRefusedBeforeItReachesTheEngine(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options = support.options();
            assertThrows(IllegalArgumentException.class, () -> options.geometricLevels(0, 10, 4));
            assertThrows(IllegalArgumentException.class, () -> options.geometricLevels(1024, 1, 4));
            assertThrows(IllegalArgumentException.class, () -> options.geometricLevels(1024, 10, 1));
            assertTrue(options.geometricLevels(1024, 2, 2) == options,
                       "the fluent setter returns itself");
        }
    }
}
