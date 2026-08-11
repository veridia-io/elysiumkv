package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Java end of the watermark. A capability the C++ API can reach and a binding cannot is the
 * asymmetry this project keeps having to remove, so the binding asserts the behaviour rather than
 * merely declaring the methods.
 *
 * <p>These are also the two calls a KIP-1035 store-managed offset is made of:
 * {@code commit(offsets)} is {@code setWatermark} then {@code flush}, and
 * {@code committedOffset()} is {@code recoveredWatermark}.
 */
@ExtendWith(PinLeakExtension.class)
class WatermarkTest {
    @Test
    void aFlushedWatermarkComesBackAfterReopen(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            assertEquals(OptionalLong.empty(), db.recoveredWatermark(),
                         "a fresh store certifies nothing");

            db.put(TestSupport.key(1), TestSupport.bytes("v"));
            db.setWatermark(4242L);
            db.flush();
            db.close();

            ElysiumKV reopened = PinLeakExtension.watch(support.open());
            assertEquals(OptionalLong.of(4242L), reopened.recoveredWatermark());
            assertEquals("v", TestSupport.string(reopened.getCopy(TestSupport.key(1))));
            reopened.close();
        }
    }

    /**
     * The control for the one above: without the flush the watermark is not durable. There is no
     * write-ahead log, so an unflushed memtable is lost — the watermark says where to resume, it
     * does not reduce what was lost.
     *
     * <p><b>{@link ElysiumKV#closeWithoutFlush()} is what keeps this a control.</b> A plain
     * {@code close()} now attempts a flush, so it would save the very watermark this case asserts is
     * not durable, and the test would pass while checking nothing.
     */
    @Test
    void aWatermarkThatWasNeverFlushedIsNotReported(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.key(1), TestSupport.bytes("v"));
            db.setWatermark(10L);
            db.flush();

            db.put(TestSupport.key(2), TestSupport.bytes("v"));
            db.setWatermark(20L);   // deliberately not flushed
            db.closeWithoutFlush();  // ...and deliberately not saved on the way out

            ElysiumKV reopened = PinLeakExtension.watch(support.open());
            assertEquals(OptionalLong.of(10L), reopened.recoveredWatermark(),
                         "the unflushed watermark must not be reported as durable");
            reopened.close();
        }
    }

    /** Zero is a valid position, and must not read as absence. */
    @Test
    void zeroIsAPositionAndNotAbsence(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.key(1), TestSupport.bytes("v"));
            db.setWatermark(0L);
            db.flush();
            db.close();

            ElysiumKV reopened = PinLeakExtension.watch(support.open());
            OptionalLong recovered = reopened.recoveredWatermark();
            assertTrue(recovered.isPresent(), "zero is a watermark, not the absence of one");
            assertEquals(0L, recovered.getAsLong());
            reopened.close();
        }
    }

    @Test
    void aDecreasingWatermarkIsRefused(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.setWatermark(100L);
            // Refused rather than clamped: clamping would hide a replay that went backwards.
            assertThrows(ConfigException.class, () -> db.setWatermark(99L));
            db.setWatermark(100L);   // equal is not decreasing
            db.close();
        }
    }

    /** The live frontier travels through the stats buffer, and is absent until one is set. */
    @Test
    void theLiveFrontierIsExportedThroughStats(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            assertFalse(db.stats().durableWatermark().isPresent(),
                        "absent, not zero — zero is a valid position");

            db.put(TestSupport.key(1), TestSupport.bytes("v"));
            db.setWatermark(7L);
            db.flush();

            assertEquals(OptionalLong.of(7L), db.stats().durableWatermark(),
                         "all-durable configuration, so the frontier is the newest upper bound");
            assertTrue(db.stats().flushes() >= 1, "the flush counter is exported too");
            db.close();
        }
    }
}
