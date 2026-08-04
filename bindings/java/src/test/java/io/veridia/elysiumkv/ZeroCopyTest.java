package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * The claim the whole native core rests on: <b>a lookup does not copy the
 * value.</b> ARCHITECTURE.md "Benchmarks" states the check exactly — "if the <i>value</i> path shows a
 * copy, the pin protocol is not doing its job."
 *
 * <p>Timing cannot show this reliably; allocation can. A copying path must
 * allocate at least the value's length, so measuring bytes allocated per
 * operation against two very different value sizes separates the two designs
 * with no dependence on machine speed. The pinned path allocates a fixed handful
 * of small objects — {@code long[1]}, a {@code Pinned}, two buffer views — no
 * matter how large the value is.
 */
@ExtendWith(PinLeakExtension.class)
class ZeroCopyTest {
    private static final int SMALL = 1024;
    private static final int LARGE = 256 * 1024;   // inside the engine's readable bound; see LargeValue
    private static final int OPERATIONS = 200;

    @Test
    void pinnedReadsDoNotScaleWithValueSize(@TempDir Path dir) throws Exception {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        assumeTrue(bean instanceof com.sun.management.ThreadMXBean,
                   "allocation accounting needs a HotSpot-style ThreadMXBean");
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) bean;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());

        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            byte[] smallKey = TestSupport.bytes("small");
            byte[] largeKey = TestSupport.bytes("large");
            db.put(smallKey, filled(SMALL));
            db.put(largeKey, filled(LARGE));
            db.flush();

            // Warm the cache and the JIT before measuring.
            for (int i = 0; i < OPERATIONS; ++i) {
                try (Pinned p = db.get(smallKey)) { assertNotNull(p); }
                try (Pinned p = db.get(largeKey)) { assertNotNull(p); }
            }

            long pinnedSmall = perOperation(allocation, () -> {
                try (Pinned p = db.get(smallKey)) { return p.size(); }
            });
            long pinnedLarge = perOperation(allocation, () -> {
                try (Pinned p = db.get(largeKey)) { return p.size(); }
            });

            // A 256x larger value through the same path. If anything copied,
            // this number would grow with it.
            assertTrue(pinnedLarge < 2048,
                       () -> "a pinned 256 KiB read allocated " + pinnedLarge
                               + " bytes per operation; the value was copied");
            assertTrue(pinnedLarge < pinnedSmall * 4,
                       () -> "pinned allocation scaled with value size: " + pinnedSmall + " -> "
                               + pinnedLarge);

            // The control: getCopy must scale, or the measurement is not
            // measuring anything.
            long copiedLarge = perOperation(allocation, () -> db.getCopy(largeKey).length);
            assertTrue(copiedLarge > LARGE,
                       () -> "getCopy allocated only " + copiedLarge
                               + " bytes for a 256 KiB value — the check is vacuous");
        }
    }

    private interface Operation {
        int run();
    }

    private static long perOperation(com.sun.management.ThreadMXBean allocation, Operation op) {
        long id = Thread.currentThread().getId();
        long before = allocation.getThreadAllocatedBytes(id);
        int sink = 0;
        for (int i = 0; i < OPERATIONS; ++i) sink += op.run();
        long after = allocation.getThreadAllocatedBytes(id);
        assertTrue(sink != Integer.MIN_VALUE);   // keep the loop from being elided
        return (after - before) / OPERATIONS;
    }

    private static byte[] filled(int size) {
        byte[] out = new byte[size];
        for (int i = 0; i < size; ++i) out[i] = (byte) (i * 31 + 7);
        return out;
    }
}
