package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The compaction window as a binding sees it. What it <em>does</em> is settled by the C++ suite;
 * what is settled here is that the value crosses the boundary at all — an option C++ can reach and
 * a binding cannot is the asymmetry this repository keeps having to remove.
 */
class CompactionWindowOptionTest {

    @Test
    void aStoreOpensWithTheWindowSetAndCompacts(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(
                    support.own(ElysiumKV.open(support.options().compactionWindowBytes(1 << 20))));
            for (int i = 0; i < 2000; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
            }
            db.flush();
            db.compactLevel(0);

            for (int i = 0; i < 2000; ++i) {
                try (Pinned found = db.get(TestSupport.key(i))) {
                    assertEquals("value:" + i, TestSupport.string(found.toByteArray()));
                }
            }
            db.close();
        }
    }

    /** Zero means "leave the default", which is how every other size on this call behaves. */
    @Test
    void zeroLeavesTheDefault(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(
                    support.own(ElysiumKV.open(support.options().compactionWindowBytes(0))));
            assertNotNull(db);
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));
            try (Pinned found = db.get(TestSupport.bytes("k"))) {
                assertEquals("v", TestSupport.string(found.toByteArray()));
            }
            db.close();
        }
    }

    /** Negative is not a size. It reaches the C ABI as a huge unsigned value, so the refusal has to
     *  come from the binding rather than from a store that quietly buffers gigabytes. */
    @Test
    void aNegativeWindowIsRefused(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            assertThrows(IllegalArgumentException.class,
                         () -> support.options().compactionWindowBytes(-1));
        }
    }
}
