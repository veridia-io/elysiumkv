package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Java end of {@code flushIntervalMs}. A knob the C++ API can reach and a binding cannot is
 * the asymmetry this project keeps having to remove, so the binding asserts the behaviour rather
 * than just declaring the setter.
 *
 * <p>Unlike the C++ suite these tests cannot inject a clock — the binding deliberately exposes no
 * way to — so they wait on real time. The interval is therefore short, and the assertion is that
 * a flush happens without any further write, which nothing but the interval can cause.
 */
@ExtendWith(PinLeakExtension.class)
class FlushIntervalTest {
    /** Large enough that one small write can never reach it, so size cannot be the cause. */
    private static final long BIG_MEMTABLE = 64L << 20;

    private ElysiumKVOptions options(Path dir, long flushIntervalMs, TestSupport support)
            throws IOException {
        Files.createDirectories(dir);
        return support.own(new ElysiumKVOptions()
                .manifestCatalog(support.own(new DiskManifestCatalog(dir.toString())))
                .memtableBytes(BIG_MEMTABLE)
                .flushIntervalMs(flushIntervalMs)
                .paranoidChecks(true)
                .addTier(support.hot, Durability.DURABLE, 0, 0, 0)
                .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                .level(1, Compression.NONE, 0, 0, 0, 0, 0));
    }

    private static int l0Files(ElysiumKV db) {
        return db.stats().levels().get(0).fileCount();
    }

    private static boolean waitForFlush(ElysiumKV db, long limitMs) throws InterruptedException {
        long deadline = System.nanoTime() + limitMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (l0Files(db) >= 1) return true;
            Thread.sleep(10);
        }
        return l0Files(db) >= 1;
    }

    @Test
    void anIdleMemtableIsFlushedOnceTheIntervalElapses(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(
                    ElysiumKV.open(options(dir, 100, support)));
            db.put(TestSupport.key(1), TestSupport.bytes("v"));
            // No second write, no flush() call. Only the interval can produce a file.
            assertTrue(waitForFlush(db, 5_000),
                       "the memtable outlived the interval and was never flushed");
            assertEquals("v", TestSupport.string(db.getCopy(TestSupport.key(1))));
        }
    }

    /** The control: without an interval the same write must stay in memory. */
    @Test
    void withoutAnIntervalAnIdleMemtableIsNeverFlushed(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(
                    ElysiumKV.open(options(dir, 0, support)));
            db.put(TestSupport.key(1), TestSupport.bytes("v"));
            Thread.sleep(500);
            assertEquals(0, l0Files(db), "size was the only trigger, and it was never reached");
            assertEquals("v", TestSupport.string(db.getCopy(TestSupport.key(1))),
                         "still readable from the memtable");
        }
    }
}
