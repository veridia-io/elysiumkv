package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(PinLeakExtension.class)
class JavaReadApisTest {
    @Test
    void directKeyStartsAtTheBuffersPosition(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("key"), TestSupport.bytes("value"));

            ByteBuffer positioned = ByteBuffer.allocateDirect(9);
            positioned.put(TestSupport.bytes("xxkeytail"));
            positioned.position(2).limit(5);
            try (Pinned value = db.get(positioned)) {
                assertArrayEquals(TestSupport.bytes("value"), value.toByteArray());
            }

            ByteBuffer sliced = positioned.slice();
            try (Pinned value = db.get(sliced)) {
                assertArrayEquals(TestSupport.bytes("value"), value.toByteArray());
            }
            db.close();
        }
    }

    @Test
    void rangeOperationsRejectNullBoundsBeforeJni(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir); WriteBatch batch = new WriteBatch()) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            byte[] key = TestSupport.bytes("key");

            assertThrows(NullPointerException.class, () -> db.deleteRange(null, key));
            assertThrows(NullPointerException.class, () -> db.deleteRange(key, null));
            assertThrows(NullPointerException.class, () -> db.rangeIsErased(null, key));
            assertThrows(NullPointerException.class, () -> db.rangeIsErased(key, null));
            assertThrows(NullPointerException.class, () -> batch.deleteRange(null, key));
            assertThrows(NullPointerException.class, () -> batch.deleteRange(key, null));
            db.close();
        }
    }

    @Test
    void copiedValueIsNeverZeroExtendedDuringConcurrentGrowth(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            byte[] key = TestSupport.bytes("key");
            byte[] small = filled(513, (byte) 0x35);
            byte[] large = filled(4096, (byte) 0x6a);
            db.put(key, small);

            ExecutorService threads = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            Future<?> writer = threads.submit(() -> {
                await(start);
                for (int i = 0; i < 20_000; ++i) db.put(key, (i & 1) == 0 ? large : small);
            });
            Future<?> reader = threads.submit(() -> {
                await(start);
                for (int i = 0; i < 20_000; ++i) {
                    byte[] value = db.getCopy(key);
                    assertTrue(Arrays.equals(value, small) || Arrays.equals(value, large),
                               "a copied value must be one complete committed value");
                }
            });
            try {
                start.countDown();
                writer.get();
                reader.get();
            } finally {
                threads.shutdownNow();
            }
            db.close();
        }
    }

    @Test
    void statsCallsDoNotShareTheirDecodeBuffer(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            ExecutorService threads = Executors.newFixedThreadPool(3);
            CountDownLatch start = new CountDownLatch(1);
            Future<?> writer = threads.submit(() -> {
                await(start);
                for (int i = 0; i < 5_000; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v"));
            });
            Future<?> first = threads.submit(() -> readStats(db, start));
            Future<?> second = threads.submit(() -> readStats(db, start));
            try {
                start.countDown();
                writer.get();
                first.get();
                second.get();
            } finally {
                threads.shutdownNow();
            }
            db.close();
        }
    }

    @Test
    void closeWaitsForAnInFlightStatsCall(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            ExecutorService thread = Executors.newSingleThreadExecutor();
            CountDownLatch started = new CountDownLatch(1);
            Future<?> metrics = thread.submit(() -> {
                for (int i = 0; i < 10_000; ++i) {
                    try {
                        ElysiumKVStats stats = db.stats();
                        assertEquals(1, stats.formatVersion());
                        started.countDown();
                    } catch (IllegalStateException closed) {
                        return;
                    }
                }
            });
            try {
                started.await();
                db.close();
                metrics.get();
            } finally {
                thread.shutdownNow();
            }
        }
    }

    private static void readStats(ElysiumKV db, CountDownLatch start) {
        await(start);
        for (int i = 0; i < 5_000; ++i) {
            ElysiumKVStats stats = db.stats();
            assertEquals(1, stats.formatVersion());
            assertEquals(stats.levelBytesTotal(), stats.tierBytesTotal());
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static byte[] filled(int size, byte value) {
        byte[] out = new byte[size];
        Arrays.fill(out, value);
        return out;
    }
}
