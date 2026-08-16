package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * The binding half of the logger. The engine suite already covers which events fire and when; what
 * only Java can get wrong is the crossing — the sink is the one call that goes C++ to Java, and it
 * arrives on engine threads the JVM has never seen.
 */
@ExtendWith(PinLeakExtension.class)
class LoggerTest {

    private ElysiumKVOptions options(Path dir, TestSupport support, ElysiumKVLogger sink,
                                     ElysiumKVLogger.Level level) throws IOException {
        Files.createDirectories(dir);
        return support.own(new ElysiumKVOptions()
                .manifestCatalog(support.own(new DiskManifestCatalog(dir.toString())))
                .memtableBytes(64L << 10)
                .logger(sink, level)
                .addTier(support.hot, Durability.DURABLE, 0, 0, 0)
                .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                .level(1, Compression.NONE, 0, 0, 0, 0, 0));
    }

    private static void fill(ElysiumKV db, int count, String prefix) {
        byte[] value = new byte[512];
        for (int i = 0; i < count; i++) {
            db.put(TestSupport.bytes(prefix + i), value);
        }
    }

    @Test
    void theEngineReachesJavaAndDecodesLevelAndEvent(@TempDir Path dir) throws Exception {
        List<String> messages = new CopyOnWriteArrayList<>();
        Set<ElysiumKVLogger.Event> events = ConcurrentHashMap.newKeySet();
        Set<ElysiumKVLogger.Level> levels = ConcurrentHashMap.newKeySet();

        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVLogger sink = (level, event, message) -> {
                levels.add(level);
                events.add(event);
                messages.add(message);
            };
            ElysiumKV db = PinLeakExtension.watch(ElysiumKV.open(
                    options(dir, support, sink, ElysiumKVLogger.Level.INFO)));
            fill(db, 400, "k");
            db.flush();

            assertTrue(events.contains(ElysiumKVLogger.Event.FLUSH_COMPLETE),
                    "no flush event reached Java; saw " + events);
            // Decoded, not raw ints, and never the fallback.
            assertFalse(events.contains(ElysiumKVLogger.Event.UNKNOWN),
                    "an event code did not decode: " + events);
            assertTrue(levels.contains(ElysiumKVLogger.Level.INFO), "levels seen: " + levels);
            for (String message : messages) {
                assertNotNull(message);
                assertFalse(message.isEmpty(), "an empty message crossed the boundary");
            }
        }
    }

    /**
     * The attach path. Compaction runs on an engine thread that the JVM did not create, so this
     * fails outright — rather than merely reporting nothing — if the native side does not attach
     * it before calling back.
     */
    @Test
    void aBackgroundThreadCanCallIntoJava(@TempDir Path dir) throws Exception {
        Set<Long> threads = ConcurrentHashMap.newKeySet();
        Set<ElysiumKVLogger.Event> events = ConcurrentHashMap.newKeySet();

        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVLogger sink = (level, event, message) -> {
                threads.add(Thread.currentThread().getId());
                events.add(event);
            };
            ElysiumKV db = PinLeakExtension.watch(ElysiumKV.open(
                    options(dir, support, sink, ElysiumKVLogger.Level.INFO)));
            for (int round = 0; round < 8; round++) {
                fill(db, 200, "round" + round + "-");
                db.flush();
            }
            // Compaction is the background half; the flushes above are enough to trigger it.
            long deadline = System.nanoTime() + 10_000L * 1_000_000L;
            while (System.nanoTime() < deadline
                    && !events.contains(ElysiumKVLogger.Event.COMPACTION_COMPLETE)) {
                Thread.sleep(20);
            }
            assertTrue(events.contains(ElysiumKVLogger.Event.COMPACTION_COMPLETE),
                    "compaction never reported; saw " + events);
            assertTrue(threads.size() >= 1, "no thread recorded");
        }
    }

    /** A flush must not fail because logging it threw. */
    @Test
    void aThrowingSinkDoesNotFailTheOperation(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVLogger sink = (level, event, message) -> {
                throw new IllegalStateException("this appender is broken");
            };
            ElysiumKV db = PinLeakExtension.watch(ElysiumKV.open(
                    options(dir, support, sink, ElysiumKVLogger.Level.INFO)));
            fill(db, 400, "k");
            db.flush();   // throws if the exception propagated back through the engine
            try (Pinned value = db.get(TestSupport.bytes("k1"))) {
                assertNotNull(value);
            }
        }
    }

    @Test
    void aNullSinkIsAcceptedAndSaysNothing(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(ElysiumKV.open(
                    options(dir, support, null, ElysiumKVLogger.Level.INFO)));
            fill(db, 200, "k");
            db.flush();
            try (Pinned value = db.get(TestSupport.bytes("k1"))) {
                assertNotNull(value);
            }
        }
    }
}
