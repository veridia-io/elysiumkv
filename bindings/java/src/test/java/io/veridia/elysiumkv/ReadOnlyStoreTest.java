package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Java end of read-only opens.
 *
 * <p>The type is the point: {@link ElysiumKV#openReadOnly} hands back {@link ReadOnlyStore}, which
 * has no write methods at all — a caller holding one cannot call {@code put} even by accident, and
 * that is a compile error rather than an exception. What these cases check is the rest: that the
 * handle reads, that it sees a snapshot until told otherwise, and that the C ABI's runtime refusal
 * is there for anyone who casts around the type.
 */
@ExtendWith(PinLeakExtension.class)
class ReadOnlyStoreTest {
    @Test
    void aReadOnlyHandleReadsAndSeesASnapshotUntilItRefreshes(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV writer = PinLeakExtension.watch(support.open());
            writer.put(TestSupport.key(1), TestSupport.bytes("first"));
            writer.flush();

            ReadOnlyStore reader = ElysiumKV.openReadOnly(support.options());
            assertArrayEquals(TestSupport.bytes("first"), reader.getCopy(TestSupport.key(1)));

            writer.put(TestSupport.key(2), TestSupport.bytes("second"));
            writer.flush();

            assertNull(reader.getCopy(TestSupport.key(2)),
                       "a reader holds a snapshot, not a subscription");
            reader.refresh();
            assertArrayEquals(TestSupport.bytes("second"), reader.getCopy(TestSupport.key(2)));

            assertNotNull(reader.stats());
            reader.close();
            writer.close();
        }
    }

    /** The C ABI has one handle type, so its refusal is a status. Casting around the type finds it. */
    @Test
    void theRuntimeRefusalIsThereForAnyoneWhoCastsAroundTheType(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV writer = PinLeakExtension.watch(support.open());
            writer.put(TestSupport.key(1), TestSupport.bytes("v"));
            writer.flush();
            writer.close();

            ReadOnlyStore reader = ElysiumKV.openReadOnly(support.options());
            ElysiumKV cast = (ElysiumKV) reader;   // exactly what the type exists to prevent
            assertThrows(ConfigException.class,
                         () -> cast.put(TestSupport.key(2), TestSupport.bytes("v")));
            assertThrows(ConfigException.class, () -> cast.delete(TestSupport.key(1)));
            assertThrows(ConfigException.class, cast::flush);
            assertThrows(ConfigException.class, () -> cast.compactLevel(0));
            assertThrows(ConfigException.class, () -> cast.setWatermark(1));
            reader.close();
        }
    }

    /** A reader that finds no store refuses rather than creating one. */
    @Test
    void aReaderRefusesAStoreThatDoesNotExist(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            assertThrows(ElysiumKVException.class, () -> ElysiumKV.openReadOnly(support.options()));
        }
    }

    /** The ordering between the two retention windows is checked at open, not documented. */
    @Test
    void anOrphanWindowShorterThanTheReaderWindowIsRefused(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKVOptions options =
                    support.options().obsoleteRetentionMs(600_000).orphanRetentionMs(300_000);
            assertThrows(ConfigException.class, () -> ElysiumKV.open(options));

            ElysiumKVOptions equal =
                    support.options().obsoleteRetentionMs(600_000).orphanRetentionMs(600_000);
            ElysiumKV db = PinLeakExtension.watch(support.own(ElysiumKV.open(equal)));
            assertEquals(0, db.stats().levels().get(0).fileCount());
            db.close();
        }
    }
}
