package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(PinLeakExtension.class)
class BatchTest {
    @Test
    void aBatchIsOrderedAndAppliedAsAUnit(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());

            try (WriteBatch batch = new WriteBatch()) {
                batch.put(TestSupport.bytes("a"), TestSupport.bytes("1"));
                batch.put(TestSupport.bytes("b"), TestSupport.bytes("2"));
                batch.delete(TestSupport.bytes("a"));
                batch.put(TestSupport.bytes("c"), TestSupport.bytes("3"));
                assertEquals(4, batch.size(), "size counts operations, not distinct keys");

                // Nothing is visible until write().
                assertNull(db.getCopy(TestSupport.bytes("b")));
                db.write(batch);
            }

            assertNull(db.getCopy(TestSupport.bytes("a")), "the later delete wins");
            assertEquals("2", TestSupport.string(db.getCopy(TestSupport.bytes("b"))));
            assertEquals("3", TestSupport.string(db.getCopy(TestSupport.bytes("c"))));
            db.close();
        }
    }

    @Test
    void anEmptyBatchIsHarmless(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            try (WriteBatch batch = new WriteBatch()) {
                assertEquals(0, batch.size());
                db.write(batch);
            }
            db.close();
        }
    }

    @Test
    void aBatchCanBeReusedAfterWriting(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            try (WriteBatch batch = new WriteBatch()) {
                batch.put(TestSupport.bytes("k"), TestSupport.bytes("first"));
                db.write(batch);
                batch.put(TestSupport.bytes("k"), TestSupport.bytes("second"));
                db.write(batch);
            }
            // The batch still held the first put, so replaying it applies both
            // in order and the last one wins.
            assertEquals("second", TestSupport.string(db.getCopy(TestSupport.bytes("k"))));
            db.close();
        }
    }

    @Test
    void usingAClosedBatchIsRefused(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            WriteBatch batch = new WriteBatch();
            batch.put(TestSupport.bytes("k"), TestSupport.bytes("v"));
            batch.close();
            assertThrows(IllegalStateException.class,
                         () -> batch.put(TestSupport.bytes("k"), TestSupport.bytes("v")));
            assertThrows(IllegalStateException.class, () -> db.write(batch));
            db.close();
        }
    }

    // ARCHITECTURE.md "Inside an SST" — the size limit is checked before any of the batch is applied, so an
    // oversized entry cannot leave the entries before it behind.
    @Test
    void anOversizedEntryRejectsTheWholeBatch(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            try (WriteBatch batch = new WriteBatch()) {
                batch.put(TestSupport.bytes("fine"), TestSupport.bytes("v"));
                batch.put(TestSupport.bytes("huge"), new byte[(1 << 20) + 1]);
                ConfigException thrown = assertThrows(ConfigException.class, () -> db.write(batch));
                assertNotNull(thrown.getMessage());
            }
            assertNull(db.getCopy(TestSupport.bytes("fine")),
                       "the entry before the oversized one must not have landed");
            db.close();
        }
    }
}
