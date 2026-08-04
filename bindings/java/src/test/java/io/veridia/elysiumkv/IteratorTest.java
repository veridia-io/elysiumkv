package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(PinLeakExtension.class)
class IteratorTest {
    @Test
    void prefixScanReturnsOnlyThePrefixInOrder(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 400; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v" + i));
            db.put(TestSupport.bytes("other:1"), TestSupport.bytes("x"));
            db.flush();

            List<String> seen = new ArrayList<>();
            try (ElysiumKVIterator it = db.prefixIterator(TestSupport.bytes("key:"))) {
                while (it.next()) seen.add(TestSupport.string(it.keyBytes()));
                it.status();
            }
            assertEquals(400, seen.size());
            for (int i = 1; i < seen.size(); ++i) {
                assertTrue(seen.get(i - 1).compareTo(seen.get(i)) < 0, "keys arrive in order");
            }
            db.close();
        }
    }

    @Test
    void boundsAreHalfOpen(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 100; ++i) db.put(TestSupport.key(i), TestSupport.bytes("v"));
            db.flush();

            try (ElysiumKVIterator it = db.iterator(TestSupport.key(10), TestSupport.key(20))) {
                List<String> seen = new ArrayList<>();
                while (it.next()) seen.add(TestSupport.string(it.keyBytes()));
                assertEquals(10, seen.size());
                assertEquals(TestSupport.string(TestSupport.key(10)), seen.get(0));
                assertEquals(TestSupport.string(TestSupport.key(19)), seen.get(9));
            }
            db.close();
        }
    }

    @Test
    void readingBeforePositioningIsRefused(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("k"), TestSupport.bytes("v"));

            try (ElysiumKVIterator it = db.iterator(null, null)) {
                // There is no seek and no valid(); the first next() positions.
                assertThrows(NoSuchElementException.class, it::key);
                assertTrue(it.next());
                assertEquals("k", TestSupport.string(it.keyBytes()));
                assertEquals("v", TestSupport.string(it.valueBytes()));
                assertFalse(it.next());
            }
            db.close();
        }
    }

    /// The copy-into accessors must agree with the allocating ones, including
    /// when the caller's buffer is too small — the length is the full length.
    @Test
    void copyIntoAgreesWithTheAllocatingAccessors(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            db.put(TestSupport.bytes("k"), TestSupport.bytes("a-long-enough-value"));
            db.flush();

            try (ElysiumKVIterator it = db.iterator(null, null)) {
                assertTrue(it.next());
                byte[] roomy = new byte[64];
                int keyLength = it.keyInto(roomy);
                assertEquals(TestSupport.string(it.keyBytes()),
                             new String(roomy, 0, keyLength, java.nio.charset.StandardCharsets.UTF_8));

                byte[] tiny = new byte[4];
                int valueLength = it.valueInto(tiny);
                assertEquals(it.valueBytes().length, valueLength,
                             "the full length is reported even when it does not fit");
                assertEquals("a-lo", new String(tiny, 0, 4,
                                                java.nio.charset.StandardCharsets.UTF_8));
            }
            db.close();
        }
    }

    /// The batched scan must agree with the per-entry one exactly — it is the
    /// same data through a different number of crossings, and the ABI addition
    /// is only worth anything if that is true.
    @Test
    void theBatchedScanAgreesWithThePerEntryScan(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 2000; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
            }
            db.flush();

            List<String> perEntry = new ArrayList<>();
            try (ElysiumKVIterator it = db.prefixIterator(TestSupport.bytes("key:"))) {
                while (it.next()) {
                    perEntry.add(TestSupport.string(it.keyBytes()) + "="
                                 + TestSupport.string(it.valueBytes()));
                }
                it.status();
            }

            List<String> batched = new ArrayList<>();
            try (BatchedIterator scan = db.batchedPrefixIterator(TestSupport.bytes("key:"))) {
                while (scan.next()) {
                    batched.add(TestSupport.string(scan.keyBytes()) + "="
                                + TestSupport.string(scan.valueBytes()));
                }
                scan.status();
            }

            assertEquals(2000, perEntry.size());
            assertEquals(perEntry, batched);
            db.close();
        }
    }

    /// A batch that cannot hold even one entry must grow rather than report
    /// exhaustion — otherwise a large value would silently truncate the scan.
    @Test
    void aBatchGrowsForAnEntryTooLargeToFit(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            byte[] big = new byte[300 * 1024];   // larger than the initial batch buffer
            db.put(TestSupport.bytes("k:1"), big);
            db.put(TestSupport.bytes("k:2"), TestSupport.bytes("small"));
            db.flush();

            int seen = 0;
            try (BatchedIterator scan = db.batchedPrefixIterator(TestSupport.bytes("k:"))) {
                while (scan.next()) {
                    assertTrue(scan.valueInto(new byte[4]) > 0);
                    ++seen;
                }
                scan.status();
            }
            assertEquals(2, seen, "the oversized entry must not truncate the scan");
            db.close();
        }
    }

    @Test
    void valuesSurviveCompactionDuringAScan(@TempDir Path dir) throws Exception {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            for (int i = 0; i < 2000; ++i) {
                db.put(TestSupport.key(i), TestSupport.bytes("value:" + i));
            }
            db.flush();

            int seen = 0;
            try (ElysiumKVIterator it = db.prefixIterator(TestSupport.bytes("key:"))) {
                while (it.next()) {
                    assertEquals("value:" + seen, TestSupport.string(it.valueBytes()));
                    if (seen == 500) db.compactLevel(0);   // rewrite underneath the scan
                    ++seen;
                }
                it.status();
            }
            assertEquals(2000, seen);
            db.close();
        }
    }
}
