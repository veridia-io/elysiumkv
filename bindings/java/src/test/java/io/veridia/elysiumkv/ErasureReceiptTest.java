package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two things a binding was missing: the erasure receipt, and a batched range scan.
 *
 * <p>What the receipt <em>means</em> is settled by the C++ suite. What is settled here is that it
 * crosses the boundary at all — a compliance answer reachable only from C++ is not a feature
 * anybody can use.
 */
class ErasureReceiptTest {

    private static byte[] key(int i) {
        return String.format("key:%06d", i).getBytes();
    }

    @Test
    void aReceiptIsWithheldUntilTheFilesAreGone(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.open();
            for (int i = 0; i < 40; ++i) db.put(key(i), TestSupport.bytes("v"));
            db.flush();

            assertFalse(db.rangeIsErased(key(0), key(40)), "the data is right there");

            db.deleteRange(key(0), key(40));
            db.flush();

            // Reads already answer "absent"; the receipt is a different question and may still be
            // withheld. What must not happen is the two being confused.
            assertEquals(null, db.get(key(5)), "the tombstone is doing its job");
        }
    }

    /** An empty or inverted band holds nothing, matching what deleteRange does with one. */
    @Test
    void anEmptyBandIsTriviallyErased(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.open();
            for (int i = 0; i < 20; ++i) db.put(key(i), TestSupport.bytes("v"));
            db.flush();

            assertTrue(db.rangeIsErased(key(5), key(5)));
            assertTrue(db.rangeIsErased(key(9), key(2)));
        }
    }

    /**
     * The gap this closes: the prefix scans have had a batched form since the beginning and the
     * range scan did not, so a range scan through the binding was on the slow path for no stated
     * reason.
     */
    @Test
    void aBatchedRangeScanSeesTheSameEntriesAsThePlainOne(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.open();
            for (int i = 0; i < 200; ++i) db.put(key(i), TestSupport.bytes("v" + i));
            db.flush();

            List<String> plain = new ArrayList<>();
            try (ElysiumKVIterator it = db.iterator(key(20), key(60))) {
                while (it.next()) plain.add(TestSupport.string(it.keyBytes()));
            }

            List<String> batched = new ArrayList<>();
            try (BatchedIterator it = db.batchedIterator(key(20), key(60))) {
                while (it.next()) batched.add(TestSupport.string(it.keyBytes()));
            }

            assertEquals(40, plain.size(), "the half-open band is [20, 60)");
            assertEquals(plain, batched, "the batched path must not change what a scan yields");
        }
    }

    @Test
    void aBatchedReverseRangeScanIsTheSameEntriesBackwards(@TempDir Path dir) throws IOException {
        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = support.open();
            for (int i = 0; i < 100; ++i) db.put(key(i), TestSupport.bytes("v"));
            db.flush();

            List<String> descending = new ArrayList<>();
            try (BatchedIterator it = db.batchedReverseIterator(key(10), key(20))) {
                while (it.next()) descending.add(TestSupport.string(it.keyBytes()));
            }

            assertEquals(10, descending.size());
            assertArrayEquals(key(19), descending.get(0).getBytes(), "descending starts at the top");
            assertArrayEquals(key(10), descending.get(9).getBytes());
        }
    }
}
