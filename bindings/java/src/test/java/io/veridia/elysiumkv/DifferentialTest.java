package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * ARCHITECTURE.md "Dependencies and artifacts" — the "differential harness through the binding": a {@link TreeMap} oracle
 * driven through the Java API over a seeded operation stream.
 *
 * <p>The C++ suite already proves the engine matches an oracle. What it cannot
 * reach is <em>this</em> layer — a key length passed where a byte count was
 * meant, a buffer read past its limit, a status folded into the wrong branch.
 * Those produce wrong answers rather than crashes, and only a comparison against
 * an independent model finds them.
 *
 * <p>Ordering matters and is easy to get wrong: the engine compares keys as
 * <b>unsigned</b> bytes, so the oracle must too. A signed comparator would agree
 * on ASCII and diverge the moment a key goes above 0x7F — which is exactly why
 * the generated keys below include high bytes.
 */
@ExtendWith(PinLeakExtension.class)
class DifferentialTest {
    private static final Comparator<byte[]> UNSIGNED = (a, b) -> {
        int limit = Math.min(a.length, b.length);
        for (int i = 0; i < limit; ++i) {
            int difference = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (difference != 0) return difference;
        }
        return a.length - b.length;
    };

    private static int operations() {
        return System.getenv("ELYSIUMKV_DIFF_FULL") != null ? 200_000 : 20_000;
    }

    @Test
    void matchesATreeMapOverASeededStream(@TempDir Path dir) throws Exception {
        long seed = 0x5EEDL;
        Random random = new Random(seed);
        TreeMap<byte[], byte[]> oracle = new TreeMap<>(UNSIGNED);

        try (TestSupport support = new TestSupport(dir)) {
            ElysiumKV db = PinLeakExtension.watch(support.open());
            int total = operations();

            for (int step = 0; step < total; ++step) {
                switch (random.nextInt(100)) {
                    case 0: case 1: case 2: case 3: case 4:
                    case 5: case 6: case 7: case 8: case 9:
                        delete(db, oracle, key(random));
                        break;
                    case 10: case 11: case 12:
                        scan(db, oracle, random);
                        break;
                    case 13:
                        db.flush();
                        break;
                    case 14:
                        // Only occasionally: it rewrites the whole level.
                        if (random.nextInt(20) == 0) db.compactLevel(0);
                        break;
                    default:
                        if (random.nextBoolean()) {
                            byte[] k = key(random);
                            byte[] v = value(random);
                            db.put(k, v);
                            oracle.put(k, v);
                        } else {
                            lookup(db, oracle, key(random));
                        }
                }
            }

            // A full pass at the end: every key the oracle holds, and the whole
            // keyspace in order.
            for (Map.Entry<byte[], byte[]> entry : oracle.entrySet()) {
                lookupExpecting(db, entry.getKey(), entry.getValue());
            }
            assertFullScanMatches(db, oracle);

            db.flush();
            assertFullScanMatches(db, oracle);
            db.close();
        }
    }

    private static void lookup(ElysiumKV db, TreeMap<byte[], byte[]> oracle, byte[] key) {
        byte[] expected = oracle.get(key);
        try (Pinned pinned = db.get(key)) {
            if (expected == null) {
                assertNull(pinned, () -> "engine has a key the oracle does not: " + hex(key));
            } else {
                assertNotNull(pinned, () -> "engine lost " + hex(key));
                assertEquals(hex(expected), hex(pinned.toByteArray()));
            }
        }
    }

    private static void lookupExpecting(ElysiumKV db, byte[] key, byte[] expected) {
        try (Pinned pinned = db.get(key)) {
            assertNotNull(pinned, () -> "engine lost " + hex(key));
            assertEquals(hex(expected), hex(pinned.toByteArray()));
        }
    }

    private static void delete(ElysiumKV db, TreeMap<byte[], byte[]> oracle, byte[] key) {
        db.delete(key);
        oracle.remove(key);
    }

    /** A prefix scan compared against the oracle's matching sub-map. */
    private static void scan(ElysiumKV db, TreeMap<byte[], byte[]> oracle, Random random) {
        byte[] prefix = new byte[] {(byte) ('a' + random.nextInt(4))};
        List<String> expected = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> entry : oracle.entrySet()) {
            if (startsWith(entry.getKey(), prefix)) expected.add(hex(entry.getKey()));
        }

        List<String> actual = new ArrayList<>();
        try (ElysiumKVIterator it = db.prefixIterator(prefix)) {
            while (it.next()) actual.add(hex(it.keyBytes()));
            it.status();
        }
        assertEquals(expected, actual, () -> "prefix scan diverged for " + hex(prefix));
    }

    private static void assertFullScanMatches(ElysiumKV db, TreeMap<byte[], byte[]> oracle) {
        List<String> expected = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> entry : oracle.entrySet()) {
            expected.add(hex(entry.getKey()) + "=" + hex(entry.getValue()));
        }
        List<String> actual = new ArrayList<>();
        try (ElysiumKVIterator it = db.iterator(null, null)) {
            while (it.next()) actual.add(hex(it.keyBytes()) + "=" + hex(it.valueBytes()));
            it.status();
        }
        assertEquals(expected, actual, "full scan diverged from the oracle");
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; ++i) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }

    /** Keys deliberately span above 0x7F, where signed and unsigned order differ. */
    private static byte[] key(Random random) {
        byte[] key = new byte[1 + random.nextInt(6)];
        key[0] = (byte) ('a' + random.nextInt(4));
        for (int i = 1; i < key.length; ++i) key[i] = (byte) random.nextInt(256);
        return key;
    }

    private static byte[] value(Random random) {
        byte[] value = new byte[random.nextInt(64)];
        random.nextBytes(value);
        return value;
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) out.append(String.format("%02x", b));
        return out.toString();
    }
}
