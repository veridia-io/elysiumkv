package io.veridia.elysiumkv.bench;

import io.veridia.elysiumkv.Compression;
import io.veridia.elysiumkv.Durability;
import io.veridia.elysiumkv.FileManifestCatalog;
import io.veridia.elysiumkv.LocalFileBlobStore;
import io.veridia.elysiumkv.Pinned;
import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVIterator;
import io.veridia.elysiumkv.ElysiumKVOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * ARCHITECTURE.md "Benchmarks" — the two binding numbers.
 *
 * <ul>
 *   <li><b>Point-lookup crossing cost</b>, against {@code BM_PointLookupHot} from
 *       the C++ suite. "If the <i>value</i> path shows a copy, the pin protocol
 *       is not doing its job" — {@link #getPinned} versus {@link #getCopy} is
 *       exactly that comparison, and {@code ZeroCopyTest} already asserts the
 *       allocation half of it.
 *   <li><b>Scan crossing cost</b>, per entry. This is the number that decides
 *       whether {@code elysiumkv_iter_next_batch} (ARCHITECTURE.md "The ABI boundary") earns its ABI surface, and
 *       it is deliberately measured before the batching exists rather than after.
 * </ul>
 *
 * <p>Recorded, not gated: ARCHITECTURE.md "Dependencies and artifacts" step 10 says "recorded", and a JVM microbenchmark on
 * a shared runner is not a build gate.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CrossingCostBenchmark {
    /** Matches the C++ BenchStore: 100k keys, so the two numbers are comparable. */
    private static final int KEYS = 100_000;
    private static final int SCAN_PREFIX_KEYS = 1_000;

    private Path dir;
    private LocalFileBlobStore store;
    private FileManifestCatalog catalog;
    private ElysiumKVOptions options;
    private ElysiumKV db;

    private byte[] hotKey;
    private ByteBuffer hotKeyDirect;
    private byte[] scratch;
    private byte[] scanPrefix;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        dir = Files.createTempDirectory("elysiumkv-bench");
        Files.createDirectories(dir.resolve("store"));
        store = new LocalFileBlobStore(dir.resolve("store").toString(), "bench");
        catalog = new FileManifestCatalog(dir.toString());
        options = new ElysiumKVOptions()
                .manifestCatalog(catalog)
                .memtableBytes(4L << 20)
                .blockCacheBytes(256L << 20)   // everything hot: this measures the boundary
                .addTier(store, Durability.DURABLE, 0, 0, 0, 0)
                .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                .level(1, Compression.NONE, 0, 0, 0, 0, 0);
        db = ElysiumKV.open(options);

        for (int i = 0; i < KEYS; ++i) {
            db.put(key(i), ("value:" + i).getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 0; i < SCAN_PREFIX_KEYS; ++i) {
            db.put(("scan:" + String.format("%06d", i)).getBytes(StandardCharsets.UTF_8),
                   ("value:" + i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush();

        hotKey = key(42);
        hotKeyDirect = ByteBuffer.allocateDirect(hotKey.length);
        hotKeyDirect.put(hotKey).flip();
        scratch = new byte[256];
        scanPrefix = "scan:".getBytes(StandardCharsets.UTF_8);

        // Warm the block cache so the benchmark measures the crossing, not IO.
        for (int i = 0; i < KEYS; ++i) db.getCopy(key(i));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        db.closeReportingOutstanding();
        options.close();
        catalog.close();
        store.close();
    }

    private static byte[] key(int i) {
        return String.format("key:%08d", i).getBytes(StandardCharsets.UTF_8);
    }

    /** The zero-copy path: one crossing, no value copy. */
    @Benchmark
    public int getPinned() {
        try (Pinned pinned = db.get(hotKey)) {
            return pinned.value().get(0);
        }
    }

    /** The same, with the key already off-heap — nothing copied either way. */
    @Benchmark
    public int getPinnedDirectKey() {
        hotKeyDirect.position(0);
        try (Pinned pinned = db.get(hotKeyDirect)) {
            return pinned.value().get(0);
        }
    }

    /** The copying baseline. The gap to getPinned is what the pin protocol buys. */
    @Benchmark
    public int getCopy() {
        return db.getCopy(hotKey).length;
    }

    /**
     * Per-entry scan cost. Divided by {@link #SCAN_PREFIX_KEYS} this is the
     * per-crossing number; ARCHITECTURE.md "The ABI boundary" puts a JNI crossing at 5-10ns and says batching
     * is worth its ABI surface only if that dominates.
     */
    @Benchmark
    public void prefixScanPerEntry(Blackhole blackhole) {
        try (ElysiumKVIterator it = db.prefixIterator(scanPrefix)) {
            while (it.next()) {
                blackhole.consume(it.key().get(0));
                blackhole.consume(it.value().get(0));
            }
        }
    }

    /** Keys only: half the crossings, so the difference isolates the per-call cost. */
    @Benchmark
    public void prefixScanKeysOnly(Blackhole blackhole) {
        try (ElysiumKVIterator it = db.prefixIterator(scanPrefix)) {
            while (it.next()) blackhole.consume(it.key().get(0));
        }
    }

    /**
     * The same scan through the copy-into accessors: identical crossings, no
     * per-entry allocation. The gap to {@link #prefixScanPerEntry} is the cost of
     * minting two direct buffers per entry; the gap to {@link
     * #prefixScanAdvanceOnly} is what is left, and only that residue is what a
     * batched ABI could remove.
     */
    @Benchmark
    public int prefixScanIntoReusedBuffers() {
        int total = 0;
        try (ElysiumKVIterator it = db.prefixIterator(scanPrefix)) {
            while (it.next()) {
                total += it.keyInto(scratch);
                total += it.valueInto(scratch);
            }
        }
        return total;
    }

    /**
     * The same scan through {@code elysiumkv_iter_next_batch}: one crossing per
     * batch instead of three per entry. This is the number the ABI addition was
     * justified by, measured the same way as the alternatives above.
     */
    @Benchmark
    public int prefixScanBatched() {
        int total = 0;
        try (io.veridia.elysiumkv.BatchedIterator scan = db.batchedPrefixIterator(scanPrefix)) {
            while (scan.next()) {
                total += scan.keyInto(scratch);
                total += scan.valueInto(scratch);
            }
        }
        return total;
    }

    /** Just the advance: the floor a batched API would be measured against. */
    @Benchmark
    public int prefixScanAdvanceOnly() {
        int seen = 0;
        try (ElysiumKVIterator it = db.prefixIterator(scanPrefix)) {
            while (it.next()) ++seen;
        }
        return seen;
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder()
                           .include(CrossingCostBenchmark.class.getSimpleName())
                           .jvmArgsAppend("-Delysiumkv.library.path="
                                          + System.getProperty("elysiumkv.library.path", ""))
                           .build())
                .run();
    }
}
