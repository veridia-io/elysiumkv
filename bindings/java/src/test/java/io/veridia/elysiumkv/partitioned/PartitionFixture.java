package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.Compression;
import io.veridia.elysiumkv.DiskBlobStore;
import io.veridia.elysiumkv.DiskManifestCatalog;
import io.veridia.elysiumkv.Durability;
import io.veridia.elysiumkv.ElysiumKVOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A directory per partition, and everything opened along the way closed at the end. Options are
 * built fresh on every call because one options object is consumed by one open — a re-assignment
 * needs its own.
 */
final class PartitionFixture implements AutoCloseable {
    static final Function<String, byte[]> KEY_BYTES = key -> key.getBytes(StandardCharsets.UTF_8);

    private final List<AutoCloseable> owned = new ArrayList<>();
    private final Path root;

    PartitionFixture(Path root) {
        this.root = root;
    }

    ElysiumKVOptions optionsFor(int partition) {
        try {
            Path dir = root.resolve("partition-" + partition);
            Path store = dir.resolve("store");
            Files.createDirectories(store);
            DiskBlobStore blobs = own(new DiskBlobStore(store.toString(), "store-" + partition));
            DiskManifestCatalog catalog = own(new DiskManifestCatalog(dir.toString()));
            return own(new ElysiumKVOptions()
                    .manifestCatalog(catalog)
                    .memtableBytes(64 * 1024)
                    .blockBytes(1024)
                    .paranoidChecks(true)
                    .addTier(blobs, Durability.DURABLE, 0, 0, 0)
                    .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                    .level(1, Compression.NONE, 0, 0, 0, 0, 0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Two tiers: a transient one the test can wipe, over a durable one.
     *
     * <p>The transient tier is bounded by <em>capacity</em> rather than by age. Age would work too
     * and the engine requires a bound either way, but capacity makes migration a function of how
     * much has been written rather than of how long the test slept, which is the difference between
     * a test that pins the behaviour and one that pins the machine it ran on.
     */
    ElysiumKVOptions tieredOptionsFor(int partition) {
        try {
            Path dir = root.resolve("partition-" + partition);
            Files.createDirectories(hotDir(partition));
            Files.createDirectories(dir.resolve("cold"));
            DiskBlobStore hot = own(new DiskBlobStore(hotDir(partition).toString(), "hot-" + partition));
            DiskBlobStore cold =
                    own(new DiskBlobStore(dir.resolve("cold").toString(), "cold-" + partition));
            DiskManifestCatalog catalog = own(new DiskManifestCatalog(dir.toString()));
            return own(new ElysiumKVOptions()
                    .manifestCatalog(catalog)
                    .memtableBytes(4 * 1024)
                    .blockBytes(512)
                    .paranoidChecks(true)
                    // An hour of age, so nothing migrates because the test was slow; 6 KB of
                    // capacity, so the oldest files migrate once enough has been written.
                    .addTier(hot, Durability.TRANSIENT, 3_600_000L, 6 * 1024, 7_200_000L)
                    .addTier(cold, Durability.DURABLE, 0, 0, 0)
                    .level(0, Compression.NONE, 0, 2, 8, 12, 0)
                    .level(1, Compression.NONE, 0, 0, 0, 0, 0));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Path hotDir(int partition) {
        return root.resolve("partition-" + partition).resolve("hot");
    }

    Path coldDir(int partition) {
        return root.resolve("partition-" + partition).resolve("cold");
    }

    /** How many SSTs a tier holds right now. */
    static int sstCount(Path dir) {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (Stream<Path> entries = Files.list(dir)) {
            return (int) entries.filter(path -> path.toString().endsWith(".sst")).count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** What a lost transient tier looks like: the objects are simply gone. */
    static int wipe(Path dir) {
        int removed = 0;
        try (Stream<Path> entries = Files.list(dir)) {
            for (Path path : entries.filter(p -> p.toString().endsWith(".sst")).toArray(Path[]::new)) {
                Files.delete(path);
                ++removed;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return removed;
    }

    private <T extends AutoCloseable> T own(T closeable) {
        owned.add(closeable);
        return closeable;
    }

    @Override
    public void close() {
        for (int i = owned.size() - 1; i >= 0; --i) {
            try {
                owned.get(i).close();
            } catch (Exception ignored) {
                // A test that already failed should report that, not this.
            }
        }
        owned.clear();
    }
}
