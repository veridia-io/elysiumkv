package io.veridia.elysiumkv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Shared fixture: a database on a temp directory, with everything closed after. */
final class TestSupport implements AutoCloseable {
    private final List<AutoCloseable> owned = new ArrayList<>();
    final Path dir;
    final LocalFileBlobStore hot;
    LocalFileBlobStore cold;

    TestSupport(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir.resolve("store"));
        hot = own(new LocalFileBlobStore(dir.resolve("store").toString(), "store-0"));
    }

    /** One durable tier, two levels — the simplest correct configuration. */
    ElysiumKVOptions options() {
        FileManifestCatalog catalog = own(new FileManifestCatalog(dir.toString()));
        return own(new ElysiumKVOptions()
                .manifestCatalog(catalog)
                .memtableBytes(64 * 1024)
                .blockBytes(1024)
                .paranoidChecks(true)
                .addTier(hot, Durability.DURABLE, 0, 0, 0, 0)
                .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                .level(1, Compression.ZSTD, 0, 0, 0, 0, 0));
    }

    /** the second shape: a transient hot tier over a durable one. */
    ElysiumKVOptions transientOptions() throws IOException {
        Path coldDir = dir.resolve("cold");
        Files.createDirectories(coldDir);
        cold = own(new LocalFileBlobStore(coldDir.toString(), "store-1"));
        FileManifestCatalog catalog = own(new FileManifestCatalog(dir.toString()));
        return own(new ElysiumKVOptions()
                .manifestCatalog(catalog)
                .memtableBytes(64 * 1024)
                .paranoidChecks(true)
                .addTier(hot, Durability.TRANSIENT, 60_000, 0, 0, 120_000)
                .addTier(cold, Durability.DURABLE, 0, 0, 0, 0)
                .level(0, Compression.NONE, 0, 4, 8, 12, 0)
                .level(1, Compression.NONE, 0, 0, 0, 0, 0));
    }

    ElysiumKV open() {
        return ElysiumKV.open(options());
    }

    <T extends AutoCloseable> T own(T closeable) {
        owned.add(closeable);
        return closeable;
    }

    static byte[] key(int i) {
        return String.format("key:%06d", i).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // Reverse order: the database goes before the stores it reads through.
        for (int i = owned.size() - 1; i >= 0; --i) {
            try {
                owned.get(i).close();
            } catch (Exception ignored) {
                // A test that already failed should report that, not this.
            }
        }
    }
}
