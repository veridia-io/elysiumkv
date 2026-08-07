package io.veridia.elysiumkv.streams.v3;

import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVIterator;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.WriteBatch;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A Kafka Streams {@link KeyValueStore} backed by ElysiumKV.
 *
 * <p><b>Phase 1: Streams' fault tolerance is exactly what it was.</b> The changelog is still the
 * source of truth, restore still replays it, and the checkpoint file still says where replay
 * resumes. The only thing that changes is where the bytes live — which is the point, because that
 * is what lets state exceed local disk.
 *
 * <p><b>What this costs, stated rather than discovered.</b> In {@link StorageMode#REMOTE} a cold
 * read is an object-store GET on the processing path — tens of milliseconds against microseconds
 * locally. It pays off when key access is skewed toward recent data, so migrated files are rarely
 * touched; the cache chain softens repeat reads and does nothing for a genuine first touch. <b>A
 * topology that scans or randomly accesses the whole keyspace will be slower.</b>
 */
public class ElysiumKVKeyValueStore implements KeyValueStore<Bytes, byte[]> {
    private final String name;
    private final ElysiumKVStoreConfig config;

    private ElysiumKVOptions options;
    private ElysiumKV db;
    private StateStoreContext context;
    private volatile boolean open;

    ElysiumKVKeyValueStore(String name, ElysiumKVStoreConfig config) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    @Deprecated
    public void init(ProcessorContext context, StateStore root) {
        open(context.stateDir(), root, null);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        open(context.stateDir(), root, context);
    }

    private void open(File stateDir, StateStore root, StateStoreContext ctx) {
        Path directory = stateDir.toPath().resolve(name);
        directory.toFile().mkdirs();
        options = config.toOptions(directory);
        db = ElysiumKV.open(options);
        open = true;

        // Streams registers the store so restore can replay the changelog into it. Phase 1 does not
        // manage offsets, so this is the ordinary path and the checkpoint file remains authoritative.
        if (ctx != null) {
            ctx.register(root, (key, value) -> {
                if (value == null) {
                    db.delete(key);
                } else {
                    db.put(key, value);
                }
            });
        }
    }

    @Override
    public void put(Bytes key, byte[] value) {
        assertOpen();
        if (value == null) {
            db.delete(key.get());
        } else {
            db.put(key.get(), value);
        }
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        assertOpen();
        byte[] existing = db.getCopy(key.get());
        if (existing == null) {
            put(key, value);
        }
        return existing;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        assertOpen();
        // One batch, so the whole call lands in one memtable and a flush cannot split it.
        try (WriteBatch batch = new WriteBatch()) {
            for (KeyValue<Bytes, byte[]> entry : entries) {
                if (entry.value == null) {
                    batch.delete(entry.key.get());
                } else {
                    batch.put(entry.key.get(), entry.value);
                }
            }
            db.write(batch);
        }
    }

    @Override
    public byte[] delete(Bytes key) {
        assertOpen();
        byte[] existing = db.getCopy(key.get());
        db.delete(key.get());
        return existing;
    }

    @Override
    public byte[] get(Bytes key) {
        assertOpen();
        return db.getCopy(key.get());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        assertOpen();
        return new IteratorAdapter(db.iterator(from == null ? null : from.get(),
                                               to == null ? null : upperBoundExclusive(to)));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        assertOpen();
        return new IteratorAdapter(db.iterator(null, null));
    }

    /**
     * An <b>upper bound</b> on the number of distinct live keys, not an estimate of them.
     *
     * <p>Records that compaction has not yet merged are counted, so on an update-heavy workload this
     * can exceed the true count substantially and then fall sharply when compaction catches up —
     * the same shape of inaccuracy RocksDB's estimate has, in the safe direction rather than an
     * unsigned one. Costs a stats call, which is O(files): fine on a reporting interval, wrong in a
     * per-record path.
     */
    @Override
    public long approximateNumEntries() {
        assertOpen();
        return db.stats().entryCount();
    }

    @Override
    public void flush() {
        if (open) {
            db.flush();
        }
    }

    @Override
    public void close() {
        if (!open) {
            return;
        }
        open = false;
        try {
            db.close();
        } finally {
            options.close();
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private void assertOpen() {
        if (!open) {
            throw new IllegalStateException("store " + name + " is not open");
        }
    }

    /**
     * Streams' {@code range} is inclusive at both ends; the engine's is upper-exclusive. Appending a
     * zero byte is the smallest key strictly greater than {@code to} under bytewise ordering, which
     * is exactly the bound that makes the two agree.
     */
    private static byte[] upperBoundExclusive(Bytes to) {
        byte[] key = to.get();
        byte[] bound = new byte[key.length + 1];
        System.arraycopy(key, 0, bound, 0, key.length);
        bound[key.length] = 0;
        return bound;
    }

    /** Bridges the engine's next()-only iterator onto Streams' hasNext/next shape. */
    private static final class IteratorAdapter implements KeyValueIterator<Bytes, byte[]> {
        private final ElysiumKVIterator delegate;
        private KeyValue<Bytes, byte[]> pending;
        private boolean exhausted;

        IteratorAdapter(ElysiumKVIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (pending != null) {
                return true;
            }
            if (exhausted) {
                return false;
            }
            if (!delegate.next()) {
                exhausted = true;
                return false;
            }
            // The engine hands back direct buffers pointing into pinned blocks, valid only until
            // the next advance. Streams' iterator contract lets a caller hold what it was given, so
            // the bytes are copied out here rather than handing over a window that is about to move.
            pending = KeyValue.pair(Bytes.wrap(copyOf(delegate.key())), copyOf(delegate.value()));
            return true;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KeyValue<Bytes, byte[]> result = pending;
            pending = null;
            return result;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return pending.key;
        }

        @Override
        public void close() {
            delegate.close();
        }

        private static byte[] copyOf(java.nio.ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }
}
