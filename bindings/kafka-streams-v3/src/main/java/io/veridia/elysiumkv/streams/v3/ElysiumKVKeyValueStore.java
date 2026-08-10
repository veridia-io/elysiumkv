package io.veridia.elysiumkv.streams.v3;

import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVIterator;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.WriteBatch;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.ResultOrder;
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
 * <p><b>This is the plain variant.</b> A KTable needs {@link ElysiumKVTimestampedKeyValueStore}
 * instead; the two differ only by a marker interface, for the reasons given there. Which one you
 * get is the supplier's choice, mirroring how Streams splits {@code RocksDBStore} from {@code
 * RocksDBTimestampedStore} rather than making one class serve both.
 *
 * <p><b>What this costs, stated rather than discovered.</b> In {@link StorageMode#REMOTE} a cold
 * read is an object-store GET on the processing path — tens of milliseconds against microseconds
 * locally. It pays off when key access is skewed toward recent data, so migrated files are rarely
 * touched; the cache chain softens repeat reads and does nothing for a genuine first touch. <b>A
 * topology that scans or randomly accesses the whole keyspace will be slower.</b>
 */
public class ElysiumKVKeyValueStore implements KeyValueStore<Bytes, byte[]> {
    final String name;
    private final ElysiumKVStoreConfig config;

    private ElysiumKVOptions options;
    private ElysiumKV db;
    private StateStoreContext context;
    private volatile boolean open;

    /**
     * How far this store's contents have advanced through its input topics.
     *
     * <p><b>Not optional for a store used through the DSL.</b> {@code StateStore.getPosition()} has
     * a default that throws, and Streams' caching layer calls it on every commit — so a store that
     * leaves it unimplemented cannot be materialized into a KTable at all. It fails at the first
     * commit rather than at construction, which is why calling the store directly never revealed it.
     *
     * <p>Tracked from {@code recordMetadata()} rather than through Streams' internal helper, so this
     * adapter depends only on published API.
     *
     * <p>Note that a store materialized with caching on never has this value read: {@code
     * CachingKeyValueStore} keeps a position of its own and answers from it. It is the
     * caching-disabled path that reaches here — hence {@code volatile}, since interactive queries
     * read the position on a different thread from the stream thread that advances it.
     */
    private volatile Position position = Position.emptyPosition();

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
        //
        // Registered as a *batching* callback: restore is the tail of every rebalance, and the
        // per-record form pays a write path traversal for each changelog record. One batch per
        // delivered chunk is the same work the DSL's own stores do.
        //
        // BatchingStateRestoreCallback rather than the RecordBatchingStateRestoreCallback that
        // RocksDB uses: the latter lives in Streams' internals, and this adapter depends only on
        // published API. Streams adapts between the two itself, and — this is the part that
        // matters — applies the timestamped-format conversion before either sees a record, so the
        // marker interface on the timestamped variant keeps working unchanged.
        if (ctx != null) {
            BatchingStateRestoreCallback restore = records -> {
                try (WriteBatch batch = new WriteBatch()) {
                    for (KeyValue<byte[], byte[]> record : records) {
                        // Applied in the order delivered, so a put followed by a delete of the same
                        // key inside one batch ends deleted, exactly as replaying them would.
                        if (record.value == null) {
                            batch.delete(record.key);
                        } else {
                            batch.put(record.key, record.value);
                        }
                    }
                    db.write(batch);
                }
            };
            ctx.register(root, restore);
        }
    }

    @Override
    public Position getPosition() {
        return position;
    }

    /**
     * Advances the position to the record being processed. Called on every write, because the
     * position has to describe what the store contains — a query bounded on it is otherwise answered
     * from state that does not yet include the record the bound names.
     */
    private void advancePosition() {
        if (context == null) return;
        context.recordMetadata().ifPresent(meta ->
                position = position.withComponent(meta.topic(), meta.partition(), meta.offset()));
    }

    @Override
    public void put(Bytes key, byte[] value) {
        assertOpen();
        if (value == null) {
            db.delete(key.get());
        } else {
            db.put(key.get(), value);
        }
        advancePosition();
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
        advancePosition();
    }

    @Override
    public byte[] delete(Bytes key) {
        assertOpen();
        byte[] existing = db.getCopy(key.get());
        db.delete(key.get());
        advancePosition();
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

    /**
     * The same two scans descending. Both are {@code KeyValueStore} methods whose defaults throw,
     * so without them a caller asking for descending order gets an exception rather than an answer.
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        assertOpen();
        return new IteratorAdapter(db.reverseIterator(from == null ? null : from.get(),
                                                      to == null ? null : upperBoundExclusive(to)));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        assertOpen();
        return new IteratorAdapter(db.reverseIterator(null, null));
    }

    /**
     * A prefix scan, which the interface leaves as a default that throws — so without this, a
     * Processor calling it against this store gets an exception rather than an answer.
     *
     * <p>Served by the engine's own prefix path rather than by synthesising {@code [prefix,
     * prefix++)}. That matters at the edge: a prefix of all-{@code 0xFF} bytes has no successor, so
     * the range spelling would compute an empty upper bound and silently return nothing.
     */
    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(
            P prefix, PS prefixKeySerializer) {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        assertOpen();
        return new IteratorAdapter(db.prefixIterator(prefixKeySerializer.serialize(null, prefix)));
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

    /**
     * Answers an IQv2 query. Without this, {@code StateStore.query}'s default reports every query as
     * unsupported, so {@code KafkaStreams.query(...)} cannot read this store at all.
     *
     * <p>Only {@link KeyQuery} and {@link RangeQuery} are answerable here — the window and versioned
     * queries address stores this is not one of, and are declined as unknown rather than guessed at.
     *
     * <p><b>Reached only when caching is disabled.</b> {@code CachingKeyValueStore} answers {@code
     * KeyQuery} from the cache and never consults the store underneath, so with caching on this code
     * runs for range queries alone. Same shape as {@link #getPosition()}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        // The bound is a freshness demand: the caller is asking not to be served state older than a
        // point it has already observed. Refusing is a correct answer — silently serving staler data
        // is not, which is why this precedes any read.
        if (!isPermitted(positionBound)) {
            return QueryResult.notUpToBound(getPosition(), positionBound, partition());
        }

        QueryResult<R> result;
        try {
            if (query instanceof KeyQuery) {
                KeyQuery<Bytes, byte[]> keyQuery = (KeyQuery<Bytes, byte[]>) query;
                // A missing key is a successful query with a null result, not a failure — that is
                // the convention the DSL's own stores follow, and callers distinguish the two.
                result = (QueryResult<R>) QueryResult.forResult(get(keyQuery.getKey()));
            } else if (query instanceof RangeQuery) {
                RangeQuery<Bytes, byte[]> rangeQuery = (RangeQuery<Bytes, byte[]>) query;
                // Descending is served by the engine iterating backwards, not by buffering the
                // range and reversing it — the result is streamed either way, so a range larger
                // than memory is answerable in both directions.
                final boolean descending = rangeQuery.resultOrder() == ResultOrder.DESCENDING;
                Optional<Bytes> lower = rangeQuery.getLowerBound();
                Optional<Bytes> upper = rangeQuery.getUpperBound();
                KeyValueIterator<Bytes, byte[]> iterator;
                if (lower.isPresent() || upper.isPresent()) {
                    iterator = descending ? reverseRange(lower.orElse(null), upper.orElse(null))
                                          : range(lower.orElse(null), upper.orElse(null));
                } else {
                    iterator = descending ? reverseAll() : all();
                }
                result = (QueryResult<R>) QueryResult.forResult(iterator);
            } else {
                return QueryResult.forUnknownQueryType(query, this);
            }
        } catch (Exception e) {
            return QueryResult.forFailure(
                    FailureReason.STORE_EXCEPTION,
                    query.getClass().getSimpleName() + " failed on store '" + name + "': " + e);
        }

        // What the answer was actually read from, so a caller chaining bounds can use this reply as
        // the bound for its next one.
        result.setPosition(getPosition());
        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(query.getClass().getSimpleName() + " served by " + name);
        }
        return result;
    }

    /** Delegates to {@link PositionBounds}, which both stores share so the rule cannot diverge. */
    private boolean isPermitted(PositionBound bound) {
        return PositionBounds.isPermitted(getPosition(), bound, partition());
    }

    /** The partition this store's task owns, or null if it was initialized without a context. */
    private Integer partition() {
        return context == null ? null : context.taskId().partition();
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
