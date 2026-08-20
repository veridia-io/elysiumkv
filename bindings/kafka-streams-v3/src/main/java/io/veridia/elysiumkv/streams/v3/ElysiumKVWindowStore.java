package io.veridia.elysiumkv.streams.v3;

import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVIterator;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.WriteBatch;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * A Kafka Streams {@link WindowStore} backed by a single ElysiumKV store.
 *
 * <p>One store, not one per segment. Streams' own window stores are segmented across
 * separate physical stores because retention then costs a store drop rather than a delete per key.
 * That is the right trade against RocksDB and the wrong one here: a store instance costs three
 * background threads, and Streams creates a store per task, so a topology with a dozen partitions
 * would pay them a dozen times over — per segment.
 *
 * <p>Leading the key with the segment id buys the same property without the instances. Expired
 * entries form one contiguous run at the bottom of the keyspace, so retention is a single
 * {@link ElysiumKV#truncateBelow} — one manifest edit, with whole files unlinked unread. See
 * {@link WindowKeys} for the layout and what it costs a fetch.
 */
public class ElysiumKVWindowStore implements WindowStore<Bytes, byte[]> {
    final String name;
    private final ElysiumKVStoreConfig config;
    private final long segmentIntervalMs;
    private final long windowSizeMs;
    private final long retentionPeriodMs;
    private final boolean retainDuplicates;

    private ElysiumKVOptions options;
    private ElysiumKV db;
    private StateStoreContext context;
    private volatile boolean open;
    private volatile Position position = Position.emptyPosition();

    /**
     * The largest window start seen, which is what retention is measured from — stream time, not
     * wall clock. A store that stops receiving records stops expiring, which is the behaviour
     * Streams specifies and also the one that makes a replay deterministic.
     */
    private long observedStreamTime = -1L;
    /** Segments strictly below this have already been truncated away. */
    private long truncatedThrough = -1L;
    /** Distinguishes repeated (key, time) pairs when the store is asked to retain them. */
    private int seqnum = 0;

    ElysiumKVWindowStore(String name, ElysiumKVStoreConfig config, long retentionPeriodMs,
                         long segmentIntervalMs, long windowSizeMs, boolean retainDuplicates) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        this.retentionPeriodMs = retentionPeriodMs;
        this.segmentIntervalMs = segmentIntervalMs;
        this.windowSizeMs = windowSizeMs;
        this.retainDuplicates = retainDuplicates;
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

        seedLiveBandFromDisk();

        if (ctx != null) {
            // A changelog record carries Kafka's key layout, which is this store's minus the
            // segment prefix — so restoring is a prefix and a copy, never a parse and a rebuild.
            BatchingStateRestoreCallback restore = records -> {
                try (WriteBatch batch = new WriteBatch()) {
                    for (KeyValue<byte[], byte[]> record : records) {
                        final long timestamp = WindowKeys.timestampOf(record.key);
                        // Restored records move stream time too, or the first put after a restore
                        // would measure retention from scratch and keep data it should drop.
                        if (timestamp > observedStreamTime) observedStreamTime = timestamp;
                        final Bytes key = WindowKeys.fromChangelogKey(record.key, segmentIntervalMs);
                        if (record.value == null) {
                            batch.delete(key.get());
                        } else {
                            batch.put(key.get(), record.value);
                        }
                    }
                    db.write(batch);
                }
            };
            ctx.register(root, restore);
        }
    }

    /**
     * Recovers the live band from what is on disk, because the clamps are derived from fields that
     * start empty.
     *
     * <p>Without this a reopened store reports itself empty: {@code observedStreamTime} is -1, so
     * every scan clamps to segment zero and finds nothing, however much data is there. Restore from
     * a changelog happens to fix it by replaying records, which is why it does not show up in the
     * usual Streams path — but a store reopened on existing local state has no changelog to replay,
     * and would silently answer nothing.
     *
     * <p>Two seeks: the newest key gives the upper clamp, the oldest gives the lower one. The lower
     * is not needed for correctness — the engine's truncation floor already hides what is below it —
     * but without it a long-lived store builds a range object per segment back to zero on every
     * scan.
     */
    private void seedLiveBandFromDisk() {
        try (io.veridia.elysiumkv.ElysiumKVIterator newest = db.reverseIterator(null, null)) {
            if (newest.next()) {
                final byte[] key = new byte[newest.key().remaining()];
                newest.key().duplicate().get(key);
                observedStreamTime = Math.max(observedStreamTime, WindowKeys.timestampOf(key));
            }
        }
        try (io.veridia.elysiumkv.ElysiumKVIterator oldest = db.iterator(null, null)) {
            if (oldest.next()) {
                final byte[] key = new byte[oldest.key().remaining()];
                oldest.key().duplicate().get(key);
                truncatedThrough = WindowKeys.segmentId(WindowKeys.timestampOf(key),
                                                        segmentIntervalMs) - 1;
            }
        }
    }

    @Override
    public Position getPosition() {
        return position;
    }

    private void advancePosition() {
        if (context == null) return;
        context.recordMetadata().ifPresent(meta ->
                position = position.withComponent(meta.topic(), meta.partition(), meta.offset()));
    }

    // --- writing -------------------------------------------------------------

    @Override
    public void put(Bytes key, byte[] value, long windowStartTimestamp) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");

        if (windowStartTimestamp > observedStreamTime) observedStreamTime = windowStartTimestamp;

        // Dropped rather than refused, which is what Streams' own stores do: a record older than
        // the retention window is late, not invalid, and failing the task over it would turn a
        // data-quality problem into an outage. It is also what keeps this store clear of the
        // engine's truncation floor, since nothing below the floor is ever offered to it.
        if (windowStartTimestamp <= observedStreamTime - retentionPeriodMs) {
            return;
        }

        if (value == null && !retainDuplicates) {
            db.delete(WindowKeys.storeKey(key, windowStartTimestamp, 0, segmentIntervalMs).get());
        } else {
            maybeUpdateSeqnumForDups();
            final Bytes storeKey =
                    WindowKeys.storeKey(key, windowStartTimestamp, seqnum, segmentIntervalMs);
            if (value == null) {
                db.delete(storeKey.get());
            } else {
                db.put(storeKey.get(), value);
            }
        }

        expireSegmentsBelowRetention();
        advancePosition();
    }

    /**
     * Drops whole segments that retention has left behind, as one truncation.
     *
     * <p>Guarded on the segment actually advancing: a truncation is a manifest edit, and doing one
     * per put would be absurd. Segments are coarse — Streams defaults the interval to half the
     * retention period — so in practice this fires a handful of times a day.
     */
    private void expireSegmentsBelowRetention() {
        final long cutoff = observedStreamTime - retentionPeriodMs;
        if (cutoff < 0) return;
        final long minLiveSegment = WindowKeys.segmentId(cutoff, segmentIntervalMs);
        if (minLiveSegment <= truncatedThrough) return;
        db.truncateBelow(WindowKeys.segmentPrefix(minLiveSegment));
        truncatedThrough = minLiveSegment;
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) seqnum = (seqnum + 1) & 0x7FFF_FFFF;
    }

    // --- reading -------------------------------------------------------------

    @Override
    public byte[] fetch(Bytes key, long time) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return db.getCopy(WindowKeys.storeKey(key, time, seqnum, segmentIntervalMs).get());
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return new TimestampedIterator(scan(keyRanges(key, key, timeFrom, timeTo), timeFrom, timeTo,
                                            true));
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(Bytes key, long timeFrom, long timeTo) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return new TimestampedIterator(scan(keyRanges(key, key, timeFrom, timeTo), timeFrom, timeTo,
                                            false));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom, Bytes keyTo,
                                                           long timeFrom, long timeTo) {
        assertOpen();
        return new WindowedIterator(scan(keyRanges(keyFrom, keyTo, timeFrom, timeTo), timeFrom,
                                         timeTo, true));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(Bytes keyFrom, Bytes keyTo,
                                                                   long timeFrom, long timeTo) {
        assertOpen();
        return new WindowedIterator(scan(keyRanges(keyFrom, keyTo, timeFrom, timeTo), timeFrom,
                                         timeTo, false));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo) {
        assertOpen();
        return new WindowedIterator(scan(segmentRanges(timeFrom, timeTo), timeFrom, timeTo, true));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(long timeFrom, long timeTo) {
        assertOpen();
        return new WindowedIterator(scan(segmentRanges(timeFrom, timeTo), timeFrom, timeTo, false));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        assertOpen();
        return new WindowedIterator(scan(List.of(new Range(null, null)), Long.MIN_VALUE,
                                         Long.MAX_VALUE, true));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        assertOpen();
        return new WindowedIterator(scan(List.of(new Range(null, null)), Long.MIN_VALUE,
                                         Long.MAX_VALUE, false));
    }

    // --- scan construction ---------------------------------------------------

    /** A half-open byte range; null bounds are unbounded, as the engine reads them. */
    private static final class Range {
        final byte[] lower;
        final byte[] upperExclusive;

        Range(byte[] lower, byte[] upperExclusive) {
            this.lower = lower;
            this.upperExclusive = upperExclusive;
        }
    }

    /**
     * One range per segment the time span touches, bounded by key within each.
     *
     * <p>Per segment rather than one range overall, because the ordering is segment-major: a single
     * range from the first segment's lower bound to the last segment's upper bound would sweep every
     * key of every segment in between.
     */
    private List<Range> keyRanges(Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo) {
        final List<Range> ranges = new ArrayList<>();
        final long first = firstLiveSegment(timeFrom);
        final long last = lastLiveSegment(timeTo);
        for (long segment = first; segment <= last; ++segment) {
            final byte[] lower = keyFrom == null
                    ? WindowKeys.segmentLowerBound(segment).get()
                    : WindowKeys.lowerRange(segment, keyFrom, timeFrom).get();
            final byte[] upper = keyTo == null
                    ? WindowKeys.segmentUpperBound(segment).get()
                    : exclusive(WindowKeys.upperRange(segment, keyTo, timeTo).get());
            ranges.add(new Range(lower, upper));
        }
        return ranges;
    }

    /** Whole segments, for the scans that name no key and must therefore filter by time. */
    private List<Range> segmentRanges(long timeFrom, long timeTo) {
        final List<Range> ranges = new ArrayList<>();
        final long first = firstLiveSegment(timeFrom);
        final long last = lastLiveSegment(timeTo);
        for (long segment = first; segment <= last; ++segment) {
            ranges.add(new Range(WindowKeys.segmentLowerBound(segment).get(),
                                 WindowKeys.segmentUpperBound(segment).get()));
        }
        return ranges;
    }

    /**
     * The segments a scan can usefully visit, clamped to those that can hold anything.
     *
     * <p>Not a refinement — a bound on the loop. A scan builds one range per segment it
     * spans, so an open-ended {@code timeTo} of {@code Long.MAX_VALUE} — which is exactly what an
     * IQv2 query with no upper bound asks for — would otherwise mean 10^14 iterations before the
     * first entry came back. Nothing above the largest timestamp seen exists, and nothing below the
     * truncation floor does either, so clamping to that band costs no result and bounds the work.
     */
    private long firstLiveSegment(long timeFrom) {
        final long requested = WindowKeys.segmentId(Math.max(timeFrom, 0L), segmentIntervalMs);
        return Math.max(requested, Math.max(truncatedThrough, 0L));
    }

    private long lastLiveSegment(long timeTo) {
        final long requested = WindowKeys.segmentId(Math.max(timeTo, 0L), segmentIntervalMs);
        final long newest = WindowKeys.segmentId(Math.max(observedStreamTime, 0L),
                                                 segmentIntervalMs);
        return Math.min(requested, newest);
    }

    /** The engine's upper bound is exclusive; these bounds are built inclusive. */
    private static byte[] exclusive(byte[] inclusive) {
        final byte[] bound = new byte[inclusive.length + 1];
        System.arraycopy(inclusive, 0, bound, 0, inclusive.length);
        return bound;
    }

    /**
     * Walks the ranges in order, each one scanned in the requested direction, filtering on time.
     *
     * <p>The filter is not redundant even when the bounds already name a key: within a segment the
     * ordering is key-major, so a scan over a <em>range</em> of keys sweeps timestamps outside the
     * span for every key but the first and last. Streams' own stores filter for the same reason.
     */
    private ScanCursor scan(List<Range> ranges, long timeFrom, long timeTo, boolean forward) {
        final List<Range> ordered = new ArrayList<>(ranges);
        if (!forward) java.util.Collections.reverse(ordered);
        return new ScanCursor(ordered, timeFrom, timeTo, forward);
    }

    /** Concatenates per-segment scans, which is ordered because segments are disjoint. */
    private final class ScanCursor {
        private final Iterator<Range> ranges;
        private final long timeFrom;
        private final long timeTo;
        private final boolean forward;
        private ElysiumKVIterator current;
        private byte[] key;
        private byte[] value;
        private boolean loaded;
        private boolean exhausted;

        ScanCursor(List<Range> ranges, long timeFrom, long timeTo, boolean forward) {
            this.ranges = ranges.iterator();
            this.timeFrom = timeFrom;
            this.timeTo = timeTo;
            this.forward = forward;
        }

        boolean advance() {
            if (loaded) return true;
            if (exhausted) return false;
            while (true) {
                if (current == null) {
                    if (!ranges.hasNext()) {
                        exhausted = true;
                        return false;
                    }
                    final Range range = ranges.next();
                    current = forward ? db.iterator(range.lower, range.upperExclusive)
                                      : db.reverseIterator(range.lower, range.upperExclusive);
                }
                if (!current.next()) {
                    current.close();
                    current = null;
                    continue;
                }
                final byte[] candidate = copyOf(current.key());
                final long timestamp = WindowKeys.timestampOf(candidate);
                if (timestamp < timeFrom || timestamp > timeTo) continue;
                key = candidate;
                value = copyOf(current.value());
                loaded = true;
                return true;
            }
        }

        byte[] key() {
            return key;
        }

        byte[] value() {
            return value;
        }

        void consume() {
            loaded = false;
        }

        void close() {
            if (current != null) {
                current.close();
                current = null;
            }
            exhausted = true;
        }

        private byte[] copyOf(ByteBuffer buffer) {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }

    /**
     * {@code fetch(key, from, to)} yields the timestamp as its key.
     *
     * <p>Static, unlike its two siblings: this one reads nothing from the enclosing store, where
     * {@code WindowedIterator} needs the window size and {@code ScanCursor} needs the database.
     */
    private static final class TimestampedIterator implements WindowStoreIterator<byte[]> {
        private final ScanCursor cursor;

        TimestampedIterator(ScanCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.advance();
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final KeyValue<Long, byte[]> entry =
                    KeyValue.pair(WindowKeys.timestampOf(cursor.key()), cursor.value());
            cursor.consume();
            return entry;
        }

        @Override
        public Long peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return WindowKeys.timestampOf(cursor.key());
        }

        @Override
        public void close() {
            cursor.close();
        }
    }

    /** The scans that span keys yield the window itself. */
    private final class WindowedIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        private final ScanCursor cursor;

        WindowedIterator(ScanCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.advance();
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final KeyValue<Windowed<Bytes>, byte[]> entry = KeyValue.pair(windowed(), cursor.value());
            cursor.consume();
            return entry;
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return windowed();
        }

        private Windowed<Bytes> windowed() {
            final long start = WindowKeys.timestampOf(cursor.key());
            return new Windowed<>(WindowKeys.userKeyOf(cursor.key()),
                                  new TimeWindow(start, start + windowSizeMs));
        }

        @Override
        public void close() {
            cursor.close();
        }
    }

    // --- interactive queries -------------------------------------------------

    /**
     * Answers an IQv2 query. Without this, {@code StateStore.query}'s default reports every query as
     * unsupported, so {@code KafkaStreams.query(...)} cannot read this store at all.
     *
     * <p>Only the two window queries are answerable. {@code WindowRangeQuery.withKey} is left to the
     * unknown-type path deliberately: Streams serves that form from a <em>session</em> store, not a
     * window one, so answering it here would invent a meaning the query does not have.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        if (!PositionBounds.isPermitted(getPosition(), positionBound, partition())) {
            return QueryResult.notUpToBound(getPosition(), positionBound, partition());
        }

        QueryResult<R> result;
        try {
            if (query instanceof WindowKeyQuery) {
                final WindowKeyQuery<Bytes, byte[]> windowKey = (WindowKeyQuery<Bytes, byte[]>) query;
                result = (QueryResult<R>) QueryResult.forResult(
                        fetch(windowKey.getKey(), from(windowKey.getTimeFrom()),
                              to(windowKey.getTimeTo())));
            } else if (query instanceof WindowRangeQuery) {
                final WindowRangeQuery<Bytes, byte[]> range = (WindowRangeQuery<Bytes, byte[]>) query;
                if (range.getKey().isPresent()) {
                    return QueryResult.forUnknownQueryType(query, this);
                }
                result = (QueryResult<R>) QueryResult.forResult(
                        fetchAll(from(range.getTimeFrom()), to(range.getTimeTo())));
            } else {
                return QueryResult.forUnknownQueryType(query, this);
            }
        } catch (Exception e) {
            return QueryResult.forFailure(
                    FailureReason.STORE_EXCEPTION,
                    query.getClass().getSimpleName() + " failed on store '" + name + "': " + e);
        }

        result.setPosition(getPosition());
        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(query.getClass().getSimpleName() + " served by " + name);
        }
        return result;
    }

    /**
     * An absent bound means unbounded, and the scan clamps it to the segments that exist — which is
     * what stops {@code Long.MAX_VALUE} becoming 10^14 empty ranges.
     */
    private static long from(java.util.Optional<java.time.Instant> bound) {
        return bound.map(java.time.Instant::toEpochMilli).orElse(0L);
    }

    private static long to(java.util.Optional<java.time.Instant> bound) {
        return bound.map(java.time.Instant::toEpochMilli).orElse(Long.MAX_VALUE);
    }

    /** The partition this store's task owns, or null if it was initialized without a context. */
    private Integer partition() {
        return context == null ? null : context.taskId().partition();
    }

    // --- lifecycle -----------------------------------------------------------

    @Override
    public void flush() {
        if (open) db.flush();
    }

    /**
     * Closes the store, flushing first.
     *
     * <p>The engine attempts a flush of its own when the handle closes, so this is not the only
     * thing standing between a caller and a lost memtable. It is the only thing that can
     * <em>report</em> one: the engine's attempt happens in a destructor and cannot raise, whereas a
     * failure here propagates and tells the caller their writes are not durable.
     *
     * <p>Costs nothing where Streams already flushed — an empty memtable is not written — and the
     * store and its options are released whether the flush succeeds or not.
     */
    @Override
    public void close() {
        if (!open) return;
        open = false;
        try {
            db.flush();
        } finally {
            try {
                db.close();
            } finally {
                if (options != null) options.close();
            }
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
            throw new org.apache.kafka.streams.errors.InvalidStateStoreException(
                    "store " + name + " is not open");
        }
    }
}
