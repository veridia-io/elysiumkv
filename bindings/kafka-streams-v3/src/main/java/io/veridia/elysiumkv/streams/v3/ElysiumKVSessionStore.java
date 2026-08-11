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
import org.apache.kafka.streams.kstream.internals.SessionWindow;
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
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

/**
 * A Kafka Streams {@link SessionStore} backed by a single ElysiumKV store.
 *
 * <p>The same design as {@link ElysiumKVWindowStore} — segment id in front of the key, so retention
 * is one {@code truncateBelow} rather than a store instance per segment — with the difference that
 * a session is a <em>pair</em> of timestamps and is segmented by its end.
 *
 * <p><b>Sessions merge, and that happens above this store.</b> When a record arrives that bridges
 * two existing sessions, Streams removes both and puts back one spanning the pair. This store is
 * asked to {@code remove} and {@code put}; it does not decide what a session is. What it must get
 * right is that a removal targets the exact {@code (key, start, end)} triple that was written, which
 * is why the key carries both timestamps rather than a duration.
 */
public class ElysiumKVSessionStore implements SessionStore<Bytes, byte[]> {
    final String name;
    private final ElysiumKVStoreConfig config;
    private final long segmentIntervalMs;
    private final long retentionPeriodMs;

    private ElysiumKVOptions options;
    private ElysiumKV db;
    private StateStoreContext context;
    private volatile boolean open;
    private volatile Position position = Position.emptyPosition();

    /** Retention is measured from the largest session end seen — stream time, not wall clock. */
    private long observedStreamTime = -1L;
    private long truncatedThrough = -1L;

    ElysiumKVSessionStore(String name, ElysiumKVStoreConfig config, long retentionPeriodMs,
                          long segmentIntervalMs) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        this.retentionPeriodMs = retentionPeriodMs;
        this.segmentIntervalMs = segmentIntervalMs;
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
            BatchingStateRestoreCallback restore = records -> {
                try (WriteBatch batch = new WriteBatch()) {
                    for (KeyValue<byte[], byte[]> record : records) {
                        final long end = SessionKeys.endOf(record.key);
                        if (end > observedStreamTime) observedStreamTime = end;
                        final Bytes key = SessionKeys.fromChangelogKey(record.key,
                                                                       segmentIntervalMs);
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
                observedStreamTime = Math.max(observedStreamTime, SessionKeys.endOf(key));
            }
        }
        try (io.veridia.elysiumkv.ElysiumKVIterator oldest = db.iterator(null, null)) {
            if (oldest.next()) {
                final byte[] key = new byte[oldest.key().remaining()];
                oldest.key().duplicate().get(key);
                truncatedThrough = SessionKeys.segmentId(SessionKeys.endOf(key),
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
    public void put(Windowed<Bytes> sessionKey, byte[] value) {
        assertOpen();
        Objects.requireNonNull(sessionKey, "sessionKey cannot be null");
        final long end = sessionKey.window().end();
        if (end > observedStreamTime) observedStreamTime = end;

        // Dropped rather than refused, as Streams' own stores do — and it is what keeps this store
        // clear of the engine's truncation floor.
        if (end <= observedStreamTime - retentionPeriodMs) {
            return;
        }

        final Bytes storeKey = storeKey(sessionKey);
        if (value == null) {
            db.delete(storeKey.get());
        } else {
            db.put(storeKey.get(), value);
        }
        expireSegmentsBelowRetention();
        advancePosition();
    }

    @Override
    public void remove(Windowed<Bytes> sessionKey) {
        assertOpen();
        Objects.requireNonNull(sessionKey, "sessionKey cannot be null");
        db.delete(storeKey(sessionKey).get());
        advancePosition();
    }

    private Bytes storeKey(Windowed<Bytes> sessionKey) {
        return SessionKeys.storeKey(sessionKey.key(), sessionKey.window().start(),
                                    sessionKey.window().end(), segmentIntervalMs);
    }

    private void expireSegmentsBelowRetention() {
        final long cutoff = observedStreamTime - retentionPeriodMs;
        if (cutoff < 0) return;
        final long minLiveSegment = SessionKeys.segmentId(cutoff, segmentIntervalMs);
        if (minLiveSegment <= truncatedThrough) return;
        db.truncateBelow(SessionKeys.segmentPrefix(minLiveSegment));
        truncatedThrough = minLiveSegment;
    }

    // --- reading -------------------------------------------------------------

    @Override
    public byte[] fetchSession(Bytes key, long startTime, long endTime) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return db.getCopy(SessionKeys.storeKey(key, startTime, endTime, segmentIntervalMs).get());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key) {
        return findSessions(key, 0L, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(Bytes key) {
        return backwardFindSessions(key, 0L, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes keyFrom, Bytes keyTo) {
        return findSessions(keyFrom, keyTo, 0L, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(Bytes keyFrom, Bytes keyTo) {
        return backwardFindSessions(keyFrom, keyTo, 0L, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key, long earliestEnd,
                                                                  long latestStart) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return new SessionIterator(scan(keyRanges(key, key, earliestEnd), earliestEnd, latestStart,
                                        true));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(Bytes key,
                                                                          long earliestEnd,
                                                                          long latestStart) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return new SessionIterator(scan(keyRanges(key, key, earliestEnd), earliestEnd, latestStart,
                                        false));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom, Bytes keyTo,
                                                                  long earliestEnd,
                                                                  long latestStart) {
        assertOpen();
        return new SessionIterator(scan(keyRanges(keyFrom, keyTo, earliestEnd), earliestEnd,
                                        latestStart, true));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(Bytes keyFrom, Bytes keyTo,
                                                                          long earliestEnd,
                                                                          long latestStart) {
        assertOpen();
        return new SessionIterator(scan(keyRanges(keyFrom, keyTo, earliestEnd), earliestEnd,
                                        latestStart, false));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(long earliestEnd,
                                                                  long latestStart) {
        assertOpen();
        return new SessionIterator(scan(segmentRanges(earliestEnd), earliestEnd, latestStart, true));
    }

    // --- scan construction ---------------------------------------------------

    private static final class Range {
        final byte[] lower;
        final byte[] upperExclusive;

        Range(byte[] lower, byte[] upperExclusive) {
            this.lower = lower;
            this.upperExclusive = upperExclusive;
        }
    }

    /**
     * The segments a query can touch, given that it wants sessions ending at or after
     * {@code earliestEnd}.
     *
     * <p>There is no upper bound derived from {@code latestStart}: a session that began before it
     * may still be open, so its end — and therefore its segment — can be arbitrarily far ahead. The
     * scan is bounded by the newest end actually seen instead, which is also what keeps an
     * open-ended query from enumerating segments that cannot exist.
     */
    private long firstLiveSegment(long earliestEnd) {
        final long requested = SessionKeys.segmentId(Math.max(earliestEnd, 0L), segmentIntervalMs);
        return Math.max(requested, Math.max(truncatedThrough, 0L));
    }

    private long lastLiveSegment() {
        return SessionKeys.segmentId(Math.max(observedStreamTime, 0L), segmentIntervalMs);
    }

    private List<Range> keyRanges(Bytes keyFrom, Bytes keyTo, long earliestEnd) {
        final List<Range> ranges = new ArrayList<>();
        for (long segment = firstLiveSegment(earliestEnd); segment <= lastLiveSegment(); ++segment) {
            ranges.add(new Range(SessionKeys.lowerRange(segment, keyFrom).get(),
                                 exclusive(SessionKeys.upperRange(segment, keyTo).get())));
        }
        return ranges;
    }

    private List<Range> segmentRanges(long earliestEnd) {
        final List<Range> ranges = new ArrayList<>();
        for (long segment = firstLiveSegment(earliestEnd); segment <= lastLiveSegment(); ++segment) {
            ranges.add(new Range(SessionKeys.segmentLowerBound(segment).get(),
                                 SessionKeys.segmentUpperBound(segment).get()));
        }
        return ranges;
    }

    private static byte[] exclusive(byte[] inclusive) {
        final byte[] bound = new byte[inclusive.length + 1];
        System.arraycopy(inclusive, 0, bound, 0, inclusive.length);
        return bound;
    }

    private ScanCursor scan(List<Range> ranges, long earliestEnd, long latestStart,
                            boolean forward) {
        final List<Range> ordered = new ArrayList<>(ranges);
        if (!forward) java.util.Collections.reverse(ordered);
        return new ScanCursor(ordered, earliestEnd, latestStart, forward);
    }

    /**
     * Concatenates per-segment scans, filtering on the session predicate.
     *
     * <p>The predicate is two-sided and cannot be expressed as a byte range: a session qualifies
     * when its end is at or after {@code earliestEnd} <em>and</em> its start at or before
     * {@code latestStart}. Ordering by end makes the first half a range; the second is a filter,
     * which is what Streams' own stores do too.
     */
    private final class ScanCursor {
        private final Iterator<Range> ranges;
        private final long earliestEnd;
        private final long latestStart;
        private final boolean forward;
        private ElysiumKVIterator current;
        private byte[] key;
        private byte[] value;
        private boolean loaded;
        private boolean exhausted;

        ScanCursor(List<Range> ranges, long earliestEnd, long latestStart, boolean forward) {
            this.ranges = ranges.iterator();
            this.earliestEnd = earliestEnd;
            this.latestStart = latestStart;
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
                if (SessionKeys.endOf(candidate) < earliestEnd) continue;
                if (SessionKeys.startOf(candidate) > latestStart) continue;
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
     * Static, unlike {@code ScanCursor}: this reads nothing from the enclosing store, where the
     * cursor needs the database. The window store's iterators split the same way.
     */
    private static final class SessionIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        private final ScanCursor cursor;

        SessionIterator(ScanCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.advance();
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final KeyValue<Windowed<Bytes>, byte[]> entry = KeyValue.pair(windowed(),
                                                                          cursor.value());
            cursor.consume();
            return entry;
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return windowed();
        }

        private Windowed<Bytes> windowed() {
            return new Windowed<>(SessionKeys.userKeyOf(cursor.key()),
                                  new SessionWindow(SessionKeys.startOf(cursor.key()),
                                                    SessionKeys.endOf(cursor.key())));
        }

        @Override
        public void close() {
            cursor.close();
        }
    }

    // --- interactive queries -------------------------------------------------

    /**
     * Answers an IQv2 query. {@code WindowRangeQuery.withKey} is the session form — the one the
     * window store declines — and is served here by fetching every session for that key.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        if (!PositionBounds.isPermitted(getPosition(), positionBound, partition())) {
            return QueryResult.notUpToBound(getPosition(), positionBound, partition());
        }

        QueryResult<R> result;
        try {
            if (query instanceof WindowRangeQuery) {
                final WindowRangeQuery<Bytes, byte[]> range = (WindowRangeQuery<Bytes, byte[]>) query;
                if (range.getKey().isEmpty()) {
                    // The time-range form addresses a window store, not this one.
                    return QueryResult.forUnknownQueryType(query, this);
                }
                result = (QueryResult<R>) QueryResult.forResult(fetch(range.getKey().get()));
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
