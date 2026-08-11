package io.veridia.elysiumkv.streams.v3;

import io.veridia.elysiumkv.ElysiumKV;
import io.veridia.elysiumkv.ElysiumKVIterator;
import io.veridia.elysiumkv.ElysiumKVOptions;
import io.veridia.elysiumkv.WriteBatch;
import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.ResultOrder;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;

/**
 * A Kafka Streams versioned store backed by a single ElysiumKV store.
 *
 * <p>Implements {@link VersionedBytesStore} directly rather than wrapping a
 * {@code VersionedKeyValueStore} in Streams' own adapter, which lives in its internals — this
 * adapter depends only on published API.
 *
 * <p><b>Two keyspaces, because two lifetimes.</b> A key's current value must be readable however
 * long ago it was written; its superseded versions only need to survive the history retention. See
 * {@link VersionedKeys} for the layout and why history sorts below latest.
 *
 * <p><b>The plain {@code KeyValueStore} methods are the versioned ones at the current time.</b>
 * Streams reaches this store through both surfaces — the versioned one from a versioned KTable, the
 * plain one from the change logger and the restore path — so they have to agree rather than being
 * two stores in a trench coat.
 */
public class ElysiumKVVersionedStore implements VersionedBytesStore {
    private final String name;
    private final ElysiumKVStoreConfig config;
    private final long historyRetentionMs;
    private final long segmentIntervalMs;

    private ElysiumKVOptions options;
    private ElysiumKV db;
    private StateStoreContext context;
    private volatile boolean open;
    private volatile Position position = Position.emptyPosition();

    /** Retention is measured from the largest timestamp seen — stream time, not wall clock. */
    private long observedStreamTime = -1L;
    private long truncatedThrough = -1L;

    ElysiumKVVersionedStore(String name, ElysiumKVStoreConfig config, long historyRetentionMs,
                            long segmentIntervalMs) {
        this.name = Objects.requireNonNull(name, "name");
        this.config = Objects.requireNonNull(config, "config");
        this.historyRetentionMs = historyRetentionMs;
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
        seedStreamTimeFromDisk();

        if (ctx != null) {
            // The changelog for a versioned store carries the *user* key with the record's
            // timestamp, not a composite — so a restored record is an ordinary versioned put and
            // must go through the same path, or a restore would place versions where a live write
            // would not have.
            BatchingStateRestoreCallback restore = records -> {
                for (KeyValue<byte[], byte[]> record : records) {
                    final Bytes key = Bytes.wrap(record.key);
                    if (record.value == null) {
                        // A changelog tombstone carries no timestamp of its own; stream time is
                        // what the live delete would have used.
                        put(key, null, Math.max(observedStreamTime, 0L));
                    } else {
                        // `timestamp ‖ value`, the format the change logger wrote.
                        put(key, valueOf(record.value), timestampOf(record.value));
                    }
                }
            };
            ctx.register(root, restore);
        }
    }

    private static long timestampOf(byte[] timestamped) {
        return java.nio.ByteBuffer.wrap(timestamped, 0, VersionedKeys.TIMESTAMP_BYTES).getLong();
    }

    private static byte[] valueOf(byte[] timestamped) {
        if (timestamped.length <= VersionedKeys.TIMESTAMP_BYTES) return new byte[0];
        final byte[] out = new byte[timestamped.length - VersionedKeys.TIMESTAMP_BYTES];
        System.arraycopy(timestamped, VersionedKeys.TIMESTAMP_BYTES, out, 0, out.length);
        return out;
    }

    /**
     * Recovers stream time from what is on disk, for the same reason the window store does: the
     * clamps are derived from a field that starts empty, so a store reopened on existing local
     * state would otherwise expire nothing and clamp its history scans to segment zero.
     */
    private void seedStreamTimeFromDisk() {
        final byte[] stored = db.getCopy(VersionedKeys.streamTimeKey());
        if (stored != null) {
            observedStreamTime = Math.max(observedStreamTime,
                                          VersionedKeys.decodeStreamTime(stored));
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

    // --- the versioned surface ------------------------------------------------

    @Override
    public long put(Bytes key, byte[] value, long timestamp) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        final boolean streamTimeAdvanced = timestamp > observedStreamTime;
        if (streamTimeAdvanced) observedStreamTime = timestamp;

        // Older than the history retention: there is nowhere to put it that a read could reach, and
        // the API has a return code that says exactly this rather than a silent drop.
        if (timestamp < observedStreamTime - historyRetentionMs) {
            return VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
        }

        final byte[] latest = db.getCopy(VersionedKeys.latestKey(key).get());
        final byte[] encoded = VersionedKeys.encodeValue(value, timestamp);

        if (latest == null || timestamp >= VersionedKeys.timestampOfValue(latest)) {
            // The new current value. Whatever it replaced becomes history — unless it is being
            // replaced at its own timestamp, which is an overwrite of that version rather than a
            // new one.
            try (WriteBatch batch = new WriteBatch()) {
                if (latest != null) {
                    final long superseded = VersionedKeys.timestampOfValue(latest);
                    if (superseded != timestamp) {
                        batch.put(VersionedKeys.historyKey(key, superseded, segmentIntervalMs).get(),
                                  latest);
                    }
                }
                batch.put(VersionedKeys.latestKey(key).get(), encoded);
                if (streamTimeAdvanced) {
                    batch.put(VersionedKeys.streamTimeKey(),
                              VersionedKeys.encodeStreamTime(observedStreamTime));
                }
                db.write(batch);
            }
            expireHistoryBelowRetention();
            advancePosition();
            return VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;
        }

        // An out-of-order write: it lands in history, and is valid until whichever version comes
        // next — the oldest one newer than it, or the current value if there is none.
        db.put(VersionedKeys.historyKey(key, timestamp, segmentIntervalMs).get(), encoded);
        expireHistoryBelowRetention();
        advancePosition();
        return validToAfter(key, timestamp, VersionedKeys.timestampOfValue(latest));
    }

    /** The timestamp of the next version after `timestamp`, which is when this one stops applying. */
    private long validToAfter(Bytes key, long timestamp, long latestTimestamp) {
        final long from = timestamp + 1;
        final long last = Math.min(segmentOf(latestTimestamp), lastLiveSegment());
        for (long segment = Math.max(segmentOf(from), firstLiveSegment()); segment <= last;
             ++segment) {
            try (ElysiumKVIterator it = db.iterator(
                         VersionedKeys.historyLowerBound(key, segment, from).get(),
                         exclusive(VersionedKeys.historyUpperBound(key, segment, latestTimestamp)
                                           .get()))) {
                // Ascending, so the first entry that is really this key's is the oldest one newer
                // than `timestamp` — which is exactly when this version stops applying.
                while (it.next()) {
                    final byte[] found = new byte[it.key().remaining()];
                    it.key().duplicate().get(found);
                    if (VersionedKeys.isHistoryEntryFor(found, key)) {
                        return VersionedKeys.timestampOfHistoryKey(found);
                    }
                }
            }
        }
        return latestTimestamp;
    }

    @Override
    public byte[] get(Bytes key) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");
        return VersionedKeys.toTimestampedFormat(db.getCopy(VersionedKeys.latestKey(key).get()));
    }

    @Override
    public byte[] get(Bytes key, long asOfTimestamp) {
        assertOpen();
        Objects.requireNonNull(key, "key cannot be null");

        final byte[] latest = db.getCopy(VersionedKeys.latestKey(key).get());
        if (latest != null && VersionedKeys.timestampOfValue(latest) <= asOfTimestamp) {
            return VersionedKeys.toTimestampedFormat(latest);
        }

        // The newest superseded version at or before the asked-for time. Descending, so the first
        // hit is the answer — and per segment, since history is segment-major.
        final long first = firstLiveSegment();
        for (long segment = Math.min(segmentOf(asOfTimestamp), lastLiveSegment()); segment >= first;
             --segment) {
            try (ElysiumKVIterator it = db.reverseIterator(
                         VersionedKeys.historyLowerBound(key, segment, 0L).get(),
                         exclusive(VersionedKeys.historyUpperBound(key, segment, asOfTimestamp)
                                           .get()))) {
                while (it.next()) {
                    final byte[] found = new byte[it.key().remaining()];
                    it.key().duplicate().get(found);
                    if (!VersionedKeys.isHistoryEntryFor(found, key)) continue;
                    final byte[] stored = new byte[it.value().remaining()];
                    it.value().duplicate().get(stored);
                    return VersionedKeys.toTimestampedFormat(stored);
                }
            }
        }
        return null;
    }

    /**
     * The band of history segments that can hold anything.
     *
     * <p>Both scans are per-segment, so an unclamped bound is not a slow query but a hang: an
     * {@code asOf} of {@link Long#MAX_VALUE} names a segment around 10^13, and the loop would open
     * an iterator for every one of them. Nothing can exist above stream time, and nothing below the
     * retention is readable — a write there is refused rather than stored — so the band is where the
     * answer must be if there is one.
     *
     * <p>The floor is the retention rather than {@link #truncatedThrough} because that field starts
     * empty on a reopen: with epoch-millisecond timestamps, a floor of zero means a miss walks every
     * segment back to 1970, which is thousands of iterator opens for a lookup that should touch two
     * or three. The truncation point is only ever at or below the retention floor anyway.
     */
    long lastLiveSegment() {
        return segmentOf(observedStreamTime);
    }

    long firstLiveSegment() {
        return Math.max(truncatedThrough, segmentOf(observedStreamTime - historyRetentionMs));
    }

    @Override
    public byte[] delete(Bytes key, long timestamp) {
        assertOpen();
        // What was valid at this instant is the answer, read before the tombstone hides it.
        final byte[] previous = get(key, timestamp);
        put(key, null, timestamp);
        return previous;
    }

    private long segmentOf(long timestamp) {
        return VersionedKeys.segmentId(Math.max(timestamp, 0L), segmentIntervalMs);
    }

    private static byte[] exclusive(byte[] inclusive) {
        final byte[] bound = new byte[inclusive.length + 1];
        System.arraycopy(inclusive, 0, bound, 0, inclusive.length);
        return bound;
    }

    /**
     * Drops history segments the retention has left behind — one truncation rather than a delete
     * per version.
     *
     * <p>Only history is below the floor, by construction: current values carry the higher prefix
     * byte and sort above every history segment, so a floor that expires the oldest history can
     * never reach them.
     */
    private void expireHistoryBelowRetention() {
        final long cutoff = observedStreamTime - historyRetentionMs;
        if (cutoff < 0) return;
        final long minLiveSegment = segmentOf(cutoff);
        if (minLiveSegment <= truncatedThrough) return;
        db.truncateBelow(VersionedKeys.historySegmentFloor(minLiveSegment));
        truncatedThrough = minLiveSegment;
    }

    // --- the plain surface, which is the versioned one at the current time -----

    @Override
    public void put(Bytes key, byte[] value) {
        put(key, value, Math.max(observedStreamTime, 0L));
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        final byte[] existing = get(key);
        if (existing == null) put(key, value);
        return existing;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        for (KeyValue<Bytes, byte[]> entry : entries) put(entry.key, entry.value);
    }

    @Override
    public byte[] delete(Bytes key) {
        return delete(key, Math.max(observedStreamTime, 0L));
    }

    /**
     * Current values only. A range over a versioned store means "what does the keyspace look like
     * now", which is the latest subspace — history is addressed by time, not by iteration.
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        assertOpen();
        final byte[] lower = from == null ? new byte[] {VersionedKeys.LATEST_PREFIX}
                                          : VersionedKeys.latestKey(from).get();
        final byte[] upper = to == null ? null
                                        : exclusive(VersionedKeys.latestKey(to).get());
        return new LatestIterator(db.iterator(lower, upper));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return range(null, null);
    }

    @Override
    public long approximateNumEntries() {
        assertOpen();
        // Every version counts, not every key — the store holds both subspaces and the engine
        // cannot tell them apart. An upper bound on the live keyspace, which is what the name
        // promises anyway.
        return db.stats().entryCount();
    }

    /** Strips the subspace prefix and the flag byte, so callers see the keys they wrote. */
    private static final class LatestIterator implements KeyValueIterator<Bytes, byte[]> {
        private final ElysiumKVIterator delegate;
        private KeyValue<Bytes, byte[]> pending;
        private boolean exhausted;

        LatestIterator(ElysiumKVIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            while (pending == null && !exhausted) {
                if (!delegate.next()) {
                    exhausted = true;
                    return false;
                }
                final byte[] storeKey = copyOf(delegate.key());
                if (storeKey.length == 0 || storeKey[0] != VersionedKeys.LATEST_PREFIX) continue;
                final byte[] stored = copyOf(delegate.value());
                final byte[] value = VersionedKeys.toTimestampedFormat(stored);
                if (value == null) continue;  // a deleted key is not part of the current keyspace
                final byte[] key = new byte[storeKey.length - 1];
                System.arraycopy(storeKey, 1, key, 0, key.length);
                pending = KeyValue.pair(Bytes.wrap(key), value);
            }
            return pending != null;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) throw new NoSuchElementException();
            final KeyValue<Bytes, byte[]> entry = pending;
            pending = null;
            return entry;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return pending.key;
        }

        @Override
        public void close() {
            delegate.close();
        }

        private static byte[] copyOf(java.nio.ByteBuffer buffer) {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
    }

    // --- interactive queries -------------------------------------------------

    /**
     * Answers an IQv2 query. The metered layer above serializes the key and asks in {@code Bytes},
     * then deserializes what comes back with the <em>plain</em> value serde — so the records here
     * carry the raw value with the timestamp beside it, not the {@code timestamp ‖ value} form the
     * store surface returns.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        if (!PositionBounds.isPermitted(getPosition(), positionBound, partition())) {
            return QueryResult.notUpToBound(getPosition(), positionBound, partition());
        }

        QueryResult<R> result;
        try {
            if (query instanceof VersionedKeyQuery) {
                final VersionedKeyQuery<Bytes, byte[]> single =
                        (VersionedKeyQuery<Bytes, byte[]>) query;
                result = (QueryResult<R>) QueryResult.forResult(
                        versionAt(single.key(),
                                  single.asOfTimestamp().map(Instant::toEpochMilli).orElse(null)));
            } else if (query instanceof MultiVersionedKeyQuery) {
                final MultiVersionedKeyQuery<Bytes, byte[]> multi =
                        (MultiVersionedKeyQuery<Bytes, byte[]>) query;
                result = (QueryResult<R>) QueryResult.forResult(versionsBetween(multi));
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

    /** One version, as of a time or as of now. Null for a key that had no value then. */
    private VersionedRecord<byte[]> versionAt(Bytes key, Long asOf) {
        final byte[] timestamped = asOf == null ? get(key) : get(key, asOf);
        if (timestamped == null) return null;
        return new VersionedRecord<>(valueOf(timestamped), timestampOf(timestamped));
    }

    /**
     * Every version of a key whose validity overlaps the asked-for interval.
     *
     * <p>A version is included when it began at or before {@code toTime} and had not yet been
     * superseded at {@code fromTime} — the question is which versions <em>were in force</em> during
     * the interval, not which were written during it. So the version current before the interval
     * began is part of the answer.
     *
     * <p>Collected rather than streamed, because a version's {@code validTo} is the next version's
     * timestamp: the answer for one entry is not known until the following one has been read, and
     * the newest needs to know there is nothing after it. One key's versions within the history
     * retention are bounded by how often that key was written, which is what a caller asking for
     * them has already accepted.
     */
    private VersionedRecordIterator<byte[]> versionsBetween(MultiVersionedKeyQuery<Bytes, byte[]> q) {
        final Bytes key = q.key();
        final long fromTime = q.fromTime().map(Instant::toEpochMilli).orElse(Long.MIN_VALUE);
        final long toTime = q.toTime().map(Instant::toEpochMilli).orElse(Long.MAX_VALUE);

        final List<VersionedRecord<byte[]>> ordered = new ArrayList<>();
        final List<Long> stamps = new ArrayList<>();   // when each collected version began
        final List<byte[]> stored = new ArrayList<>();

        // History, oldest first, across the live band.
        for (long segment = firstLiveSegment(); segment <= lastLiveSegment(); ++segment) {
            try (ElysiumKVIterator it = db.iterator(
                         VersionedKeys.historyLowerBound(key, segment, 0L).get(),
                         exclusive(VersionedKeys.historyUpperBound(key, segment, Long.MAX_VALUE)
                                           .get()))) {
                while (it.next()) {
                    final byte[] storeKey = new byte[it.key().remaining()];
                    it.key().duplicate().get(storeKey);
                    if (!VersionedKeys.isHistoryEntryFor(storeKey, key)) continue;
                    final byte[] value = new byte[it.value().remaining()];
                    it.value().duplicate().get(value);
                    stamps.add(VersionedKeys.timestampOfValue(value));
                    stored.add(value);
                }
            }
        }

        final byte[] latest = db.getCopy(VersionedKeys.latestKey(key).get());
        if (latest != null) {
            stamps.add(VersionedKeys.timestampOfValue(latest));
            stored.add(latest);
        }

        for (int i = 0; i < stored.size(); ++i) {
            final long validFrom = stamps.get(i);
            // The newest version has no successor, so it is still in force.
            final Long validTo = i + 1 < stored.size() ? stamps.get(i + 1) : null;
            if (validFrom > toTime) continue;
            if (validTo != null && validTo <= fromTime) continue;
            if (VersionedKeys.isTombstone(stored.get(i))) continue;  // a deletion is not a value
            final byte[] value = VersionedKeys.toTimestampedFormat(stored.get(i));
            ordered.add(validTo == null
                                ? new VersionedRecord<>(valueOf(value), validFrom)
                                : new VersionedRecord<>(valueOf(value), validFrom, validTo));
        }

        if (q.resultOrder() == ResultOrder.DESCENDING) Collections.reverse(ordered);
        return new ListVersionedRecordIterator(ordered);
    }

    /** The collected versions, handed out under the interface Streams expects. */
    private static final class ListVersionedRecordIterator
            implements VersionedRecordIterator<byte[]> {
        private final java.util.Iterator<VersionedRecord<byte[]>> delegate;

        ListVersionedRecordIterator(List<VersionedRecord<byte[]>> records) {
            this.delegate = records.iterator();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public VersionedRecord<byte[]> next() {
            return delegate.next();
        }

        @Override
        public void close() {}
    }

    // --- lifecycle -----------------------------------------------------------

    @Override
    public void flush() {
        if (open) db.flush();
    }

    @Override
    public void close() {
        if (!open) return;
        open = false;
        try {
            db.close();
        } finally {
            if (options != null) options.close();
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
