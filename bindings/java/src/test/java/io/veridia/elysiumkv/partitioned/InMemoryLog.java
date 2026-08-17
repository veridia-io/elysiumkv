package io.veridia.elysiumkv.partitioned;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A partitioned log with transactions, in memory. What makes the spec's tests possible without a
 * broker — and what lets them assert the things a broker would hide, like whether a record was
 * visible before its transaction committed.
 *
 * <p>Deletes travel as an encoded value rather than as a tombstone, which is the precondition the
 * whole incremental restore rests on: a compacted topic keeps delete markers only for a retention
 * window, so a partition away for longer would resume from its watermark and never learn a key was
 * removed. {@link #encode} and {@link #decode} are the caller's side of that contract.
 */
final class InMemoryLog {
    /** 0x00 marks a delete, 0x01 a put. Any non-null byte would do; the point is that it is not null. */
    private static final byte DELETED = 0x00;
    private static final byte PRESENT = 0x01;

    static final class Record {
        final String key;
        final byte[] value;

        Record(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    private final Map<Integer, List<Record>> committed = new TreeMap<>();
    private final Map<Integer, List<Record>> open = new LinkedHashMap<>();

    /** Sends assign offsets immediately, as Kafka does on append; commit is what makes them visible. */
    PendingPosition send(int partition, String key, Mutation mutation) {
        List<Record> pending = open.computeIfAbsent(partition, id -> new ArrayList<>());
        long offset = size(partition) + pending.size();
        pending.add(new Record(key, encode(mutation)));
        return () -> offset;
    }

    void commitTransaction() {
        for (Map.Entry<Integer, List<Record>> entry : open.entrySet()) {
            committed.computeIfAbsent(entry.getKey(), id -> new ArrayList<>()).addAll(entry.getValue());
        }
        open.clear();
    }

    void abortTransaction() {
        open.clear();
    }

    int size(int partition) {
        List<Record> records = committed.get(partition);
        return records == null ? 0 : records.size();
    }

    /** What a reader with read_committed sees: never the open transaction. */
    List<Record> committed(int partition) {
        return committed.getOrDefault(partition, new ArrayList<>());
    }

    /**
     * A restore callback over this log, replaying in batches so that a partial restore is a state the
     * tests can reach.
     */
    Restore<String> restoreIn(int batchSize) {
        return (partition, materializedThrough, sink) -> {
            List<Record> records = committed(partition);
            int from = materializedThrough.isPresent() ? (int) materializedThrough.getAsLong() + 1 : 0;
            for (int start = from; start < records.size(); start += batchSize) {
                int end = Math.min(start + batchSize, records.size());
                Map<String, Mutation> batch = new LinkedHashMap<>();
                for (int i = start; i < end; ++i) {
                    Record record = records.get(i);
                    batch.put(record.key, decode(record.value));
                }
                sink.putBatch(end - 1, batch);
            }
        };
    }

    static byte[] encode(Mutation mutation) {
        if (mutation.isDelete()) {
            return new byte[] {DELETED};
        }
        byte[] value = mutation.value();
        byte[] encoded = new byte[value.length + 1];
        encoded[0] = PRESENT;
        System.arraycopy(value, 0, encoded, 1, value.length);
        return encoded;
    }

    static Mutation decode(byte[] encoded) {
        if (encoded == null) {
            // A real tombstone. Compaction may drop it, so an incremental restore that resumed after
            // it would keep a value the log has already deleted. Failing here is the whole point.
            throw new IllegalStateException("a log tombstone cannot be restored incrementally");
        }
        if (encoded[0] == DELETED) {
            return Mutation.delete();
        }
        byte[] value = new byte[encoded.length - 1];
        System.arraycopy(encoded, 1, value, 0, value.length);
        return Mutation.put(value);
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static String string(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
