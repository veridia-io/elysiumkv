package io.veridia.elysiumkv.partitioned;

import io.veridia.elysiumkv.WriteBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One partition's staged mutations, indexed by store key and remembering the order they arrived in.
 *
 * <p>Inspectable rather than a native {@link WriteBatch}, which is what lets a read inside the
 * transaction see what that transaction staged: a batch is opaque and write-only. The batch is built
 * from this at apply time instead, so an abandoned transaction also holds no native handle.
 *
 * <p>A point mutation and a range delete each supersede the other depending on which was staged
 * later, so both carry a sequence and every question about a key is answered by comparing the two.
 */
final class StagedOverlay {

    /** Store order: unsigned lexicographic, the order the engine's iterators deliver. */
    static final Comparator<byte[]> BY_KEY = Arrays::compareUnsigned;

    /**
     * Sorted for lookup, not for order: {@code byte[]} uses identity equality, so a {@link java.util.HashMap}
     * here would never find a key it was given and every staged read would miss. The comparator is
     * what gives these keys value semantics.
     */
    private final Map<byte[], Point> points = new TreeMap<>(BY_KEY);
    private final List<Range> ranges = new ArrayList<>();
    private final List<PendingPosition> positions = new ArrayList<>();
    private long sequence;

    void put(byte[] key, Mutation mutation, PendingPosition position) {
        points.put(key, new Point(key, mutation, sequence++));
        positions.add(position);
    }

    void deleteRange(byte[] lower, byte[] upper, PendingPosition position) {
        ranges.add(new Range(lower, upper, sequence++));
        positions.add(position);
    }

    /** The watermark a successful commit stamps: the highest position this partition's records reached. */
    long maxPosition() {
        if (positions.isEmpty()) {
            // Unreachable — nothing stages without sending — but the fallback would be to stamp
            // Long.MIN_VALUE as a watermark, which is the kind of quiet catastrophe worth one branch.
            throw new IllegalStateException("a staged batch with no changelog positions");
        }
        long highest = Long.MIN_VALUE;
        for (PendingPosition position : positions) {
            highest = Math.max(highest, position.position());
        }
        return highest;
    }

    /**
     * What the staged set says about one key, in one lookup: nothing, an absence, or a value.
     *
     * <p>One call rather than a "does it decide" followed by a "what is it", because the second
     * cannot report an absence apart from a miss and the pair only worked in that order.
     *
     * <p>A value is copied, because the changelog encoded these bytes when they were staged and the
     * apply reads them again: a caller that mutated what it was handed would send one value to the
     * log and write another to the store, which no replay repairs.
     */
    Resolution resolve(byte[] key) {
        Point point = points.get(key);
        long covered = covering(key);
        if (point == null) {
            return covered >= 0 ? Resolution.ABSENT : Resolution.UNSTAGED;
        }
        if (point.sequence < covered || point.mutation.isDelete()) {
            return Resolution.ABSENT;
        }
        return new Resolution(true, point.mutation.value().clone());
    }

    /** A staged answer. {@code staged} false is a miss; true with a null {@code value} is an absence. */
    static final class Resolution {
        static final Resolution UNSTAGED = new Resolution(false, null);
        static final Resolution ABSENT = new Resolution(true, null);

        final boolean staged;
        final byte[] value;

        private Resolution(boolean staged, byte[] value) {
            this.staged = staged;
            this.value = value;
        }
    }

    /** Appends every staged operation in the order it was staged, so that later ones win. */
    void into(WriteBatch batch) {
        List<Op> ordered = new ArrayList<>(points.size() + ranges.size());
        ordered.addAll(points.values());
        ordered.addAll(ranges);
        ordered.sort(Comparator.comparingLong(op -> op.sequence));
        for (Op op : ordered) {
            op.into(batch);
        }
    }

    /**
     * What this staged set contributes to one scan: the keys it injects, and which committed keys it
     * answers for. Taken at the start of a scan and not affected by later staging, matching an engine
     * iterator holding the version it started on.
     *
     * @param reverse the scan's direction, which decides the order injections are delivered in
     */
    StagedSnapshot snapshot(byte[] lo, byte[] hi, boolean reverse) {
        return snapshot(lo, hi, null, reverse);
    }

    /**
     * The same for a prefix scan, which tests the prefix rather than deriving a range from it. A
     * derived upper bound has to answer what follows a prefix of nothing but {@code 0xFF} bytes; a
     * prefix test has no such case, which is why the engines this mirrors all take it.
     */
    StagedSnapshot prefixSnapshot(byte[] prefix, boolean reverse) {
        return snapshot(prefix, null, prefix, reverse);
    }

    private StagedSnapshot snapshot(byte[] lo, byte[] hi, byte[] prefix, boolean reverse) {
        NavigableMap<byte[], byte[]> puts = new TreeMap<>(BY_KEY);
        NavigableSet<byte[]> decided = new TreeSet<>(BY_KEY);
        for (Point point : points.values()) {
            if (!inRange(point.key, lo, hi) || !startsWith(point.key, prefix)) {
                continue;
            }
            decided.add(point.key);
            Resolution resolved = resolve(point.key);
            if (resolved.value != null) {
                puts.put(point.key, resolved.value);
            }
        }
        List<Range> overlapping = new ArrayList<>();
        for (Range range : ranges) {
            if (range.overlaps(lo, hi)) {
                overlapping.add(range);
            }
        }
        return new StagedSnapshot(puts, decided, overlapping, reverse);
    }

    /** The highest range covering this key, or -1 when none does. */
    private long covering(byte[] key) {
        long highest = -1;
        for (Range range : ranges) {
            if (range.covers(key) && range.sequence > highest) {
                highest = range.sequence;
            }
        }
        return highest;
    }

    /** Half-open, with null bounds unbounded — the convention the engine's iterators use. */
    static boolean inRange(byte[] key, byte[] lo, byte[] hi) {
        return (lo == null || BY_KEY.compare(key, lo) >= 0)
                && (hi == null || BY_KEY.compare(key, hi) < 0);
    }

    /** True when {@code prefix} is null, so that a range scan shares the one filter. */
    static boolean startsWith(byte[] key, byte[] prefix) {
        return prefix == null
                || (key.length >= prefix.length
                        && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length));
    }

    private abstract static class Op {
        final long sequence;

        Op(long sequence) {
            this.sequence = sequence;
        }

        abstract void into(WriteBatch batch);
    }

    private static final class Point extends Op {
        final byte[] key;
        final Mutation mutation;

        Point(byte[] key, Mutation mutation, long sequence) {
            super(sequence);
            this.key = key;
            this.mutation = mutation;
        }

        @Override
        void into(WriteBatch batch) {
            if (mutation.isDelete()) {
                batch.delete(key);
            } else {
                batch.put(key, mutation.value());
            }
        }
    }

    static final class Range extends Op {
        final byte[] lower;
        final byte[] upper;

        Range(byte[] lower, byte[] upper, long sequence) {
            super(sequence);
            this.lower = lower;
            this.upper = upper;
        }

        boolean covers(byte[] key) {
            return inRange(key, lower, upper);
        }

        /** Whether this range has any key in common with a scan over {@code [lo, hi)}. */
        boolean overlaps(byte[] lo, byte[] hi) {
            return (hi == null || BY_KEY.compare(lower, hi) < 0)
                    && (lo == null || BY_KEY.compare(upper, lo) > 0);
        }

        @Override
        void into(WriteBatch batch) {
            batch.deleteRange(lower, upper);
        }
    }
}
